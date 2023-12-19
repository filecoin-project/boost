package piecedirectory

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/filecoin-project/boost/db"
	bdclient "github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var doclog = logging.Logger("piecedoc")

// The Doctor periodically queries the local index directory for piece cids, and runs
// checks against those pieces. If there is a problem with a piece, it is
// flagged, so that it can be surfaced to the user.
// Note that multiple Doctor processes can run in parallel. The logic for which
// pieces to give to the Doctor to check is in the local index directory.
type Doctor struct {
	maddr       address.Address
	store       *bdclient.Store
	ssm         *sectorstatemgr.SectorStateMgr
	fullnodeApi api.FullNode
}

func NewDoctor(maddr address.Address, store *bdclient.Store, ssm *sectorstatemgr.SectorStateMgr, fullnodeApi api.FullNode) *Doctor {
	return &Doctor{maddr: maddr, store: store, ssm: ssm, fullnodeApi: fullnodeApi}
}

// The average interval between calls to NextPiecesToCheck
const avgCheckInterval = 30 * time.Second

func (d *Doctor) Run(ctx context.Context) {
	doclog.Info("piece doctor: running")

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		err := func() error {
			var lu *sectorstatemgr.SectorStateUpdates
			d.ssm.LatestUpdateMu.Lock()
			lu = d.ssm.LatestUpdate
			d.ssm.LatestUpdateMu.Unlock()
			if lu == nil {
				doclog.Warn("sector state manager not yet updated")
				return nil
			}

			head, err := d.fullnodeApi.ChainHead(ctx)
			if err != nil {
				return err
			}

			// Get the next pieces to check (eg pieces that haven't been checked
			// for a while) from the local index directory
			pcids, err := d.store.NextPiecesToCheck(ctx, d.maddr)
			if err != nil {
				return err
			}

			// Check each piece for problems
			doclog.Debugw("piece doctor: checking pieces", "count", len(pcids))
			for _, pcid := range pcids {
				err := d.checkPiece(ctx, pcid, lu, head)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return err
					}
					doclog.Errorw("checking piece", "piece", pcid, "err", err)
				}
			}
			doclog.Debugw("piece doctor: completed checking pieces", "count", len(pcids))

			return nil
		}()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				doclog.Errorw("piece doctor: context canceled, stopping doctor", "error", err)
				return
			}

			doclog.Errorw("piece doctor: iteration got error", "error", err)
		}

		// Sleep for a few seconds between ticks.
		// The time to sleep is randomized, so that if there are multiple doctor
		// processes they will each process some pieces some of the time.
		sleepTime := avgCheckInterval/2 + time.Duration(rand.Intn(int(avgCheckInterval)))
		timer.Reset(sleepTime)
	}
}

func (d *Doctor) checkPiece(ctx context.Context, pieceCid cid.Cid, lu *sectorstatemgr.SectorStateUpdates, head *types.TipSet) error {
	defer func(start time.Time) { log.Debugw("checkPiece processing", "took", time.Since(start)) }(time.Now())

	// Check if piece belongs to an active sector
	md, err := d.store.GetPieceMetadata(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("failed to get piece %s from local index directory: %w", pieceCid, err)
	}

	lacksActiveSector := true // check whether the piece is present in active sector
	hasDealsOnThisMiner := false
	var chainDeals []model.DealInfo
	for _, dl := range md.Deals {
		// Ignore deals that were not made on this node's miner
		if d.maddr != dl.MinerAddr {
			continue
		}
		hasDealsOnThisMiner = true

		mid, err := address.IDFromAddress(dl.MinerAddr)
		if err != nil {
			return err
		}

		sectorID := abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: dl.SectorID,
		}

		// check if we have an active sector
		if _, ok := lu.ActiveSectors[sectorID]; ok {
			lacksActiveSector = false
			chainDeals = append(chainDeals, dl)
		}
	}

	if !hasDealsOnThisMiner {
		doclog.Warnw("ignoring piece as it is not present in any deals on this miner", "piece", pieceCid.String(), "miner", d.maddr.String())
		return nil
	}

	if lacksActiveSector {
		doclog.Debugw("ignoring and unflagging piece as it is not present in an active sector", "piece", pieceCid.String())

		err = d.store.UnflagPiece(ctx, pieceCid, d.maddr)
		if err != nil {
			return fmt.Errorf("failed to unflag piece %s: %w", pieceCid, err)
		}
		return nil
	}

	// Check that Deal is actually on-chain for the active sectors
	if d.fullnodeApi != nil { // nil in tests
		found := false
		claims, err := d.fullnodeApi.StateGetClaims(ctx, d.maddr, types.EmptyTSK)
		if err != nil {
			return fmt.Errorf("getting claims for the miner %s: %w", d.maddr, err)
		}
		for _, dealId := range chainDeals {
			if dealId.IsDirectDeal {
				doclog.Debugw("checking state for direct deal", "piece", pieceCid, "allocation", dealId.ChainDealID)
				for _, v := range claims {
					if v.Sector == dealId.SectorID {
						found = true
					}
				}
			} else {
				doclog.Debugw("checking state for market deal", "piece", pieceCid, "deal", dealId.ChainDealID)
				_, err := d.fullnodeApi.StateMarketStorageDeal(ctx, dealId.ChainDealID, head.Key())
				if err == nil {
					found = true
					break
				}
			}
		}

		if !found {
			doclog.Debugw("ignoring and unflagging piece as no deal id found on chain", "piece", pieceCid)

			err = d.store.UnflagPiece(ctx, pieceCid, d.maddr)
			if err != nil {
				return fmt.Errorf("failed to unflag piece %s: %w", pieceCid, err)
			}
			return nil
		}
	}

	var hasUnsealedCopy bool

	for _, dl := range md.Deals {
		// Ignore deals that were not made on this node's miner
		if d.maddr != dl.MinerAddr {
			continue
		}

		mid, err := address.IDFromAddress(dl.MinerAddr)
		if err != nil {
			return err
		}

		sectorID := abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: dl.SectorID,
		}

		if lu.SectorStates[sectorID] == db.SealStateUnsealed {
			hasUnsealedCopy = true
			break
		}
	}

	// Check if piece has been indexed
	isIndexed, err := d.store.IsIndexed(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("failed to check index status of piece %s: %w", pieceCid, err)
	}

	// If piece is not indexed or has no unsealed copy, flag it
	if !isIndexed || !hasUnsealedCopy {
		err = d.store.FlagPiece(ctx, pieceCid, hasUnsealedCopy, d.maddr)
		if err != nil {
			return fmt.Errorf("failed to flag piece %s: %w", pieceCid, err)
		}
		doclog.Debugw("flagging piece", "piece", pieceCid, "isIndexed", isIndexed, "hasUnsealedCopy", hasUnsealedCopy, "len(activeSectors)", len(lu.ActiveSectors), "len(sectorStates)", len(lu.SectorStates))
		return nil
	}

	// There are no known issues with the piece, so unflag it
	doclog.Debugw("unflagging piece", "piece", pieceCid)
	err = d.store.UnflagPiece(ctx, pieceCid, d.maddr)
	if err != nil {
		return fmt.Errorf("failed to unflag piece %s: %w", pieceCid, err)
	}

	return nil
}
