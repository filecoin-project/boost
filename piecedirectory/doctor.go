package piecedirectory

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	verifregtypes "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/boost/db"
	bdclient "github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/sectorstatemgr"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
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

			// Prefetch the state claims
			doclog.Debugw("prefetching state claims", "address", d.maddr)
			claims, err := d.fullnodeApi.StateGetClaims(ctx, d.maddr, types.EmptyTSK)
			if err != nil {
				return fmt.Errorf("getting claims for the miner %s: %w", d.maddr, err)
			}

			nv, err := d.fullnodeApi.StateNetworkVersion(ctx, types.EmptyTSK)
			if err != nil {
				return fmt.Errorf("getting network version: %w", err)
			}
			nv29 := nv >= network.Version29

			for _, pcid := range pcids {
				err := d.checkPiece(ctx, pcid, lu, head, claims, nv29)
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

// nv29 reports whether claims have stopped being created (FIP-0118).
func (d *Doctor) checkPiece(ctx context.Context, pieceCid cid.Cid, lu *sectorstatemgr.SectorStateUpdates, head *types.TipSet, claims map[verifregtypes.ClaimId]verifregtypes.Claim, nv29 bool) error {
	defer func(start time.Time) { log.Debugw("checkPiece processing", "took", time.Since(start)) }(time.Now())

	// Check if piece belongs to an active sector
	md, err := d.store.GetPieceMetadata(ctx, pieceCid)
	if err != nil {
		// If piece is not found then it should be unflagged and removed from future tracking
		if strings.Contains(err.Error(), "not found") {
			serr := d.store.UnflagPiece(ctx, pieceCid, d.maddr)
			if serr != nil {
				return fmt.Errorf("failed to unflag the missing piece %s: %w", pieceCid.String(), serr)
			}
			serr = d.store.UntrackPiece(ctx, pieceCid, d.maddr)
			if serr != nil {
				return fmt.Errorf("failed to delete piece from tracker table %s: %w", pieceCid.String(), serr)
			}
			return nil
		}
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
		for _, dealId := range chainDeals {
			if dealId.IsDirectDeal {
				doclog.Debugw("checking state for direct deal", "piece", pieceCid, "allocation", dealId.ChainDealID)
				for _, v := range claims {
					if v.Sector == dealId.SectorID {
						found = true
					}
				}
				// Claims stop being written at nv29, so for such a sector the
				// active sector found above is the only evidence left and reading
				// the missing claim as "gone from chain" would skip the checks
				// below on every pass.
				//
				// The chain version is enough here, unlike elsewhere in this
				// change: the sector is already known to be active, and a sector
				// that sealed before nv29 always has its claim, so dating each
				// sector individually would only ever re-derive that. Reading the
				// version too high just means a claim is assumed present a little
				// later than it was, which makes the doctor check more, not less.
				if !found && nv29 {
					doclog.Debugw("no claim for direct deal at nv29+; relying on its active sector",
						"piece", pieceCid, "allocation", dealId.ChainDealID, "sector", dealId.SectorID)
					found = true
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
