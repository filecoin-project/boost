package piecedirectory

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/sectorstatemgr"
	bdclient "github.com/filecoin-project/boostd-data/client"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
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
	store *bdclient.Store
	ssm   *sectorstatemgr.SectorStateMgr
}

func NewDoctor(store *bdclient.Store, ssm *sectorstatemgr.SectorStateMgr) *Doctor {
	return &Doctor{store: store, ssm: ssm}
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
			// Get the next pieces to check (eg pieces that haven't been checked
			// for a while) from the local index directory
			pcids, err := d.store.NextPiecesToCheck(ctx)
			if err != nil {
				return err
			}

			// Check each piece for problems
			doclog.Debugw("piece doctor: checking pieces", "count", len(pcids))
			for _, pcid := range pcids {
				err := d.checkPiece(ctx, pcid)
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

func (d *Doctor) checkPiece(ctx context.Context, pieceCid cid.Cid) error {
	md, err := d.store.GetPieceMetadata(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("failed to get piece %s from local index directory: %w", pieceCid, err)
	}

	// Check if piece has been indexed
	isIndexed, err := d.store.IsIndexed(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("failed to check index status of piece %s: %w", pieceCid, err)
	}

	if !isIndexed {
		err = d.store.FlagPiece(ctx, pieceCid)
		if err != nil {
			return fmt.Errorf("failed to flag unindexed piece %s: %w", pieceCid, err)
		}
		doclog.Debugw("flagging piece as unindexed", "piece", pieceCid)
		return nil
	}

	// Check if there is an unsealed copy of the piece
	var hasUnsealedDeal bool

	// Check whether the piece is present in active sector
	lacksActiveSector := true

	dls := md.Deals
	for _, dl := range dls {
		mid, err := address.IDFromAddress(dl.MinerAddr)
		if err != nil {
			return err
		}

		sectorID := abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: dl.SectorID,
		}

		// check if we have an active sector
		if _, ok := d.ssm.ActiveSectors[sectorID]; ok {
			lacksActiveSector = false
		}

		if d.ssm.SectorStates[sectorID] == db.SealStateUnsealed {
			hasUnsealedDeal = true
			break
		}
	}

	if !hasUnsealedDeal && !lacksActiveSector {
		err = d.store.FlagPiece(ctx, pieceCid)
		if err != nil {
			return fmt.Errorf("failed to flag piece %s with no unsealed deal: %w", pieceCid, err)
		}

		doclog.Debugw("flagging piece as having no unsealed copy", "piece", pieceCid)
		return nil
	}

	// There are no known issues with the piece, so unflag it
	err = d.store.UnflagPiece(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("failed to unflag piece %s: %w", pieceCid, err)
	}

	return nil
}
