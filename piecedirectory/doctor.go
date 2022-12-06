package piecedirectory

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var doclog = logging.Logger("piecedoc")

type SealingApi interface {
	IsUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error)
}

// The Doctor periodically queries the piece directory for piece cids, and runs
// checks against those pieces. If there is a problem with a piece, it is
// flagged, so that it can be surfaced to the user.
// Note that multiple Doctor processes can run in parallel. The logic for which
// pieces to give to the Doctor to check is in the piece directory.
type Doctor struct {
	store Store
	sapi  SealingApi
	queue chan cid.Cid
}

func NewDoctor(store Store, sapi SealingApi) *Doctor {
	// TODO: remove
	logging.SetLogLevel("piecedoc", "debug")

	return &Doctor{store: store, sapi: sapi}
}

func (d *Doctor) Run(ctx context.Context) {
	for ctx.Err() == nil {
		// Get the next pieces to check (eg pieces that haven't been checked
		// for a while) from the piece directory
		pcids, err := d.store.NextPiecesToCheck(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			doclog.Errorw("getting next pieces to check", "err", err)
			time.Sleep(time.Minute)
			continue
		}

		// Check each piece for problems
		doclog.Debugw("piece doctor: checking pieces", "count", len(pcids))
		for _, pcid := range pcids {
			err := d.checkPiece(ctx, pcid)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				doclog.Errorw("checking piece", "piece", pcid, "err", err)
			}
		}
		doclog.Debugw("piece doctor: completed checking pieces", "count", len(pcids))

		// Sleep for about 10 seconds. The time to sleep is randomized, so that
		// if there are multiple doctor processes they will each process some
		// pieces some of the time.
		//time.Sleep(time.Duration(5000+rand.Intn(10000)) * time.Millisecond)
		time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)
	}
}

func (d *Doctor) checkPiece(ctx context.Context, pieceCid cid.Cid) error {
	md, err := d.store.GetPieceMetadata(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("failed to get piece %s from piece directory: %w", pieceCid, err)
	}

	// Check if the piece is in an error state
	if md.Error != "" {
		err = d.store.FlagPiece(ctx, pieceCid)
		if err != nil {
			return fmt.Errorf("failed to flag piece in error state %s: %w", pieceCid, err)
		}
		doclog.Infow("piece is in error state", "err", md.Error)
		return nil
	}

	// Check if piece has been indexed
	isIndexed, err := d.store.IsIndexed(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("failed to check index status of piece %s: %w", pieceCid, err)
	}

	if !isIndexed {
		err = d.store.FlagPiece(ctx, pieceCid)
		if err != nil {
			return fmt.Errorf("failed to flag unindexed piece %s: %w", err)
		}
		doclog.Infow("flagging piece as unindexed", "piece", pieceCid)
		return nil
	}

	// Check if there is an unsealed copy of the piece
	var hasUnsealedDeal bool
	dls := md.Deals
	for _, dl := range dls {
		isUnsealed, err := d.sapi.IsUnsealed(ctx, dl.SectorID, dl.PieceOffset.Unpadded(), dl.PieceLength.Unpadded())
		if err != nil {
			isUnsealed = false
			//return fmt.Errorf("failed to check unsealed status of piece %s (sector %d, offset %d, length %d): %w",
			//	pieceCid, dl.SectorID, dl.PieceOffset.Unpadded(), dl.PieceLength.Unpadded(), err)
		}

		if isUnsealed {
			hasUnsealedDeal = true
			break
		}
	}

	if !hasUnsealedDeal {
		err = d.store.FlagPiece(ctx, pieceCid)
		if err != nil {
			return fmt.Errorf("failed to flag piece %s with no unsealed deal: %w", pieceCid, err)
		}

		doclog.Infow("flagging piece as having no unsealed copy", "piece", pieceCid)
		return nil
	}

	// There are no known issues with the piece, so unflag it
	err = d.store.UnflagPiece(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("failed to unflag piece %s: %w", pieceCid, err)
	}

	return nil
}
