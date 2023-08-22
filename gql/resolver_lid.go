package gql

import (
	"context"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/sectorstatemgr"
	bdtypes "github.com/filecoin-project/boostd-data/svc/types"
)

type pieces struct {
	Indexed         int32
	FlaggedUnsealed int32
	FlaggedSealed   int32
}

type sectorUnsealedCopies struct {
	Unsealed int32
	Sealed   int32
}

type sectorProvingState struct {
	Active   int32
	Inactive int32
}

type lidState struct {
	Pieces               pieces
	SectorUnsealedCopies sectorUnsealedCopies
	SectorProvingState   sectorProvingState
	FlaggedPieces        int32
}

// query: lid: [LID]
func (r *resolver) LID(ctx context.Context) (*lidState, error) {
	var lu *sectorstatemgr.SectorStateUpdates
	for lu == nil {
		r.ssm.LatestUpdateMu.Lock()
		lu = r.ssm.LatestUpdate
		r.ssm.LatestUpdateMu.Unlock()
		if lu == nil {
			time.Sleep(2 * time.Second)
			log.Debug("LID sector states updates is nil, waiting for update...")
		} else {
			log.Debug("LID sector states updates set")
		}
	}

	var sealed, unsealed int32
	for id, s := range lu.SectorStates { // TODO: consider adding this data directly in SSM
		_, sectorHasDeals := lu.SectorWithDeals[id]

		if s == db.SealStateUnsealed {
			unsealed++
		} else if s == db.SealStateSealed && sectorHasDeals {
			sealed++

			log.Debugw("LID only sealed sector", "miner", id.Miner, "sector_number", id.Number)
		}
	}

	maddr := r.provider.Address
	fpHasUnsealed, err := r.piecedirectory.FlaggedPiecesCount(ctx, &bdtypes.FlaggedPiecesListFilter{
		HasUnsealedCopy: true,
		MinerAddr:       maddr,
	})
	if err != nil {
		return nil, err
	}

	fpNoUnsealed, err := r.piecedirectory.FlaggedPiecesCount(ctx, &bdtypes.FlaggedPiecesListFilter{
		HasUnsealedCopy: false,
		MinerAddr:       maddr,
	})
	if err != nil {
		return nil, err
	}

	flaggedPiecesCount := fpHasUnsealed + fpNoUnsealed

	ap, err := r.piecedirectory.PiecesCount(ctx, maddr)
	if err != nil {
		return nil, err
	}

	ls := &lidState{
		FlaggedPieces: int32(flaggedPiecesCount),
		Pieces: pieces{
			Indexed:         int32(ap - flaggedPiecesCount),
			FlaggedUnsealed: int32(fpHasUnsealed),
			FlaggedSealed:   int32(fpNoUnsealed),
		},
		SectorUnsealedCopies: sectorUnsealedCopies{
			Sealed:   sealed,
			Unsealed: unsealed,
		},
		SectorProvingState: sectorProvingState{
			Active:   int32(len(lu.ActiveSectors)),
			Inactive: int32(len(lu.SectorStates) - len(lu.ActiveSectors)), // TODO: add an explicit InactiveSectors in ssm
		},
	}

	return ls, nil
}
