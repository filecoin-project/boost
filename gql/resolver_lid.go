package gql

import (
	"context"
	"time"

	"github.com/filecoin-project/boost/db"
	bdtypes "github.com/filecoin-project/boost/extern/boostd-data/svc/types"
	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/sectorstatemgr"
)

type dealData struct {
	Indexed         gqltypes.Uint64
	FlaggedUnsealed gqltypes.Uint64
	FlaggedSealed   gqltypes.Uint64
}

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
	DealData             dealData
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

	fp, err := r.piecedirectory.FlaggedPiecesCount(ctx, nil)
	if err != nil {
		return nil, err
	}

	fpt, err := r.piecedirectory.FlaggedPiecesCount(ctx, &bdtypes.FlaggedPiecesListFilter{HasUnsealedCopy: true})
	if err != nil {
		return nil, err
	}

	fpf, err := r.piecedirectory.FlaggedPiecesCount(ctx, &bdtypes.FlaggedPiecesListFilter{HasUnsealedCopy: false})
	if err != nil {
		return nil, err
	}

	ap, err := r.piecedirectory.PiecesCount(ctx)
	if err != nil {
		return nil, err
	}

	ls := &lidState{
		FlaggedPieces: int32(fp),
		DealData: dealData{
			Indexed:         gqltypes.Uint64(12094627905536),
			FlaggedUnsealed: gqltypes.Uint64(1094627905536),
			FlaggedSealed:   gqltypes.Uint64(18094627905536),
		},
		Pieces: pieces{
			Indexed:         int32(ap - fp),
			FlaggedUnsealed: int32(fpt),
			FlaggedSealed:   int32(fpf),
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
