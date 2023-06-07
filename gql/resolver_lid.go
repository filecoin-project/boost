package gql

import (
	"context"
	"time"

	"github.com/filecoin-project/boost/db"
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
		}
	}

	var sealed, unsealed int32
	for _, s := range lu.SectorStates { // TODO: consider adding this data directly in SSM
		if s == db.SealStateUnsealed {
			unsealed++
		} else if s == db.SealStateSealed {
			sealed++
		}
	}

	fp, err := r.piecedirectory.FlaggedPiecesCount(ctx)
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
			Indexed:         360,
			FlaggedUnsealed: 33,
			FlaggedSealed:   480,
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
