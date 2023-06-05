package gql

import (
	"context"

	gqltypes "github.com/filecoin-project/boost/gql/types"
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
	ls := &lidState{
		FlaggedPieces: 3654,
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
			Sealed:   340,
			Unsealed: 340,
		},
		SectorProvingState: sectorProvingState{
			Active:   340,
			Inactive: 340,
		},
	}

	return ls, nil
}
