package gql

import (
	"context"
	"time"

	"github.com/filecoin-project/boost/db"
	bdtypes "github.com/filecoin-project/boost/extern/boostd-data/svc/types"
	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/graph-gophers/graphql-go"
)

type resolverScanProgress struct {
	Progress float64
	LastScan *graphql.Time
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
	ScanProgress         resolverScanProgress
	Pieces               pieces
	SectorUnsealedCopies sectorUnsealedCopies
	SectorProvingState   sectorProvingState
	FlaggedPieces        int32
}

// query: lid: [LID]
func (r *resolver) LID(ctx context.Context) (*lidState, error) {
	lus := make([]*sectorstatemgr.SectorStateUpdates, 0, len(r.ssm.Maddrs))

	for _, addr := range r.ssm.Maddrs {
		var lu *sectorstatemgr.SectorStateUpdates
		for lu == nil {
			mu := r.ssm.LatestUpdateMus[addr]
			mu.Lock()
			lu = r.ssm.LatestUpdates[addr]
			mu.Unlock()
			if lu == nil {
				time.Sleep(2 * time.Second)
				log.Debugf("LID sector states updates for miner %s is nil, waiting for update...", addr.String())
			} else {
				log.Debugf("LID sector states updates for miner %s set", addr.String())
			}
		}
		lus = append(lus, lu)
	}

	var sealed, unsealed int32
	var sectorstates, activesectors int
	for _, lu := range lus {
		sectorstates += len(lu.SectorStates)
		activesectors += len(lu.ActiveSectors)

		for id, s := range lu.SectorStates { // TODO: consider adding this data directly in SSM
			_, sectorHasDeals := lu.SectorWithDeals[id]

			if s == db.SealStateUnsealed {
				unsealed++
			} else if s == db.SealStateSealed && sectorHasDeals {
				sealed++

				log.Debugw("LID only sealed sector", "miner", id.Miner, "sector_number", id.Number)
			}
		}
	}

	var fpHasUnsealed int
	var fpNoUnsealed int
	var ap int
	// var scanProgress int

	for _, maddr := range r.ssm.Maddrs {
		// TODO: pass in miner id explicitly from the UI
		tfpHasUnsealed, err := r.piecedirectory.FlaggedPiecesCount(ctx, &bdtypes.FlaggedPiecesListFilter{
			HasUnsealedCopy: true,
			MinerAddr:       maddr,
		})
		if err != nil {
			return nil, err
		}
		fpHasUnsealed += tfpHasUnsealed

		tfpNoUnsealed, err := r.piecedirectory.FlaggedPiecesCount(ctx, &bdtypes.FlaggedPiecesListFilter{
			HasUnsealedCopy: false,
			MinerAddr:       maddr,
		})
		if err != nil {
			return nil, err
		}
		fpNoUnsealed += tfpNoUnsealed

		tap, err := r.piecedirectory.PiecesCount(ctx, maddr)
		if err != nil {
			return nil, err
		}
		ap += tap

		// tscanProgress, err := r.piecedirectory.ScanProgress(ctx, maddr)
		// if err != nil {
		// 	return nil, err
		// }
		// scanProgress += int(tscanProgress.Progress)
	}

	flaggedPiecesCount := fpHasUnsealed + fpNoUnsealed

	// var lastScan *graphql.Time
	// if !scanProgress.LastScan.IsZero() {
	// 	lastScan = &graphql.Time{Time: scanProgress.LastScan}
	// }

	ls := &lidState{
		// ScanProgress: resolverScanProgress{
		// 	Progress: scanProgress.Progress,
		// 	LastScan: lastScan,
		// },
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
			Active:   int32(activesectors),
			Inactive: int32(sectorstates) - int32(activesectors), // TODO: add an explicit InactiveSectors in ssm
		},
	}

	return ls, nil
}
