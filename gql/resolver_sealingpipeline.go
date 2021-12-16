package gql

import (
	"context"
	"time"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
)

// query: sealingpipeline: [SealingPipeline]
func (r *resolver) SealingPipeline(ctx context.Context) (*sealingPipelineResolver, error) {
	committing := int32(0)
	waitSeed := int32(0)

	//res, err := r.spApi.WorkerJobs(ctx)
	//if err != nil {
	//return nil, err
	//}

	//_ = res

	//spew.Dump(res)

	summary, err := r.spApi.SectorsSummary(ctx)
	if err != nil {
		return nil, err
	}

	wdSectors, err := r.spApi.SectorsListInStates(ctx, []api.SectorState{"WaitDeals"})
	if err != nil {
		return nil, err
	}

	taken := uint64(0)
	deals := []abi.DealID{}
	for _, s := range wdSectors {
		wdSectorStatus, err := r.spApi.SectorsStatus(ctx, s, false)
		if err != nil {
			return nil, err
		}

		for _, p := range wdSectorStatus.Pieces {
			taken += uint64(p.Piece.Size)
		}

		deals = append(deals, wdSectorStatus.Deals...)
	}

	log.Debugw("sealing pipeline", "waitdeals", summary["WaitDeals"], "taken", taken, "deals", deals, "pc1", summary["PreCommit1"], "pc2", summary["PreCommit2"], "precommitwait", summary["PreCommitWait"], "waitseed", summary["WaitSeed"], "committing", summary["Committing"], "commitwait", summary["CommitWait"], "proving", summary["Proving"])

	return &sealingPipelineResolver{
		WaitDeals: waitDeals{
			SectorSize: 32 * 1024 * 1024 * 1024,
			Deals: []*waitDeal{{
				ID:   graphql.ID(uuid.New().String()),
				Size: 10 * 1024 * 1024 * 1024,
			}, {
				ID:   graphql.ID(uuid.New().String()),
				Size: 7 * 1024 * 1024 * 1024,
			}, {
				ID:   graphql.ID(uuid.New().String()),
				Size: 12 * 1024 * 1024 * 1024,
			}},
		},
		SectorStates: sectorStates{
			AddPiece:       1,
			Packing:        0,
			PreCommit1:     0,
			PreCommit2:     1,
			PreCommitWait:  0,
			WaitSeed:       waitSeed,
			Committing:     committing,
			CommittingWait: 1,
			FinalizeSector: 0,
		},
		Workers: []*worker{{
			ID:     "3152",
			Start:  graphql.Time{Time: time.Now().Add(-20 * time.Minute)},
			Stage:  "AddPiece",
			Sector: 311,
		}, {
			ID:     "5231",
			Start:  graphql.Time{Time: time.Now().Add(-23 * time.Minute)},
			Stage:  "AddPiece",
			Sector: 341,
		}, {
			ID:     "572",
			Start:  graphql.Time{Time: time.Now().Add(-11 * time.Minute)},
			Stage:  "Precommit2",
			Sector: 633,
		}, {
			ID:     "9522",
			Start:  graphql.Time{Time: time.Now().Add(-132 * time.Minute)},
			Stage:  "WaitSeed",
			Sector: 624,
		}},
	}, nil
}

type waitDeal struct {
	ID   graphql.ID
	Size gqltypes.Uint64
}

type waitDeals struct {
	SectorSize gqltypes.Uint64
	Deals      []*waitDeal
}

type sectorStates struct {
	AddPiece       int32
	Committing     int32
	WaitSeed       int32
	PreCommitting  int32
	Packing        int32
	PreCommit1     int32
	PreCommit2     int32
	PreCommitWait  int32
	CommittingWait int32
	FinalizeSector int32
}

type worker struct {
	ID     string
	Start  graphql.Time
	Stage  string
	Sector int32
}

type sealingPipelineResolver struct {
	WaitDeals    waitDeals
	SectorStates sectorStates
	Workers      []*worker
}
