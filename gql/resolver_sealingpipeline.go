package gql

import (
	"context"

	"github.com/davecgh/go-spew/spew"
	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/graph-gophers/graphql-go"
)

// query: sealingpipeline: [SealingPipeline]
func (r *resolver) SealingPipeline(ctx context.Context) (*sealingPipelineResolver, error) {
	res, err := r.spApi.WorkerJobs(ctx)
	if err != nil {
		return nil, err
	}

	spew.Dump(res)

	var workers []*worker

	for workerId, jobs := range res {
		for _, j := range jobs {
			workers = append(workers, &worker{
				ID: workerId.String(),
				//JobID: j.ID
				Start:  graphql.Time{j.Start},
				Stage:  j.Task.Short(),
				Sector: int32(j.Sector.Number),
			})

			// 1+ - assigned
			// 0  - running
			// -1 - ret-wait
			// -2 - returned
			// -3 - ret-done
			//RunWait int
		}
	}

	summary, err := r.spApi.SectorsSummary(ctx)
	if err != nil {
		return nil, err
	}

	ssize, err := getSectorSize(ctx, r.fullNode, r.minerAddr)
	if err != nil {
		return nil, err
	}

	wdSectors, err := r.spApi.SectorsListInStates(ctx, []api.SectorState{"WaitDeals"})
	if err != nil {
		return nil, err
	}

	taken := uint64(0)
	deals := []*waitDeal{}
	for _, s := range wdSectors {
		wdSectorStatus, err := r.spApi.SectorsStatus(ctx, s, false)
		if err != nil {
			return nil, err
		}

		for _, p := range wdSectorStatus.Pieces {
			//TODO: any other way to map deal from sector with deal from db?
			d, err := r.dealByPublishCID(ctx, p.DealInfo.PublishCid)
			if err != nil {
				return nil, err
			}
			deals = append(deals, &waitDeal{
				ID:   graphql.ID(d.DealUuid.String()),
				Size: gqltypes.Uint64(p.Piece.Size),
			})
			taken += uint64(p.Piece.Size)
		}

	}

	log.Debugw("sealing pipeline", "waitdeals", summary["WaitDeals"], "taken", taken, "deals", deals, "pc1", summary["PreCommit1"], "pc2", summary["PreCommit2"], "precommitwait", summary["PreCommitWait"], "waitseed", summary["WaitSeed"], "committing", summary["Committing"], "commitwait", summary["CommitWait"], "proving", summary["Proving"])

	return &sealingPipelineResolver{
		WaitDeals: waitDeals{
			SectorSize: gqltypes.Uint64(ssize),
			Deals:      deals,
		},
		SectorStates: sectorStates{
			AddPiece:       1, //TODO: fixme
			Packing:        0, //TODO: fixme
			PreCommit1:     int32(summary["PreCommit1"]),
			PreCommit2:     int32(summary["PreCommit2"]),
			PreCommitWait:  int32(summary["PreCommitWait"]),
			WaitSeed:       int32(summary["WaitSeed"]),
			Committing:     int32(summary["Committing"]),
			CommittingWait: int32(summary["CommitWait"]),
			FinalizeSector: 0, //TODO: fixme
		},
		Workers: workers,
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

func getSectorSize(ctx context.Context, fullNode v1api.FullNode, maddr address.Address) (uint64, error) {
	mi, err := fullNode.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return 0, err
	}

	return uint64(mi.SectorSize), nil
}
