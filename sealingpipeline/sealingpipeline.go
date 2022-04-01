package sealingpipeline

import (
	"context"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/google/uuid"
)

type API interface {
	ActorAddress(context.Context) (address.Address, error)
	WorkerJobs(context.Context) (map[uuid.UUID][]storiface.WorkerJob, error)
	SectorsStatus(ctx context.Context, sid abi.SectorNumber, showOnChainInfo bool) (api.SectorInfo, error)
	SectorsList(context.Context) ([]abi.SectorNumber, error)
	SectorsSummary(ctx context.Context) (map[api.SectorState]int, error)
	SectorsListInStates(context.Context, []api.SectorState) ([]abi.SectorNumber, error)
}

func GetStatus(ctx context.Context, api API) (*Status, error) {
	res, err := api.WorkerJobs(ctx)
	if err != nil {
		return nil, err
	}

	var workers []*worker
	for workerId, jobs := range res {
		for _, j := range jobs {
			workers = append(workers, &worker{
				ID:     workerId.String(),
				Start:  j.Start,
				Stage:  j.Task.Short(),
				Sector: int32(j.Sector.Number),
			})
		}
	}

	summary, err := api.SectorsSummary(ctx)
	if err != nil {
		return nil, err
	}

	st := &Status{
		SectorStates: sectorStates{
			AddPiece:       int32(summary["AddPiece"]),
			Packing:        int32(summary["Packing"]),
			PreCommit1:     int32(summary["PreCommit1"]),
			PreCommit2:     int32(summary["PreCommit2"]),
			PreCommitWait:  int32(summary["PreCommitWait"]),
			WaitSeed:       int32(summary["WaitSeed"]),
			Committing:     int32(summary["Committing"]),
			CommittingWait: int32(summary["CommitWait"]),
			FinalizeSector: int32(summary["FinalizeSector"]),
		},
		Workers: workers,
	}

	return st, nil
}

type sectorStates struct {
	AddPiece       int32
	Packing        int32
	PreCommit1     int32
	PreCommit2     int32
	WaitSeed       int32
	PreCommitWait  int32
	Committing     int32
	CommittingWait int32
	FinalizeSector int32
}

type worker struct {
	ID     string
	Start  time.Time
	Stage  string
	Sector int32
}

// TODO: maybe add json tags
type Status struct {
	SectorStates sectorStates
	Workers      []*worker
}
