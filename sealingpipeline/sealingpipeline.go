package sealingpipeline

import (
	"context"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/google/uuid"
)

type State interface {
	WorkerJobs(context.Context) (map[uuid.UUID][]storiface.WorkerJob, error)
	SectorsStatus(ctx context.Context, sid abi.SectorNumber, showOnChainInfo bool) (api.SectorInfo, error)
	SectorsList(context.Context) ([]abi.SectorNumber, error)
	SectorsSummary(ctx context.Context) (map[api.SectorState]int, error)
	SectorsListInStates(context.Context, []api.SectorState) ([]abi.SectorNumber, error)
}
