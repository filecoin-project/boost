package gql

import (
	"context"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/graph-gophers/graphql-go"
)

// query: sealingpipeline: [SealingPipeline]
func (r *resolver) SealingPipeline(ctx context.Context) (*sealingPipelineState, error) {
	res, err := r.spApi.WorkerJobs(ctx)
	if err != nil {
		return nil, err
	}

	var workers []*worker
	for workerId, jobs := range res {
		for _, j := range jobs {
			workers = append(workers, &worker{
				ID:     workerId.String(),
				Start:  graphql.Time{Time: j.Start},
				Stage:  j.Task.Short(),
				Sector: int32(j.Sector.Number),
			})
		}
	}

	summary, err := r.spApi.SectorsSummary(ctx)
	if err != nil {
		return nil, err
	}

	minerAddr, err := r.spApi.ActorAddress(ctx)
	if err != nil {
		return nil, err
	}

	ssize, err := getSectorSize(ctx, r.fullNode, minerAddr)
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
			if p.DealInfo == nil {
				continue
			}
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

	sectorStates := []*sectorState{}
	for k, v := range summary {
		if v == 0 {
			continue
		}

		if _, ok := normalSectors[string(k)]; ok {
			sectorStates = append(sectorStates, &sectorState{
				Key:   string(k),
				Value: int32(v),
				Type:  "Regular",
			})
			continue
		}

		if _, ok := snapdealsSectors[string(k)]; ok {
			sectorStates = append(sectorStates, &sectorState{
				Key:   string(k),
				Value: int32(v),
				Type:  "Snap Deals",
			})
			continue
		}

		log.Errorw("unknown sector in resolver", "name", string(k), "count", v)
	}

	return &sealingPipelineState{
		WaitDeals: waitDeals{
			SectorSize: gqltypes.Uint64(ssize),
			Deals:      deals,
		},
		SectorStates: sectorStates,
		Workers:      workers,
	}, nil
}

type sectorState struct {
	Key   string
	Value int32
	Type  string
}

type waitDeal struct {
	ID   graphql.ID
	Size gqltypes.Uint64
}

type waitDeals struct {
	SectorSize gqltypes.Uint64
	Deals      []*waitDeal
}

type worker struct {
	ID     string
	Start  graphql.Time
	Stage  string
	Sector int32
}

type sealingPipelineState struct {
	WaitDeals    waitDeals
	SectorStates []*sectorState
	Workers      []*worker
}

func getSectorSize(ctx context.Context, fullNode v1api.FullNode, maddr address.Address) (uint64, error) {
	mi, err := fullNode.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return 0, err
	}

	return uint64(mi.SectorSize), nil
}
