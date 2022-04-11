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

	sectors := []*waitDealSector{}
	for _, s := range wdSectors {
		taken := uint64(0)
		deals := []*waitDeal{}

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

		sectors = append(sectors, &waitDealSector{
			Deals:      deals,
			Taken:      gqltypes.Uint64(taken),
			SectorSize: gqltypes.Uint64(ssize),
		})
	}

	var ss sectorStates
	for order, state := range allSectorStates {
		count, ok := summary[api.SectorState(state)]
		if !ok {
			continue
		}
		if count == 0 {
			continue
		}

		if _, ok := normalSectors[state]; ok {
			ss.Regular = append(ss.Regular, &sectorState{
				Key:   state,
				Value: int32(count),
				Order: int32(order),
			})
			continue
		}

		if _, ok := normalErredSectors[state]; ok {
			ss.RegularError = append(ss.RegularError, &sectorState{
				Key:   state,
				Value: int32(count),
				Order: int32(order),
			})
			continue
		}

		if _, ok := snapdealsSectors[state]; ok {
			ss.SnapDeals = append(ss.SnapDeals, &sectorState{
				Key:   state,
				Value: int32(count),
				Order: int32(order),
			})
			continue
		}

		if _, ok := snapdealsSectors[state]; ok {
			ss.SnapDealsError = append(ss.SnapDealsError, &sectorState{
				Key:   state,
				Value: int32(count),
				Order: int32(order),
			})
			continue
		}
	}

	return &sealingPipelineState{
		Sectors:      sectors,
		SectorStates: ss,
		Workers:      workers,
	}, nil
}

type sectorState struct {
	Key   string
	Value int32
	Order int32
}

type waitDeal struct {
	ID   graphql.ID
	Size gqltypes.Uint64
}

type waitDealSector struct {
	Deals      []*waitDeal
	Taken      gqltypes.Uint64
	SectorSize gqltypes.Uint64
}

type sectorStates struct {
	Regular        []*sectorState
	SnapDeals      []*sectorState
	RegularError   []*sectorState
	SnapDealsError []*sectorState
}

type worker struct {
	ID     string
	Start  graphql.Time
	Stage  string
	Sector int32
}

type sealingPipelineState struct {
	Sectors      []*waitDealSector
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
