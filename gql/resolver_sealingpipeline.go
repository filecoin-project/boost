package gql

import (
	"context"
	"errors"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
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

	sdwdSectors, err := r.spApi.SectorsListInStates(ctx, []api.SectorState{"SnapDealsWaitDeals"})
	if err != nil {
		return nil, err
	}

	waitDealsSectors, err := r.populateWaitDealsSectors(ctx, wdSectors, ssize)
	if err != nil {
		return nil, err
	}
	snapDealsWaitDealsSectors, err := r.populateWaitDealsSectors(ctx, sdwdSectors, ssize)
	if err != nil {
		return nil, err
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
		WaitDealsSectors:          waitDealsSectors,
		SnapDealsWaitDealsSectors: snapDealsWaitDealsSectors,
		SectorStates:              ss,
		Workers:                   workers,
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
	SectorID   gqltypes.Uint64
	Deals      []*waitDeal
	Used       gqltypes.Uint64
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
	WaitDealsSectors          []*waitDealSector
	SnapDealsWaitDealsSectors []*waitDealSector
	SectorStates              sectorStates
	Workers                   []*worker
}

func getSectorSize(ctx context.Context, fullNode v1api.FullNode, maddr address.Address) (uint64, error) {
	mi, err := fullNode.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return 0, err
	}

	return uint64(mi.SectorSize), nil
}

func (r *resolver) populateWaitDealsSectors(ctx context.Context, sectorNumbers []abi.SectorNumber, ssize uint64) ([]*waitDealSector, error) {
	waitDealsSectors := []*waitDealSector{}
	for _, s := range sectorNumbers {
		used := uint64(0)
		deals := []*waitDeal{}

		wdSectorStatus, err := r.spApi.SectorsStatus(ctx, s, false)
		if err != nil {
			return nil, err
		}

		for _, p := range wdSectorStatus.Pieces {
			if p.DealInfo == nil {
				continue
			}

			dcid, err := p.DealInfo.DealProposal.Cid()
			if err != nil {
				return nil, err
			}

			ds, err := r.dealsByPublishCID(ctx, p.DealInfo.PublishCid)
			if err != nil {
				return nil, err
			}
			var i int

			if len(ds) == 0 { // legacy deal
				lds, err := r.legacyProv.ListLocalDeals()
				if err != nil {
					return nil, err
				}

				var j int
				for ; j < len(lds); j++ {
					l := lds[j]

					if p.DealInfo.PublishCid == nil || l.PublishCid == nil {
						continue
					}

					lpcid, err := l.ClientDealProposal.Proposal.Cid()
					if err != nil {
						return nil, err
					}

					if l.PublishCid.Equals(*p.DealInfo.PublishCid) && lpcid.Equals(dcid) {
						break
					}
				}

				if j == len(lds) {
					return nil, errors.New("couldnt match legacy markets deal based on publish cid and proposal cid")
				}

				deals = append(deals, &waitDeal{
					ID:   graphql.ID(lds[j].ProposalCid.String()),
					Size: gqltypes.Uint64(p.Piece.Size),
				})
				used += uint64(p.Piece.Size)
				continue
			}

			if len(ds) > 1 { // compare by deal proposal cid
				for ; i < len(ds); i++ {
					cid, err := ds[i].ClientDealProposal.Proposal.Cid()
					if err != nil {
						return nil, err
					}

					if cid.Equals(dcid) {
						break
					}
				}
			}

			if i == len(ds) {
				return nil, errors.New("couldnt match boost deal based on publish cid and proposal cid")
			}

			deals = append(deals, &waitDeal{
				ID:   graphql.ID(ds[i].DealUuid.String()),
				Size: gqltypes.Uint64(p.Piece.Size),
			})
			used += uint64(p.Piece.Size)
		}

		waitDealsSectors = append(waitDealsSectors, &waitDealSector{
			SectorID:   gqltypes.Uint64(s),
			Deals:      deals,
			Used:       gqltypes.Uint64(used),
			SectorSize: gqltypes.Uint64(ssize),
		})
	}

	return waitDealsSectors, nil
}
