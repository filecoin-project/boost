package gql

import (
	"context"
	"errors"
	"fmt"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
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
	ID       graphql.ID
	Size     gqltypes.Uint64
	IsLegacy bool
	IsDirect bool
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

			publishCid := p.DealInfo.PublishCid
			if publishCid == nil {
				// This is likely a direct deal
				var directDeals []*smtypes.DirectDeal
				if p.DealInfo.PieceActivationManifest.VerifiedAllocationKey != nil {
					vkey := p.DealInfo.PieceActivationManifest.VerifiedAllocationKey
					activeDeals, err := r.directDealsDB.ActiveByPieceAllocID(ctx, p.DealInfo.PieceCID(), verifreg.AllocationId(vkey.ID))
					if err != nil {
						return nil, err
					}
					// Match the client address
					for _, deal := range activeDeals {
						deal := deal
						ac, err := r.fullNode.StateLookupID(ctx, deal.Client, types.EmptyTSK)
						if err != nil {
							return nil, err
						}
						w, err := address.IDFromAddress(ac)
						if err != nil {
							return nil, fmt.Errorf("converting client address to ID: %w", err)
						}

						wid := abi.ActorID(w)

						if wid != vkey.Client {
							log.Info("Client mismatch for deal", deal.ID)
							continue
						}
						directDeals = append(directDeals, deal)
					}
					// We should have only 1 deal left here as import check prevents more than 1 active deal
					// with for same client and allocationID
					if len(directDeals) > 1 {
						return nil, errors.New("more than 1 active deal with same client ID and allocation ID found")
					}
					if len(directDeals) == 1 {
						d := directDeals[0]
						deals = append(deals, &waitDeal{
							ID:       graphql.ID(d.ID.String()),
							Size:     gqltypes.Uint64(p.DealInfo.PieceActivationManifest.Size),
							IsDirect: true,
						})
						used += uint64(p.DealInfo.PieceActivationManifest.Size)
						continue
					}
				}
				continue
			}

			dcid, err := p.DealInfo.DealProposal.Cid()
			if err != nil {
				return nil, err
			}

			ds, err := r.dealsByPublishCID(ctx, *publishCid)
			if err != nil {
				return nil, err
			}

			var i int
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

			// we matched the deal from piece with a deal from the boost db
			// single deal in publish message; i == 0; len(ds) == 1;
			// multiple deals in publish message; i == smth; len(ds) > 1;
			if i < len(ds) {
				deals = append(deals, &waitDeal{
					ID:       graphql.ID(ds[i].DealUuid.String()),
					Size:     gqltypes.Uint64(p.Piece.Size),
					IsLegacy: false,
				})
				used += uint64(p.Piece.Size)
				continue
			}

			// match not found in boost db - fallback to legacy deals list
			lds, err := r.legacyDeals.ByPublishCid(ctx, *publishCid)
			if err != nil {
				return nil, err
			}

			var j int
			for ; j < len(lds); j++ {
				l := lds[j]
				lpcid, err := l.Proposal.Cid()
				if err != nil {
					return nil, err
				}

				if lpcid.Equals(dcid) {
					break
				}
			}

			if j == len(lds) {
				log.Errorw("couldn't match deal to boost or legacy market deal based on publish cid and proposal cid", "publishCid", publishCid, "dealProposalCid", dcid)
				continue
			}

			deals = append(deals, &waitDeal{
				ID:       graphql.ID(lds[j].ProposalCid.String()),
				Size:     gqltypes.Uint64(p.Piece.Size),
				IsLegacy: true,
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
