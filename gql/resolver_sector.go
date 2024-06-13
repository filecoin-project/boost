package gql

import (
	"context"

	"github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
)

type sectorStatusResolver struct {
	si api.SectorInfo
}

func (r *sectorStatusResolver) Number() types.Uint64 {
	return types.Uint64(r.si.SectorID)
}

func (r *sectorStatusResolver) State() string {
	return string(r.si.State)
}

func (r *sectorStatusResolver) DealIDs() []types.Uint64 {
	ids := make([]types.Uint64, 0, len(r.si.Deals))
	for _, id := range r.si.Deals {
		ids = append(ids, types.Uint64(id))
	}
	return ids
}

func (r *resolver) SectorStatus(ctx context.Context, args struct{ SectorNumber types.Uint64 }) (*sectorStatusResolver, error) {
	sec := abi.SectorNumber(args.SectorNumber)
	si, err := r.spApi.SectorsStatus(ctx, sec, false)
	if err != nil {
		return nil, err
	}

	return &sectorStatusResolver{si: si}, nil
}
