package gql

import (
	"context"
	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/graph-gophers/graphql-go"
)

type sectorStateResolver struct {
	SectorNumber  int32
	Status        string
	Unsealed      bool
	DealCount     int32
	Active        bool
	DealWeight    gqltypes.BigInt
	VerifiedPower gqltypes.BigInt
	Expiration    gqltypes.Uint64
	OnChain       bool
}

type sectorsListResolver struct {
	TotalCount int32
	Sectors    []*sectorStateResolver
	More       bool
	At         graphql.Time
}

type sectorsArgs struct {
	Cursor graphql.NullInt // SectorNumber
	Offset graphql.NullInt
	Limit  graphql.NullInt
}

// query: sectorsList(cursor, offset, limit) SectorsList
func (r *resolver) SectorsList(ctx context.Context, args sectorsArgs) (*sectorsListResolver, error) {
	offset := 0
	if args.Offset.Set && args.Offset.Value != nil && *args.Offset.Value > 0 {
		offset = int(*args.Offset.Value)
	}

	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	snapshot, err := r.sectorsList.Snapshot(ctx, offset, limit)
	if err != nil {
		return nil, err
	}

	resolvers := make([]*sectorStateResolver, 0, len(snapshot.List))
	for _, sector := range snapshot.List {
		resolvers = append(resolvers, &sectorStateResolver{
			SectorNumber:  int32(sector.SectorNumber),
			Status:        sector.State.String(),
			Unsealed:      sector.Unsealed,
			DealCount:     int32(len(sector.Deals)),
			Active:        sector.Active,
			DealWeight:    gqltypes.BigInt{Int: sector.DealWeight},
			VerifiedPower: gqltypes.BigInt{Int: sector.VerifiedPower},
			Expiration:    gqltypes.Uint64(sector.Expiration),
			OnChain:       sector.OnChain,
		})
	}

	return &sectorsListResolver{
		TotalCount: int32(snapshot.TotalCount),
		Sectors:    resolvers,
		More:       snapshot.More,
		At:         graphql.Time{Time: snapshot.At},
	}, nil
}

// mutation: sectorsListRefresh()
func (r *resolver) SectorsListRefresh(ctx context.Context) (bool, error) {
	return true, r.sectorsList.Refresh(ctx)
}
