package gql

import (
	"context"
	"github.com/filecoin-project/boost/db"
	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/graph-gophers/graphql-go"
	"time"
)

type directDealResolver struct {
	types.DirectDeal
	transferred uint64
	dealsDB     *db.DealsDB
	logsDB      *db.LogsDB
	spApi       sealingpipeline.API
}

type directDealListResolver struct {
	TotalCount int32
	Deals      []*directDealResolver
	More       bool
}

// query: directDeals(query, filter, cursor, offset, limit) DirectDealList
func (r *resolver) DirectDeals(ctx context.Context, args dealsArgs) (*directDealListResolver, error) {
	offset := 0
	if args.Offset.Set && args.Offset.Value != nil && *args.Offset.Value > 0 {
		offset = int(*args.Offset.Value)
	}

	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	query := ""
	if args.Query.Set && args.Query.Value != nil {
		query = *args.Query.Value
	}

	var filter *db.FilterOptions
	if args.Filter != nil {
		filter = &db.FilterOptions{
			Checkpoint:   args.Filter.Checkpoint.Value,
			IsOffline:    args.Filter.IsOffline.Value,
			TransferType: args.Filter.TransferType.Value,
			IsVerified:   args.Filter.IsVerified.Value,
		}
	}

	// Fetch one extra deal so that we can check if there are more deals
	// beyond the limit
	deals, err := r.directDealsDB.List(ctx, query, filter, args.Cursor, offset, limit+1)
	if err != nil {
		return nil, err
	}
	more := len(deals) > limit
	if more {
		// Truncate deal list to limit
		deals = deals[:limit]
	}

	// Get the total deal count
	count, err := r.directDealsDB.Count(ctx, query, filter)
	if err != nil {
		return nil, err
	}

	// Include data transfer information with the deal
	//dis := make([]types.DirectDataEntry, 0, len(deals))
	//for _, deal := range deals {
	//	deal.NBytesReceived = int64(r.provider.NBytesReceived(deal.DealUuid))
	//	dis = append(dis, *deal)
	//}

	resolvers := make([]*directDealResolver, 0, len(deals))
	for _, deal := range deals {
		//deal.NBytesReceived = int64(r.provider.NBytesReceived(deal.DealUuid))
		resolvers = append(resolvers, &directDealResolver{
			DirectDeal:  *deal,
			transferred: 0, // TODO
			dealsDB:     r.dealsDB,
			logsDB:      r.logsDB,
			spApi:       r.spApi,
		})
	}

	return &directDealListResolver{
		TotalCount: int32(count),
		Deals:      resolvers,
		More:       more,
	}, nil
}

// query: directDeal(id) DirectDeal
func (r *resolver) DirectDeal(ctx context.Context, args struct{ ID graphql.ID }) (*directDealResolver, error) {
	id, err := toUuid(args.ID)
	if err != nil {
		return nil, err
	}

	deal, err := r.directDealsDB.ByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return &directDealResolver{
		DirectDeal:  *deal,
		transferred: 0, // TODO
		dealsDB:     r.dealsDB,
		logsDB:      r.logsDB,
		spApi:       r.spApi,
	}, nil
}

func (r *resolver) DirectDealsCount(ctx context.Context) (int32, error) {
	count, err := r.directDealsDB.Count(ctx, "", nil)
	if err != nil {
		return 0, err
	}

	return int32(count), nil
}

func (dr *directDealResolver) ID() graphql.ID {
	return graphql.ID(dr.DirectDeal.ID.String())
}

func (dr *directDealResolver) CreatedAt() graphql.Time {
	return graphql.Time{Time: dr.DirectDeal.CreatedAt}
}

func (dr *directDealResolver) ClientAddress() string {
	return dr.DirectDeal.Client.String()
}

func (dr *directDealResolver) ProviderAddress() string {
	return dr.DirectDeal.Provider.String()
}

func (dr *directDealResolver) KeepUnsealedCopy() bool {
	return dr.DirectDeal.KeepUnsealedCopy
}

func (dr *directDealResolver) AnnounceToIPNI() bool {
	return dr.DirectDeal.AnnounceToIPNI
}

func (dr *directDealResolver) PieceSize() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.DirectDeal.PieceSize)
}

func (dr *directDealResolver) AllocationID() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.DirectDeal.AllocationID)
}

func (dr *directDealResolver) Transferred() gqltypes.Uint64 {
	return gqltypes.Uint64(0) // TODO
}

func (dr *directDealResolver) Sector() *sectorResolver {
	return &sectorResolver{
		ID:     gqltypes.Uint64(dr.DirectDeal.SectorID),
		Offset: gqltypes.Uint64(dr.DirectDeal.Offset),
		Length: gqltypes.Uint64(dr.DirectDeal.Length),
	}
}

func (dr *directDealResolver) StartEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.DirectDeal.StartEpoch)
}

func (dr *directDealResolver) EndEpoch() gqltypes.Uint64 {
	return gqltypes.Uint64(dr.DirectDeal.EndEpoch)
}

func (dr *directDealResolver) PieceCid() string {
	return dr.DirectDeal.PieceCID.String()
}

func (dr *directDealResolver) Checkpoint() string {
	return dr.DirectDeal.Checkpoint.String()
}

func (dr *directDealResolver) CheckpointAt() graphql.Time {
	return graphql.Time{Time: dr.DirectDeal.CheckpointAt}
}

func (dr *directDealResolver) Retry() string {
	return string(dr.DirectDeal.Retry)
}

func (dr *directDealResolver) Message(ctx context.Context) string {
	msg := dr.message(ctx, dr.DirectDeal.Checkpoint, dr.DirectDeal.CheckpointAt)
	if dr.DirectDeal.Retry != types.DealRetryFatal && dr.DirectDeal.Err != "" {
		msg = "Paused at '" + msg + "': " + dr.DirectDeal.Err
	}
	return msg
}

func (dr *directDealResolver) message(ctx context.Context, checkpoint dealcheckpoints.Checkpoint, checkpointAt time.Time) string {
	switch checkpoint {
	case dealcheckpoints.Accepted:
		if dr.DirectDeal.InboundFilePath != "" {
			return "Verifying Commp"
		}
		return "Awaiting Direct Data Import"

	case dealcheckpoints.AddedPiece:
		return "Announcing"
	case dealcheckpoints.IndexedAndAnnounced:
		return dr.sealingState(ctx)
	case dealcheckpoints.Complete:
		switch dr.Err {
		case "":
			return "Complete"
		case "Cancelled":
			return "Cancelled"
		}
		return "Error: " + dr.Err
	}
	return checkpoint.String()
}

func (dr *directDealResolver) sealingState(ctx context.Context) string {
	si, err := dr.spApi.SectorsStatus(ctx, dr.SectorID, false)
	if err != nil {
		log.Warnw("error getting sealing status for sector", "sector", dr.SectorID, "error", err)
		return "Sealer: Sealing"
	}
	return "Sealer: " + string(si.State)
	// TODO: How to check that deal is in sector?
	//for _, d := range si.Deals {
	//	if d == dr.DirectDataEntry.ChainDealID {
	//		return "Sealer: " + string(si.State)
	//	}
	//}
	//return fmt.Sprintf("Sealer: failed - deal not found in sector %d", si.SectorID)
}

func (dr *directDealResolver) Logs(ctx context.Context) ([]*logsResolver, error) {
	logs, err := dr.logsDB.Logs(ctx, dr.DirectDeal.ID)
	if err != nil {
		return nil, err
	}

	logResolvers := make([]*logsResolver, 0, len(logs))
	for _, l := range logs {
		logResolvers = append(logResolvers, &logsResolver{l})
	}
	return logResolvers, nil
}
