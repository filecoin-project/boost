package gql

import (
	"context"

	"github.com/filecoin-project/boost/db"
	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/graph-gophers/graphql-go"
)

type proposalLogResolver struct {
	db.ProposalLog
}

func (pl *proposalLogResolver) DealUUID() graphql.ID {
	return graphql.ID(pl.ProposalLog.DealUUID.String())
}

func (pl *proposalLogResolver) CreatedAt() graphql.Time {
	return graphql.Time{Time: pl.ProposalLog.CreatedAt}
}

func (pl *proposalLogResolver) ClientAddress() string {
	return pl.ProposalLog.ClientAddress.String()
}

func (pl *proposalLogResolver) PieceSize() gqltypes.Uint64 {
	return gqltypes.Uint64(pl.ProposalLog.PieceSize)
}

type proposalLogListResolver struct {
	TotalCount int32
	Logs       []*proposalLogResolver
	More       bool
}

type proposalLogsArgs struct {
	Accepted graphql.NullBool
	Cursor   *gqltypes.BigInt // CreatedAt in milli-seconds
	Offset   graphql.NullInt
	Limit    graphql.NullInt
}

// query: proposalLogs(accepted, cursor, offset, limit) ProposalLogList
func (r *resolver) ProposalLogs(ctx context.Context, args proposalLogsArgs) (*proposalLogListResolver, error) {
	offset := 0
	if args.Offset.Set && args.Offset.Value != nil && *args.Offset.Value > 0 {
		offset = int(*args.Offset.Value)
	}

	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	// Fetch one extra deal so that we can check if there are more deals
	// beyond the limit
	cursor := bigIntToTime(args.Cursor)
	logs, err := r.plDB.List(ctx, args.Accepted.Value, cursor, offset, limit+1)
	if err != nil {
		return nil, err
	}
	more := len(logs) > limit
	if more {
		// Truncate deal list to limit
		logs = logs[:limit]
	}

	// Get the total log count
	count, err := r.plDB.Count(ctx, args.Accepted.Value)
	if err != nil {
		return nil, err
	}

	resolvers := make([]*proposalLogResolver, 0, len(logs))
	for _, l := range logs {
		resolvers = append(resolvers, &proposalLogResolver{ProposalLog: l})
	}

	return &proposalLogListResolver{
		TotalCount: int32(count),
		Logs:       resolvers,
		More:       more,
	}, nil
}

type propLogCount struct {
	Accepted int32
	Rejected int32
	Period   gqltypes.Uint64
}

func (r *resolver) ProposalLogsCount(ctx context.Context) (*propLogCount, error) {
	total, err := r.plDB.Count(ctx, nil)
	if err != nil {
		return nil, err
	}

	accepted := true
	acceptCount, err := r.plDB.Count(ctx, &accepted)
	if err != nil {
		return nil, err
	}

	return &propLogCount{
		Accepted: int32(acceptCount),
		Rejected: int32(total - acceptCount),
		Period:   gqltypes.Uint64(r.cfg.Dealmaking.DealProposalLogDuration),
	}, nil
}
