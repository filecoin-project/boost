package gql

import (
	"context"
	"time"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/retrievalmarket/rtvllog"
	"github.com/graph-gophers/graphql-go"
)

type retrievalStateResolver struct {
	rtvllog.RetrievalDealState
	db *rtvllog.RetrievalLogDB
}

func (r *retrievalStateResolver) CreatedAt() graphql.Time {
	return graphql.Time{Time: r.RetrievalDealState.CreatedAt}
}

func (r *retrievalStateResolver) UpdatedAt() graphql.Time {
	return graphql.Time{Time: r.RetrievalDealState.UpdatedAt}
}

func (r *retrievalStateResolver) DealID() gqltypes.Uint64 {
	return gqltypes.Uint64(r.RetrievalDealState.DealID)
}

func (r *retrievalStateResolver) TransferID() gqltypes.Uint64 {
	return gqltypes.Uint64(r.RetrievalDealState.TransferID)
}

func (r *retrievalStateResolver) PeerID() string {
	return r.RetrievalDealState.PeerID.String()
}

func (r *retrievalStateResolver) PayloadCID() string {
	return r.RetrievalDealState.PayloadCID.String()
}

func (r *retrievalStateResolver) PieceCid() string {
	if r.RetrievalDealState.PieceCID == nil {
		return ""
	}
	return r.RetrievalDealState.PieceCID.String()
}

func (r *retrievalStateResolver) PaymentInterval() gqltypes.Uint64 {
	return gqltypes.Uint64(r.RetrievalDealState.PaymentInterval)
}

func (r *retrievalStateResolver) PaymentIntervalIncrease() gqltypes.Uint64 {
	return gqltypes.Uint64(r.RetrievalDealState.PaymentIntervalIncrease)
}

func (r *retrievalStateResolver) PricePerByte() gqltypes.BigInt {
	return gqltypes.BigInt{Int: r.RetrievalDealState.PricePerByte}
}

func (r *retrievalStateResolver) UnsealPrice() gqltypes.BigInt {
	return gqltypes.BigInt{Int: r.RetrievalDealState.UnsealPrice}
}

func (r *retrievalStateResolver) TotalSent() gqltypes.Uint64 {
	return gqltypes.Uint64(r.RetrievalDealState.TotalSent)
}

func (r *retrievalStateResolver) DTEvents(ctx context.Context) ([]*retrievalDTEventResolver, error) {
	if r.RetrievalDealState.TransferID == 0 || r.RetrievalDealState.LocalPeerID == "" {
		return nil, nil
	}

	pid := r.RetrievalDealState.PeerID.String()
	evts, err := r.db.ListDTEvents(ctx, pid, r.RetrievalDealState.TransferID)
	if err != nil {
		log.Warnw("getting data-transfer events for retrieval %s/%d: %s", pid, r.RetrievalDealState.TransferID, err)
		return nil, nil
	}

	evtResolvers := make([]*retrievalDTEventResolver, 0, len(evts))
	for _, evt := range evts {
		evtResolvers = append(evtResolvers, &retrievalDTEventResolver{DTEvent: evt})
	}
	return evtResolvers, nil
}

type retrievalDTEventResolver struct {
	rtvllog.DTEvent
}

func (r *retrievalDTEventResolver) CreatedAt() graphql.Time {
	return graphql.Time{Time: r.DTEvent.CreatedAt}
}

type retrievalStateListResolver struct {
	TotalCount int32
	Logs       []*retrievalStateResolver
	More       bool
}

type retLogArgs struct {
	PeerID string
	DealID gqltypes.Uint64
}

func (r *resolver) RetrievalLog(ctx context.Context, args retLogArgs) (*retrievalStateResolver, error) {
	st, err := r.retDB.Get(ctx, args.PeerID, uint64(args.DealID))
	if err != nil {
		return nil, err
	}

	return &retrievalStateResolver{RetrievalDealState: *st, db: r.retDB}, nil
}

type retrievalStatesArgs struct {
	Cursor *gqltypes.BigInt // CreatedAt in milli-seconds
	Offset graphql.NullInt
	Limit  graphql.NullInt
}

func (r *resolver) RetrievalLogs(ctx context.Context, args retrievalStatesArgs) (*retrievalStateListResolver, error) {
	offset := 0
	if args.Offset.Set && args.Offset.Value != nil && *args.Offset.Value > 0 {
		offset = int(*args.Offset.Value)
	}

	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	// Fetch one extra row so that we can check if there are more rows
	// beyond the limit
	var cursor *time.Time
	if args.Cursor != nil {
		val := (*args.Cursor).Int64()
		asTime := time.Unix(val/1000, (val%1000)*1e6)
		cursor = &asTime
	}
	rows, err := r.retDB.List(ctx, cursor, offset, limit+1)
	if err != nil {
		return nil, err
	}
	more := len(rows) > limit
	if more {
		// Truncate list to limit
		rows = rows[:limit]
	}

	// Get the total row count
	count, err := r.retDB.Count(ctx)
	if err != nil {
		return nil, err
	}

	resolvers := make([]*retrievalStateResolver, 0, len(rows))
	for _, row := range rows {
		resolvers = append(resolvers, &retrievalStateResolver{RetrievalDealState: row, db: r.retDB})
	}

	return &retrievalStateListResolver{
		TotalCount: int32(count),
		Logs:       resolvers,
		More:       more,
	}, nil
}

type retStateCount struct {
	Count  int32
	Period gqltypes.Uint64
}

func (r *resolver) RetrievalLogsCount(ctx context.Context) (*retStateCount, error) {
	count, err := r.retDB.Count(ctx)
	return &retStateCount{
		Count:  int32(count),
		Period: gqltypes.Uint64(r.cfg.Dealmaking.RetrievalLogDuration),
	}, err
}
