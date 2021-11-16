package gql

import (
	"context"
	"fmt"

	"github.com/filecoin-project/boost/storagemarket"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
	"golang.org/x/xerrors"
)

type dealListResolver struct {
	TotalCount int32
	Next       *graphql.ID
	Deals      []*dealResolver
}

// resolver translates from a request for a graphql field to the data for
// that field
type resolver struct {
	ctx      context.Context
	dealsDB  *db.DealsDB
	provider *storagemarket.Provider
}

func newResolver(ctx context.Context, dealsDB *db.DealsDB, provider *storagemarket.Provider) *resolver {
	r := &resolver{
		ctx:      ctx,
		dealsDB:  dealsDB,
		provider: provider,
	}

	return r
}

type storageResolver struct {
	Name     string
	Capacity float64
	Used     float64
}

// query: storage: [Storage]
func (r *resolver) Storage(ctx context.Context) ([]*storageResolver, error) {
	// TODO: Get these values from storage space manager
	return []*storageResolver{{
		Name:     "Free",
		Capacity: 8.5 * 1024 * 1024 * 1024,
		Used:     0,
	}, {
		Name:     "Transferring",
		Capacity: 5.2 * 1024 * 1024 * 1024,
		Used:     3.5 * 1024 * 1024 * 1024,
	}, {
		Name:     "Queued",
		Capacity: 11.2 * 1024 * 1024 * 1024,
		Used:     0,
	}}, nil
}

// query: deal(id) Deal
func (r *resolver) Deal(ctx context.Context, args struct{ ID graphql.ID }) (*dealResolver, error) {
	id, err := toUuid(args.ID)
	if err != nil {
		return nil, err
	}

	deal, err := r.dealByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return newDealResolver(deal, r.dealsDB), nil
}

type dealsArgs struct {
	First *graphql.ID
	Limit graphql.NullInt
}

// query: deals(first, limit) DealList
func (r *resolver) Deals(ctx context.Context, args dealsArgs) (*dealListResolver, error) {
	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	deals, count, next, err := r.dealList(ctx, args.First, limit)
	if err != nil {
		return nil, err
	}

	resolvers := make([]*dealResolver, 0, len(deals))
	for _, deal := range deals {
		resolvers = append(resolvers, newDealResolver(&deal, r.dealsDB))
	}

	var nextID *graphql.ID
	if next != nil {
		gqlid := graphql.ID(next.String())
		nextID = &gqlid
	}
	return &dealListResolver{
		TotalCount: int32(count),
		Next:       nextID,
		Deals:      resolvers,
	}, nil
}

// subscription: dealUpdate(id) <-chan Deal
func (r *resolver) DealUpdate(ctx context.Context, args struct{ ID graphql.ID }) (<-chan *dealResolver, error) {
	dealUuid, err := toUuid(args.ID)
	if err != nil {
		return nil, err
	}

	c := make(chan *dealResolver, 1)

	sub, err := r.provider.SubscribeDealUpdates(dealUuid)
	if err != nil {
		if xerrors.Is(err, storagemarket.ErrDealHandlerFound) {
			close(c)
			return c, nil
		}
		return nil, xerrors.Errorf("%s: subscribing to deal updates: %w", args.ID, err)
	}

	// Send an update to the client with the initial state
	deal, err := r.dealByID(ctx, dealUuid)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c <- newDealResolver(deal, r.dealsDB):
	}

	// Updates to deal state are broadcast on pubsub. Pipe these updates to the
	// deal subscription channel returned by this method.
	go func() {
		// When the connection ends, unsubscribe
		defer sub.Close()

		for {
			select {
			case <-ctx.Done():
				// Connection closed
				return

			// Deal updated
			case evti := <-sub.Out():
				// Pipe the update to the deal subscription channel
				di := evti.(types.ProviderDealInfo)
				rsv := newDealResolver(&di, r.dealsDB)

				select {
				case <-ctx.Done():
					return

				case c <- rsv:
				}
			}
		}
	}()

	return c, nil
}

// subscription: dealNew() <-chan Deal
func (r *resolver) DealNew(ctx context.Context) (<-chan *dealResolver, error) {
	c := make(chan *dealResolver, 1)

	sub, err := r.provider.SubscribeNewDeals()
	if err != nil {
		return nil, xerrors.Errorf("subscribing to new deal events: %w", err)
	}

	// New deals are broadcast on pubsub. Pipe these deals to the
	// new deal subscription channel returned by this method.
	go func() {
		// When the connection ends, unsubscribe
		defer sub.Close()

		for {
			select {
			case <-ctx.Done():
				// Connection closed
				return

			// New deal
			case evti := <-sub.Out():
				// Pipe the deal to the new deal channel
				di := evti.(types.ProviderDealInfo)
				rsv := newDealResolver(&di, r.dealsDB)

				select {
				case <-ctx.Done():
					return

				case c <- rsv:
				}
			}
		}
	}()

	return c, nil
}

// mutation: dealCancel(id): ID
func (r *resolver) DealCancel(ctx context.Context, args struct{ ID graphql.ID }) (graphql.ID, error) {
	dealUuid, err := toUuid(args.ID)
	if err != nil {
		return args.ID, err
	}

	err = r.provider.CancelDeal(ctx, dealUuid)
	return args.ID, err
}

func (r *resolver) dealByID(ctx context.Context, dealUuid uuid.UUID) (*types.ProviderDealInfo, error) {
	deal, err := r.dealsDB.ByID(ctx, dealUuid)
	if err != nil {
		return nil, err
	}

	return &types.ProviderDealInfo{
		Deal:        deal,
		Transferred: r.provider.Transport.Transferred(deal.DealUuid),
	}, nil
}

func (r *resolver) dealList(ctx context.Context, first *graphql.ID, limit int) ([]types.ProviderDealInfo, int, *uuid.UUID, error) {
	// Get one extra deal so we can get the first deal UUID of the next page
	allDeals, err := r.dealsDB.List(ctx, first, limit+1)
	if err != nil {
		return nil, 0, nil, err
	}

	deals := allDeals
	var nextDealUuid *uuid.UUID
	// If there was more than one page of deals available
	if len(allDeals) > limit {
		// Get the first deal UUID of the next page
		nextDealUuid = &allDeals[len(allDeals)-1].DealUuid
		// Filter for deals on this page
		deals = allDeals[:limit]
	}

	// Get the total deal count
	count, err := r.dealsDB.Count(ctx)
	if err != nil {
		return nil, 0, nil, err
	}

	// Include data transfer information with the deal
	dis := make([]types.ProviderDealInfo, 0, len(deals))
	for _, deal := range deals {
		dis = append(dis, types.ProviderDealInfo{
			Deal:        deal,
			Transferred: r.provider.Transport.Transferred(deal.DealUuid),
		})
	}

	return dis, count, nextDealUuid, nil
}

type dealResolver struct {
	types.ProviderDealState
	transferred uint64
	dealsDB     *db.DealsDB
}

func newDealResolver(deal *types.ProviderDealInfo, dealsDB *db.DealsDB) *dealResolver {
	return &dealResolver{
		ProviderDealState: *deal.Deal,
		transferred:       deal.Transferred,
		dealsDB:           dealsDB,
	}
}

func (dr *dealResolver) ID() graphql.ID {
	return graphql.ID(dr.ProviderDealState.DealUuid.String())
}

func (dr *dealResolver) CreatedAt() graphql.Time {
	return graphql.Time{Time: dr.ProviderDealState.CreatedAt}
}

func (dr *dealResolver) ClientAddress() string {
	return dr.ProviderDealState.ClientDealProposal.Proposal.Client.String()
}

func (dr *dealResolver) ProviderAddress() string {
	return dr.ProviderDealState.ClientDealProposal.Proposal.Provider.String()
}

func (dr *dealResolver) PieceSize() float64 {
	return float64(dr.ProviderDealState.ClientDealProposal.Proposal.PieceSize)
}

func (dr *dealResolver) ProviderCollateral() float64 {
	return float64(dr.ProviderDealState.ClientDealProposal.Proposal.ProviderCollateral.Int64())
}

func (dr *dealResolver) StartEpoch() float64 {
	return float64(dr.ProviderDealState.ClientDealProposal.Proposal.StartEpoch)
}

func (dr *dealResolver) EndEpoch() float64 {
	return float64(dr.ProviderDealState.ClientDealProposal.Proposal.EndEpoch)
}

func (dr *dealResolver) PieceCid() string {
	return dr.ProviderDealState.ClientDealProposal.Proposal.PieceCID.String()
}

func (dr *dealResolver) Message() string {
	switch dr.Checkpoint {
	case dealcheckpoints.New:
		switch dr.transferred {
		case 0:
			return "Transfer queued"
		case 100:
			return "Transfer Complete"
		default:
			pct := (100 * dr.transferred) / uint64(dr.ClientDealProposal.Proposal.PieceSize)
			return fmt.Sprintf("Transferring %d%%", pct)
		}
	case dealcheckpoints.Transferred:
		return "Publishing"
	case dealcheckpoints.AddedPiece:
		return "Sealing"
	case dealcheckpoints.Complete:
		switch dr.Err {
		case "":
			return "Complete"
		case "Cancelled":
			return "Cancelled"
		}
		return "Error: " + dr.Err
	}
	return dr.ProviderDealState.Checkpoint.String()
}

func (dr *dealResolver) Logs(ctx context.Context) ([]*logsResolver, error) {
	logs, err := dr.dealsDB.Logs(ctx, dr.ProviderDealState.DealUuid)
	if err != nil {
		return nil, err
	}

	logResolvers := make([]*logsResolver, 0, len(logs))
	for _, l := range logs {
		logResolvers = append(logResolvers, &logsResolver{l})
	}
	return logResolvers, nil
}

type logsResolver struct {
	db.DealLog
}

func (lr *logsResolver) DealID() graphql.ID {
	return graphql.ID(lr.DealLog.DealUuid.String())
}

func (lr *logsResolver) CreatedAt() graphql.Time {
	return graphql.Time{Time: lr.DealLog.CreatedAt}
}

func toUuid(id graphql.ID) (uuid.UUID, error) {
	var dealUuid uuid.UUID
	err := dealUuid.UnmarshalText([]byte(id))
	if err != nil {
		return uuid.UUID{}, xerrors.Errorf("parsing graphql ID '%s' as UUID: %w", id, err)
	}
	return dealUuid, nil
}
