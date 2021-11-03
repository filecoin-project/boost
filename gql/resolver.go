package gql

import (
	"context"
	"fmt"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	"github.com/cskr/pubsub"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("resolver")

// resolver translates from a request for a graphql field to the data for
// that field
type resolver struct {
	ctx      context.Context
	dealsDB  *db.DealsDB
	transfer *mockTransfer
	// TODO: pass dealUpdates pubsub through to the DB layer so that updates
	// are published whenever the deals table is updated
	dealUpdates *pubsub.PubSub
}

func newResolver(ctx context.Context, dealsDB *db.DealsDB) *resolver {
	r := &resolver{
		ctx:         ctx,
		dealsDB:     dealsDB,
		dealUpdates: pubsub.New(16),
		transfer:    newMockTransfer(),
	}

	dealsDB.OnUpdate(r.onDealUpdate)
	r.transfer.OnUpdate(r.onTransferUpdate)

	return r
}

// query: deal(id) Deal
func (r *resolver) Deal(ctx context.Context, args struct{ ID graphql.ID }) (*dealResolver, error) {
	deal, err := r.byID(ctx, args.ID)
	if err != nil {
		return nil, err
	}

	return &dealResolver{ProviderDealState: *deal, dealsDB: r.dealsDB}, nil
}

// query: deals() []Deal
func (r *resolver) Deals(ctx context.Context) (*[]*dealResolver, error) {
	deals, err := r.dealsDB.List(ctx)
	if err != nil {
		return nil, err
	}

	resolvers := make([]*dealResolver, 0, len(deals))
	for _, deal := range deals {
		resolvers = append(resolvers, &dealResolver{ProviderDealState: deal, dealsDB: r.dealsDB})
	}
	return &resolvers, nil
}

// subscription: dealSub(id) <-chan Deal
func (r *resolver) DealSub(ctx context.Context, args struct{ ID graphql.ID }) (<-chan *dealResolver, error) {
	c := make(chan *dealResolver, 1)

	sub := r.dealUpdates.Sub(string(args.ID))

	// Send an update to the client with the initial state
	deal, err := r.byID(ctx, args.ID)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c <- &dealResolver{ProviderDealState: *deal, dealsDB: r.dealsDB}:
	}

	// Updates to deal state are broadcast on pubsub. Pipe these updates to the
	// deal subscription channel returned by this method.
	go func() {
		// When the connection ends, unsubscribe
		defer func() {
			r.dealUpdates.Unsub(sub)
			for range sub {
				// drain the subscription
			}
		}()

		for {
			select {
			case <-ctx.Done():
				// Connection closed
				return

			// Deal updated
			case ri := <-sub:
				select {
				case <-ctx.Done():
					return

				// Pipe the update to the deal subscription channel
				case c <- ri.(*dealResolver):
				}
			}
		}
	}()

	// For demo purposes update state a few times
	go r.transfer.SimulateTransfer(ctx, deal)

	return c, nil
}

func (r *resolver) onDealUpdate(deal *types.ProviderDealState) {
	rsv := &dealResolver{
		ProviderDealState: *deal,
		dealsDB:           r.dealsDB,
		transfer:          r.transfer,
	}
	r.dealUpdates.Pub(rsv, deal.DealUuid.String())
}

func (r *resolver) onTransferUpdate(dealUuid uuid.UUID, transferred uint64) {
	deal, err := r.dealsDB.ByID(r.ctx, dealUuid)
	if err != nil {
		log.Warnf("failed to get deal %d from DB: %s", err)
	}

	r.onDealUpdate(deal)
}

func (r *resolver) byID(ctx context.Context, gqlid graphql.ID) (*types.ProviderDealState, error) {
	var id uuid.UUID
	err := id.UnmarshalText([]byte(gqlid))
	if err != nil {
		return nil, xerrors.Errorf("parsing graphql ID '%s' as UUID: %w", gqlid, err)
	}

	return r.dealsDB.ByID(ctx, id)
}

type dealResolver struct {
	types.ProviderDealState
	dealsDB  *db.DealsDB
	transfer *mockTransfer
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
	transferred := dr.transfer.Transferred(dr.DealUuid)
	if dr.Checkpoint == dealcheckpoints.New {
		switch transferred {
		case 0:
			return "Ready to transfer data"
		case 100:
			return "Transfer Complete"
		default:
			pct := (100 * transferred) / uint64(dr.ClientDealProposal.Proposal.PieceSize)
			return fmt.Sprintf("Transferring %d%%", pct)
		}
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
