package gql

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemarket"

	"github.com/google/uuid"

	"github.com/graph-gophers/graphql-go"
)

type dealPublishResolver struct {
	dealsDB   *db.DealsDB
	publisher *storagemarket.DealPublisher

	dealIDs []uuid.UUID

	Start          graphql.Time
	Period         int32
	MaxDealsPerMsg int32
}

// query: dealPublish: DealPublish
func (r *resolver) DealPublish(ctx context.Context) (*dealPublishResolver, error) {
	pending := r.publisher.PendingDeals()

	return &dealPublishResolver{
		dealsDB:   r.dealsDB,
		publisher: r.publisher,

		dealIDs: pending.DealIDs,

		Period:         int32(pending.PublishPeriod.Seconds()),
		Start:          graphql.Time{Time: pending.PublishPeriodStart},
		MaxDealsPerMsg: int32(pending.MaxDealsPerMsg),
	}, nil
}

func (r *dealPublishResolver) Deals(ctx context.Context) ([]*dealResolver, error) {
	deals := make([]*dealResolver, 0, len(r.dealIDs))
	for _, id := range r.dealIDs {
		deal, err := r.dealsDB.ByID(ctx, id)
		if err != nil {
			return nil, xerrors.Errorf("getting deal from DB by ID: %w", err)
		}
		deals = append(deals, &dealResolver{
			ProviderDealState: *deal,
			dealsDB:           r.dealsDB,
		})
	}

	return deals, nil
}

// mutation: dealPublishNow(): bool
func (r *resolver) DealPublishNow(ctx context.Context) (bool, error) {
	r.publisher.ForcePublishPendingDeals()
	return true, nil
}
