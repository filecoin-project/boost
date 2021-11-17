package gql

import (
	"context"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
)

type fundAmount struct {
	Name     string
	Capacity float64
}

// query: funds: [FundAmount]
func (r *resolver) Funds(ctx context.Context) ([]*fundAmount, error) {
	// TODO: Get these values from funds manager
	return []*fundAmount{{
		Name:     "Available (Deal escrow)",
		Capacity: 5 * 1024 * 1024,
	}, {
		Name:     "Available (Publish message)",
		Capacity: 6 * 1024 * 1024,
	}, {
		Name:     "Reserved for ongoing deals",
		Capacity: 2 * 1024 * 1024,
	}, {
		Name:     "Locked for ongoing deals",
		Capacity: 3 * 1024 * 1024,
	}}, nil
}

type fundsLogList struct {
	TotalCount int32
	Next       *graphql.Time
}

type fundsLogResolver struct {
	CreatedAt graphql.Time
	DealID    graphql.ID
	Amount    float64
	Text      string
}

var fundLogs = []*fundsLogResolver{{
	CreatedAt: graphql.Time{Time: time.Now().Add(-time.Minute)},
	DealID:    graphql.ID(uuid.New().String()),
	Amount:    1e18 * rand.Float64(),
	Text:      "Reserved",
}, {
	CreatedAt: graphql.Time{Time: time.Now().Add(-time.Minute * 2)},
	DealID:    graphql.ID(uuid.New().String()),
	Amount:    1e18 * rand.Float64(),
	Text:      "Locked",
}, {
	CreatedAt: graphql.Time{Time: time.Now().Add(-time.Minute * 5)},
	DealID:    graphql.ID(uuid.New().String()),
	Amount:    1e18 * rand.Float64(),
	Text:      "Publish Storage Deals (10 deals)",
}, {
	CreatedAt: graphql.Time{Time: time.Now().Add(-time.Minute * 23)},
	DealID:    graphql.ID(uuid.New().String()),
	Amount:    1e18 * rand.Float64(),
	Text:      "Refunded (deal error)",
}}

// query: fundsLogs: FundsLogList
func (r *resolver) FundsLogs(ctx context.Context) (*fundsLogList, error) {
	return &fundsLogList{
		TotalCount: int32(len(fundLogs)),
		Next:       nil,
	}, nil
}

func (r *fundsLogList) Logs(ctx context.Context) ([]*fundsLogResolver, error) {
	return fundLogs, nil
}
