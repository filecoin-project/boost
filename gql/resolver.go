package gql

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/graph-gophers/graphql-go"

	"github.com/filecoin-project/boost/db"
)

// resolver translates from a request for a graphql field to the data for
// that field
type resolver struct {
	dealsDB *db.DealsDB
}

func newResolver(dealsDB *db.DealsDB) *resolver {
	return &resolver{dealsDB: dealsDB}
}

// query: deal(id) Deal
func (r *resolver) Deal(args struct{ ID graphql.ID }) (*dealResolver, error) {
	deal, err := r.dealsDB.ByID(string(args.ID))
	if err != nil {
		return nil, err
	}

	return &dealResolver{Deal: *deal, dealsDB: r.dealsDB}, nil
}

// query: deals() []Deal
func (r *resolver) Deals(ctx context.Context) (*[]*dealResolver, error) {
	deals, err := r.dealsDB.List(ctx)
	if err != nil {
		return nil, err
	}

	resolvers := make([]*dealResolver, 0, len(deals))
	for _, deal := range deals {
		resolvers = append(resolvers, &dealResolver{Deal: *deal, dealsDB: r.dealsDB})
	}
	return &resolvers, nil
}

// subscription: dealSub(id) <-chan Deal
func (r *resolver) DealSub(args struct{ ID graphql.ID }) (<-chan *dealResolver, error) {
	c := make(chan *dealResolver)

	deal, err := r.dealsDB.ByID(string(args.ID))
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(c)

		if deal.State != "Transferring" {
			return
		}

		for i := 0; i < 20; i++ {
			dl := *deal
			dl.State = fmt.Sprintf("Transferring %d%%", i*5+rand.Intn(5))
			c <- &dealResolver{Deal: dl, dealsDB: r.dealsDB}
			time.Sleep(500*time.Millisecond + time.Duration(rand.Intn(1000))*time.Millisecond)
		}
		dl := *deal
		dl.State = "Transfer Complete"
		c <- &dealResolver{Deal: dl, dealsDB: r.dealsDB}
	}()

	return c, nil
}

type dealResolver struct {
	db.Deal
	dealsDB *db.DealsDB
}

func (dr *dealResolver) ID() graphql.ID {
	return graphql.ID(dr.Deal.ID)
}

func (dr *dealResolver) CreatedAt() graphql.Time {
	return graphql.Time{Time: dr.Deal.CreatedAt}
}

func (dr *dealResolver) PieceSize() float64 {
	return float64(dr.Deal.PieceSize)
}

func (dr *dealResolver) ProviderCollateral() float64 {
	return float64(dr.Deal.ProviderCollateral)
}

func (dr *dealResolver) StartEpoch() float64 {
	return float64(dr.Deal.StartEpoch)
}

func (dr *dealResolver) EndEpoch() float64 {
	return float64(dr.Deal.StartEpoch)
}

func (dr *dealResolver) Logs(ctx context.Context) ([]*logsResolver, error) {
	logs, err := dr.dealsDB.Logs(ctx, dr.Deal.ID)
	if err != nil {
		return nil, err
	}

	logResolvers := make([]*logsResolver, 0, len(logs))
	for _, l := range logs {
		logResolvers = append(logResolvers, &logsResolver{l})
	}
	return logResolvers, nil
}

func (dr *dealResolver) PieceCid() string {
	return dr.Deal.PieceCid.String()
}

type logsResolver struct {
	db.DealLog
}

func (lr *logsResolver) DealID() graphql.ID {
	return graphql.ID(lr.DealLog.DealID)
}

func (lr *logsResolver) CreatedAt() graphql.Time {
	return graphql.Time{Time: lr.DealLog.CreatedAt}
}
