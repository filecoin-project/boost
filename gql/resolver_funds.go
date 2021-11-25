package gql

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"time"

	stbig "github.com/filecoin-project/go-state-types/big"
	"github.com/google/uuid"
	"github.com/graph-gophers/graphql-go"
)

type fundAmount struct {
	Name   string
	Amount float64
}

// query: funds: [FundAmount]
func (r *resolver) Funds(ctx context.Context) ([]*fundAmount, error) {
	tagged, err := r.fundMgr.TotalTagged(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting total tagged: %w", err)
	}

	balMkt, err := r.fundMgr.BalanceMarket(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting market balance: %w", err)
	}

	balPubMsg, err := r.fundMgr.BalancePublishMsg(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting publish message balance: %w", err)
	}

	balCollateral, err := r.fundMgr.BalancePledgeCollateral(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting pledge collateral balance: %w", err)
	}

	escrowTagged, _ := new(big.Float).SetInt(tagged.Collateral.Int).Float64()
	pubMsgTagged, _ := new(big.Float).SetInt(tagged.PubMsg.Int).Float64()
	escrowAvail, _ := new(big.Float).SetInt(balMkt.Available.Int).Float64()
	escrowLocked, _ := new(big.Float).SetInt(balMkt.Locked.Int).Float64()
	pubMsgBalance, _ := new(big.Float).SetInt(balPubMsg.Int).Float64()
	collateralBalance, _ := new(big.Float).SetInt(balCollateral.Int).Float64()

	return []*fundAmount{{
		Name:   "escrow-available",
		Amount: escrowAvail,
	}, {
		Name:   "escrow-locked",
		Amount: escrowLocked,
	}, {
		Name:   "escrow-tagged",
		Amount: escrowTagged,
	}, {
		Name:   "collateral-balance",
		Amount: collateralBalance,
	}, {
		Name:   "publish-message-balance",
		Amount: pubMsgBalance,
	}, {
		Name:   "publish-message-tagged",
		Amount: pubMsgTagged,
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

// mutation: moveFundsToEscrow(amount): Boolean
func (r *resolver) FundsMoveToEscrow(ctx context.Context, args struct{ Amount float64 }) (bool, error) {
	amt := new(big.Int)
	new(big.Float).SetFloat64(args.Amount).Int(amt)
	_, err := r.fundMgr.MoveFundsToEscrow(ctx, stbig.Int{Int: amt})
	return true, err
}
