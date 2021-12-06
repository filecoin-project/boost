package gql

import (
	"context"
	"fmt"
	"math/big"

	"github.com/filecoin-project/go-state-types/abi"

	stbig "github.com/filecoin-project/go-state-types/big"
	"github.com/graph-gophers/graphql-go"
)

type fundsEscrow struct {
	Available float64
	Locked    float64
	Tagged    float64
}

type fundsWallet struct {
	Address string
	Balance float64
	Tagged  float64
}

type funds struct {
	Escrow     fundsEscrow
	Collateral fundsWallet
	PubMsg     fundsWallet
}

// query: funds: Funds
func (r *resolver) Funds(ctx context.Context) (*funds, error) {
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

	return &funds{
		Escrow: fundsEscrow{
			Tagged:    toFloat64(tagged.Collateral),
			Available: toFloat64(balMkt.Available),
			Locked:    toFloat64(balMkt.Locked),
		},
		Collateral: fundsWallet{
			Address: r.fundMgr.AddressPledgeCollateral().String(),
			Balance: toFloat64(balCollateral),
		},
		PubMsg: fundsWallet{
			Address: r.fundMgr.AddressPublishMsg().String(),
			Balance: toFloat64(balPubMsg),
			Tagged:  toFloat64(tagged.PubMsg),
		},
	}, nil
}

func toFloat64(i abi.TokenAmount) float64 {
	f64, _ := new(big.Float).SetInt(i.Int).Float64()
	return f64
}

type fundsLogList struct {
	TotalCount int32
	Next       *graphql.Time
	Logs       []*fundsLogResolver
}

type fundsLogResolver struct {
	CreatedAt graphql.Time
	DealUUID  graphql.ID
	Amount    float64
	Text      string
}

// query: fundsLogs: FundsLogList
func (r *resolver) FundsLogs(ctx context.Context) (*fundsLogList, error) {
	logs, err := r.fundMgr.Logs(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting funds logs: %w", err)
	}

	fundsLogs := make([]*fundsLogResolver, 0, len(logs))
	for _, l := range logs {
		fundsLogs = append(fundsLogs, &fundsLogResolver{
			CreatedAt: graphql.Time{Time: l.CreatedAt},
			DealUUID:  graphql.ID(l.DealUUID.String()),
			Amount:    toFloat64(l.Amount),
			Text:      l.Text,
		})
	}

	return &fundsLogList{
		Logs:       fundsLogs,
		TotalCount: int32(len(logs)),
		Next:       nil,
	}, nil
}

// mutation: moveFundsToEscrow(amount): Boolean
func (r *resolver) FundsMoveToEscrow(ctx context.Context, args struct{ Amount float64 }) (bool, error) {
	amt := new(big.Int)
	new(big.Float).SetFloat64(args.Amount).Int(amt)
	_, err := r.fundMgr.MoveFundsToEscrow(ctx, stbig.Int{Int: amt})
	return true, err
}
