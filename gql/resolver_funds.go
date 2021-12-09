package gql

import (
	"context"
	"fmt"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/graph-gophers/graphql-go"
)

type fundsEscrow struct {
	Available gqltypes.BigInt
	Locked    gqltypes.BigInt
	Tagged    gqltypes.BigInt
}

type fundsWallet struct {
	Address string
	Balance gqltypes.BigInt
	Tagged  gqltypes.BigInt
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
			Tagged:    gqltypes.BigInt{Int: tagged.Collateral},
			Available: gqltypes.BigInt{Int: balMkt.Available},
			Locked:    gqltypes.BigInt{Int: balMkt.Locked},
		},
		Collateral: fundsWallet{
			Address: r.fundMgr.AddressPledgeCollateral().String(),
			Balance: gqltypes.BigInt{Int: balCollateral},
		},
		PubMsg: fundsWallet{
			Address: r.fundMgr.AddressPublishMsg().String(),
			Balance: gqltypes.BigInt{Int: balPubMsg},
			Tagged:  gqltypes.BigInt{Int: tagged.PubMsg},
		},
	}, nil
}

type fundsLogList struct {
	TotalCount int32
	Next       *graphql.Time
	Logs       []*fundsLogResolver
}

type fundsLogResolver struct {
	CreatedAt graphql.Time
	DealUUID  graphql.ID
	Amount    gqltypes.BigInt
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
			Amount:    gqltypes.BigInt{Int: l.Amount},
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
func (r *resolver) FundsMoveToEscrow(ctx context.Context, args struct{ Amount gqltypes.BigInt }) (bool, error) {
	_, err := r.fundMgr.MoveFundsToEscrow(ctx, args.Amount.Int)
	return true, err
}
