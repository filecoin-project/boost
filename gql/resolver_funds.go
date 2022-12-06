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

	balCollateral, err := r.fundMgr.BalanceDealCollateral(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting deal collateral balance: %w", err)
	}

	return &funds{
		Escrow: fundsEscrow{
			Tagged:    gqltypes.BigInt{Int: tagged.Collateral},
			Available: gqltypes.BigInt{Int: balMkt.Available},
			Locked:    gqltypes.BigInt{Int: balMkt.Locked},
		},
		Collateral: fundsWallet{
			Address: r.fundMgr.AddressDealCollateral().String(),
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
	Logs       []*fundsLogResolver
	More       bool
}

type fundsLogResolver struct {
	CreatedAt graphql.Time
	DealUUID  graphql.ID
	Amount    gqltypes.BigInt
	Text      string
}

type fundsLogsArgs struct {
	Cursor *gqltypes.BigInt // CreatedAt in milli-seconds
	Offset graphql.NullInt
	Limit  graphql.NullInt
}

// query: fundsLogs: FundsLogList
func (r *resolver) FundsLogs(ctx context.Context, args fundsLogsArgs) (*fundsLogList, error) {
	offset := 0
	if args.Offset.Set && args.Offset.Value != nil && *args.Offset.Value > 0 {
		offset = int(*args.Offset.Value)
	}

	limit := 10
	if args.Limit.Set && args.Limit.Value != nil && *args.Limit.Value > 0 {
		limit = int(*args.Limit.Value)
	}

	// Fetch one extra log so that we can check if there are more logs
	// beyond the limit
	cursor := bigIntToTime(args.Cursor)
	logs, err := r.fundsDB.Logs(ctx, cursor, offset, limit+1)
	if err != nil {
		return nil, fmt.Errorf("getting funds logs: %w", err)
	}

	more := len(logs) > limit
	if more {
		// Truncate log list to limit
		logs = logs[:limit]
	}

	// Get the total log count
	count, err := r.fundsDB.LogsCount(ctx)
	if err != nil {
		return nil, err
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
		TotalCount: int32(count),
		More:       more,
	}, nil
}

// mutation: moveFundsToEscrow(amount): Boolean
func (r *resolver) FundsMoveToEscrow(ctx context.Context, args struct{ Amount gqltypes.BigInt }) (bool, error) {
	_, err := r.fundMgr.MoveFundsToEscrow(ctx, args.Amount.Int)
	return true, err
}
