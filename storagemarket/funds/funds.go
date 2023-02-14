package funds

import (
	"context"
	"fmt"

	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/go-state-types/abi"
)

type Status struct {
	// Funds in the Storage Market Actor
	Escrow SMAEscrow
	// Funds in the wallet used for deal collateral
	Collateral CollatWallet
	// Funds in the wallet used to pay for Publish Storage Deals messages
	PubMsg PubMsgWallet
}

type SMAEscrow struct {
	// Funds tagged for ongoing deals
	Tagged abi.TokenAmount
	// Funds in escrow available to be used for deal making
	Available abi.TokenAmount
	// Funds in escrow that are locked for ongoing deals
	Locked abi.TokenAmount
}

type CollatWallet struct {
	// The wallet address
	Address string
	// The wallet balance
	Balance abi.TokenAmount
}

type PubMsgWallet struct {
	// The wallet address
	Address string
	// The wallet balance
	Balance abi.TokenAmount
	// The funds that are tagged for ongoing deals
	Tagged abi.TokenAmount
}

func GetStatus(ctx context.Context, fm *fundmanager.FundManager) (*Status, error) {
	tagged, err := fm.TotalTagged(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting total tagged: %w", err)
	}

	balMkt, err := fm.BalanceMarket(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting market balance: %w", err)
	}

	balPubMsg, err := fm.BalancePublishMsg(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting publish message balance: %w", err)
	}

	balCollateral, err := fm.BalanceDealCollateral(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting pledge collateral balance: %w", err)
	}

	return &Status{
		Escrow: SMAEscrow{
			Tagged:    tagged.Collateral,
			Available: balMkt.Available,
			Locked:    balMkt.Locked,
		},
		Collateral: CollatWallet{
			Address: fm.AddressDealCollateral().String(),
			Balance: balCollateral,
		},
		PubMsg: PubMsgWallet{
			Address: fm.AddressPublishMsg().String(),
			Balance: balPubMsg,
			Tagged:  tagged.PubMsg,
		},
	}, nil
}
