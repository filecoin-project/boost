package fundmanager

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/types"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("funds")

type fundManagerAPI interface {
	MarketAddBalance(ctx context.Context, wallet, addr address.Address, amt types.BigInt) (cid.Cid, error)
	StateMarketBalance(ctx context.Context, addr address.Address, tsk types.TipSetKey) (api.MarketBalance, error)
	WalletBalance(context.Context, address.Address) (types.BigInt, error)
}

type Config struct {
	// Whether to enable fund tagging
	Enabled bool
	// The address of the storage miner, used as the target address when
	// moving funds to escrow
	StorageMiner address.Address
	// Wallet used as source of deal collateral when moving funds to
	// escrow
	CollatWallet address.Address
	// Wallet used to send the publish message (and pay gas fees)
	PubMsgWallet address.Address
	// How much to reserve for each publish message
	PubMsgBalMin abi.TokenAmount
}

type FundManager struct {
	api fundManagerAPI
	db  *db.FundsDB
	cfg Config
}

func New(cfg Config) func(api v1api.FullNode, fundsDB *db.FundsDB) *FundManager {
	return func(api api.FullNode, fundsDB *db.FundsDB) *FundManager {
		return &FundManager{
			api: api,
			db:  fundsDB,
			cfg: cfg,
		}
	}
}

type TagFundsResp struct {
	// The amount of deal collateral tagged for this deal
	Collateral abi.TokenAmount
	// The amount of publish message funds tagged for this deal
	PublishMessage abi.TokenAmount

	// The total amount of deal collateral tagged for all deals so far
	TotalCollateral abi.TokenAmount
	// The total amount of funds tagged for all publish messages so far
	TotalPublishMessage abi.TokenAmount

	// The total available funds for deal collateral
	AvailableCollateral abi.TokenAmount
	// The total available funds for deal publishing
	AvailablePublishMessage abi.TokenAmount
}

var ErrInsufficientFunds = errors.New("insufficient funds")

// TagFunds tags funds for deal collateral and for the publish storage
// deals message, so those funds cannot be used for other deals.
// It returns ErrInsufficientFunds if there are not enough funds available
// in the respective wallets to cover either of these operations.
func (m *FundManager) TagFunds(ctx context.Context, dealUuid uuid.UUID, proposal market.DealProposal) (*TagFundsResp, error) {
	marketBal, err := m.BalanceMarket(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting market balance: %w", err)
	}

	pubMsgBal, err := m.BalancePublishMsg(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting publish deals message wallet balance: %w", err)
	}

	// Check that the provider has enough funds in escrow to cover the
	// collateral requirement for the deal
	tagged, err := m.totalTagged(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting total tagged: %w", err)
	}

	dealCollateralTag := abi.NewTokenAmount(0)
	pubMsgTag := abi.NewTokenAmount(0)
	availForDealCollat := big.Sub(marketBal.Available, tagged.Collateral)
	availForPubMsg := big.Sub(pubMsgBal, tagged.PubMsg)
	if m.cfg.Enabled {
		dealCollateralTag = proposal.ProviderBalanceRequirement()
		if availForDealCollat.LessThan(dealCollateralTag) {
			err := fmt.Errorf("%w: available funds %d is less than collateral needed for deal %d: "+
				"available = funds in escrow %d - amount reserved for other deals %d",
				ErrInsufficientFunds, availForDealCollat, dealCollateralTag, marketBal.Available, tagged.Collateral)
			return nil, err
		}

		// Check that the provider has enough funds to send a PublishStorageDeals message
		pubMsgTag = m.cfg.PubMsgBalMin
		if availForPubMsg.LessThan(pubMsgTag) {
			err := fmt.Errorf("%w: available funds %d is less than needed for publish deals message %d: "+
				"available = funds in publish deals wallet %d - amount reserved for other deals %d",
				ErrInsufficientFunds, availForPubMsg, pubMsgTag, pubMsgBal, tagged.PubMsg)
			return nil, err
		}

		// Provider has enough funds to make deal, so persist tagged funds
		err = m.persistTagged(ctx, dealUuid, dealCollateralTag, pubMsgTag)
		if err != nil {
			return nil, fmt.Errorf("saving total tagged: %w", err)
		}
	}

	return &TagFundsResp{
		Collateral:     dealCollateralTag,
		PublishMessage: pubMsgTag,

		TotalPublishMessage: big.Add(tagged.PubMsg, pubMsgTag),
		TotalCollateral:     big.Add(tagged.Collateral, dealCollateralTag),

		AvailablePublishMessage: big.Sub(availForPubMsg, pubMsgTag),
		AvailableCollateral:     big.Sub(availForDealCollat, dealCollateralTag),
	}, nil
}

// TotalTagged returns the total funds tagged for specific deals for
// collateral and publish storage deals message
func (m *FundManager) TotalTagged(ctx context.Context) (*db.TotalTagged, error) {
	return m.totalTagged(ctx)
}

// unlocked
func (m *FundManager) totalTagged(ctx context.Context) (*db.TotalTagged, error) {
	total, err := m.db.TotalTagged(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting total tagged from DB: %w", err)
	}
	return total, nil
}

// UntagFunds untags funds that were associated (tagged) with a deal.
// It's called when it's no longer necessary to prevent the funds from being
// used for a different deal (eg because the deal failed / was published)
func (m *FundManager) UntagFunds(ctx context.Context, dealUuid uuid.UUID) (collat, pub abi.TokenAmount, err error) {
	untaggedCollat, untaggedPublish, err := m.db.Untag(ctx, dealUuid)
	if err != nil {
		return abi.NewTokenAmount(0), abi.NewTokenAmount(0), fmt.Errorf("persisting untag funds for deal to DB: %w", err)
	}

	tot := big.Add(untaggedCollat, untaggedPublish)

	fundsLog := &db.FundsLog{
		DealUUID: dealUuid,
		Text:     "Untag funds for deal",
		Amount:   tot,
	}
	err = m.db.InsertLog(ctx, fundsLog)
	if err != nil {
		return abi.NewTokenAmount(0), abi.NewTokenAmount(0), fmt.Errorf("persisting untag funds log to DB: %w", err)
	}

	log.Infow("untag", "id", dealUuid, "amount", tot)
	return untaggedCollat, untaggedPublish, nil
}

func (m *FundManager) persistTagged(ctx context.Context, dealUuid uuid.UUID, dealCollateral abi.TokenAmount, pubMsgBal abi.TokenAmount) error {
	err := m.db.Tag(ctx, dealUuid, dealCollateral, pubMsgBal)
	if err != nil {
		return fmt.Errorf("persisting tag funds for deal to DB: %w", err)
	}

	collatFundsLog := &db.FundsLog{
		DealUUID: dealUuid,
		Amount:   dealCollateral,
		Text:     "Tag funds for collateral",
	}
	pubMsgFundsLog := &db.FundsLog{
		DealUUID: dealUuid,
		Amount:   pubMsgBal,
		Text:     "Tag funds for deal publish message",
	}
	err = m.db.InsertLog(ctx, collatFundsLog, pubMsgFundsLog)
	if err != nil {
		return fmt.Errorf("persisting tag funds log to DB: %w", err)
	}

	log.Infow("tag", "id", dealUuid, "collateral", dealCollateral, "pubmsgbal", pubMsgBal)
	return nil
}

// MoveFundsToEscrow moves funds from the deal collateral wallet into escrow with
// the storage market actor
func (m *FundManager) MoveFundsToEscrow(ctx context.Context, amt abi.TokenAmount) (cid.Cid, error) {
	msgCid, err := m.api.MarketAddBalance(ctx, m.cfg.CollatWallet, m.cfg.StorageMiner, amt)
	if err != nil {
		return cid.Undef, fmt.Errorf("moving %d to escrow wallet %s: %w", amt, m.cfg.StorageMiner, err)
	}

	return msgCid, err
}

// BalanceMarket returns available and locked amounts in escrow
// (on chain with the Storage Market Actor)
func (m *FundManager) BalanceMarket(ctx context.Context) (storagemarket.Balance, error) {
	bal, err := m.api.StateMarketBalance(ctx, m.cfg.StorageMiner, types.EmptyTSK)
	if err != nil {
		return storagemarket.Balance{}, err
	}

	return toSharedBalance(bal), nil
}

// BalanceDealCollateral returns the amount of funds in the wallet used for
// collateral for deal making
func (m *FundManager) BalanceDealCollateral(ctx context.Context) (abi.TokenAmount, error) {
	return m.api.WalletBalance(ctx, m.cfg.CollatWallet)
}

func (m *FundManager) AddressDealCollateral() address.Address {
	return m.cfg.CollatWallet
}

// BalancePublishMsg returns the amount of funds in the wallet used to send
// publish storage deals messages
func (m *FundManager) BalancePublishMsg(ctx context.Context) (abi.TokenAmount, error) {
	return m.api.WalletBalance(ctx, m.cfg.PubMsgWallet)
}

func (m *FundManager) AddressPublishMsg() address.Address {
	return m.cfg.PubMsgWallet
}

func toSharedBalance(bal api.MarketBalance) storagemarket.Balance {
	return storagemarket.Balance{
		Locked:    bal.Locked,
		Available: big.Sub(bal.Escrow, bal.Locked),
	}
}
