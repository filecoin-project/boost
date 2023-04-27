package fundmanager

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
)

func TestFundManager(t *testing.T) {
	_ = logging.SetLogLevel("funds", "debug")

	req := require.New(t)
	ctx := context.Background()

	sqldb := db.CreateTestTmpDB(t)
	require.NoError(t, db.CreateAllBoostTables(ctx, sqldb, sqldb))

	fundsDB := db.NewFundsDB(sqldb)

	api := &mockApi{}
	fm := &FundManager{
		api: api,
		db:  fundsDB,
		cfg: Config{
			Enabled:      true,
			StorageMiner: address.TestAddress,
			PubMsgWallet: address.TestAddress2,
			PubMsgBalMin: abi.NewTokenAmount(10),
		},
	}

	// There should be nothing tagged to start with
	total, err := fm.TotalTagged(ctx)
	req.NoError(err)
	req.EqualValues(0, total.Collateral.Int64())
	req.EqualValues(0, total.PubMsg.Int64())

	deals, err := db.GenerateDeals()
	req.NoError(err)

	// Tag funds for a deal with collateral 3
	deal := deals[0]
	prop := deal.ClientDealProposal.Proposal
	prop.ProviderCollateral = abi.NewTokenAmount(3)
	rsp, err := fm.TagFunds(ctx, deal.DealUuid, prop)
	req.NoError(err)
	req.NotNil(rsp)
	b, err := api.WalletBalance(ctx, address.TestAddress2)
	req.NoError(err)
	mb, err := api.StateMarketBalance(ctx, address.TestAddress2, types.TipSetKey{})
	req.NoError(err)
	avail := big.Sub(mb.Escrow, mb.Locked)

	ex := &TagFundsResp{
		Collateral:     prop.ProviderCollateral,
		PublishMessage: fm.cfg.PubMsgBalMin,

		TotalCollateral:     prop.ProviderCollateral,
		TotalPublishMessage: fm.cfg.PubMsgBalMin,

		AvailableCollateral:     big.Sub(avail, prop.ProviderCollateral),
		AvailablePublishMessage: big.Sub(b, fm.cfg.PubMsgBalMin),
	}
	req.Equal(ex, rsp)

	total, err = fm.TotalTagged(ctx)
	req.NoError(err)
	// Total tagged for collateral should be 3
	req.EqualValues(3, total.Collateral.Int64())
	// Total tagged for publish message should be PubMsgBalMin (ie 10)
	req.EqualValues(10, total.PubMsg.Int64())

	// Tag funds for a deal with collateral 4
	deal2 := deals[1]
	prop2 := deal2.ClientDealProposal.Proposal
	prop2.ProviderCollateral = abi.NewTokenAmount(4)
	rsp, err = fm.TagFunds(ctx, deal2.DealUuid, prop2)
	req.NoError(err)
	req.NotNil(rsp)

	total, err = fm.TotalTagged(ctx)
	req.NoError(err)
	// Total tagged for collateral should be 3 + 4 = 7
	req.EqualValues(7, total.Collateral.Int64())
	// Total tagged for publish message should be 2 x PubMsgBalMin (ie 20)
	req.EqualValues(20, total.PubMsg.Int64())

	// Untag second deal
	collat, pub, err := fm.UntagFunds(ctx, deal2.DealUuid)
	req.NoError(err)
	req.EqualValues(fm.cfg.PubMsgBalMin.Int64(), pub.Int64())
	req.EqualValues(prop2.ProviderCollateral.Int64(), collat.Int64())

	// Totals should go back to what they were before tagging the second deal
	total, err = fm.TotalTagged(ctx)
	req.NoError(err)
	req.EqualValues(3, total.Collateral.Int64())
	req.EqualValues(10, total.PubMsg.Int64())
}

func TestFundManagerDisabled(t *testing.T) {
	_ = logging.SetLogLevel("funds", "debug")

	req := require.New(t)
	ctx := context.Background()

	sqldb := db.CreateTestTmpDB(t)
	require.NoError(t, db.CreateAllBoostTables(ctx, sqldb, sqldb))

	fundsDB := db.NewFundsDB(sqldb)

	api := &mockApi{}
	fm := &FundManager{
		api: api,
		db:  fundsDB,
		cfg: Config{
			Enabled:      false,
			StorageMiner: address.TestAddress,
			PubMsgWallet: address.TestAddress2,
			PubMsgBalMin: abi.NewTokenAmount(10),
		},
	}

	// There should be nothing tagged to start with
	deals, err := db.GenerateDeals()
	req.NoError(err)

	// Tag funds for a deal with collateral 3
	deal := deals[0]
	prop := deal.ClientDealProposal.Proposal
	prop.ProviderCollateral = abi.NewTokenAmount(3)
	rsp, err := fm.TagFunds(ctx, deal.DealUuid, prop)
	req.NoError(err)
	req.NotNil(rsp)
	b, err := api.WalletBalance(ctx, address.TestAddress2)
	req.NoError(err)
	mb, err := api.StateMarketBalance(ctx, address.TestAddress2, types.TipSetKey{})
	req.NoError(err)
	avail := big.Sub(mb.Escrow, mb.Locked)

	ex := &TagFundsResp{
		Collateral:     abi.NewTokenAmount(0),
		PublishMessage: abi.NewTokenAmount(0),

		TotalCollateral:     abi.NewTokenAmount(0),
		TotalPublishMessage: abi.NewTokenAmount(0),

		AvailableCollateral:     avail,
		AvailablePublishMessage: b,
	}
	req.Equal(ex, rsp)

	total, err := fm.TotalTagged(ctx)
	req.NoError(err)
	// Total tagged for collateral and publish message should be 0
	req.EqualValues(0, total.Collateral.Int64())
	req.EqualValues(0, total.PubMsg.Int64())
}

type mockApi struct {
}

func (m mockApi) MarketAddBalance(ctx context.Context, wallet, addr address.Address, amt types.BigInt) (cid.Cid, error) {
	return cid.Undef, nil
}

func (m mockApi) StateMarketBalance(ctx context.Context, addr address.Address, tsk types.TipSetKey) (lapi.MarketBalance, error) {
	return lapi.MarketBalance{
		Escrow: big.NewInt(30),
		Locked: big.NewInt(20),
	}, nil
}

func (m mockApi) WalletBalance(ctx context.Context, a address.Address) (types.BigInt, error) {
	return big.NewInt(50), nil
}

var _ fundManagerAPI = (*mockApi)(nil)
