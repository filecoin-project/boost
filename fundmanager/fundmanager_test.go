package fundmanager

import (
	"context"
	"database/sql"
	"path"
	"testing"

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
	logging.SetLogLevel("funds", "debug")

	req := require.New(t)
	ctx := context.Background()

	tmpFile := path.Join(t.TempDir(), "test.db")
	//fmt.Println(tmpFile)

	sqldb, err := sql.Open("sqlite3", "file:"+tmpFile)
	req.NoError(err)

	dealsDb := db.NewDealsDB(sqldb)
	req.NoError(err)

	// TODO: move somewhere generic (not on deals DB)
	dealsDb.Init(ctx)
	req.NoError(err)

	fundsDB := db.NewFundsDB(sqldb)

	api := &mockApi{}
	fm := &FundManager{
		api: api,
		db:  fundsDB,
		cfg: Config{
			EscrowWallet: address.TestAddress,
			PubMsgWallet: address.TestAddress2,
			PubMsgBalMin: abi.NewTokenAmount(10),
		},
	}

	total, err := fm.TotalTagged(ctx)
	req.NoError(err)
	req.EqualValues(0, total.Collateral.Int64())
	req.EqualValues(0, total.PubMsg.Int64())

	deals, err := db.GenerateDeals()
	req.NoError(err)

	deal := deals[0]
	prop := deal.ClientDealProposal.Proposal
	prop.ProviderCollateral = abi.NewTokenAmount(3)
	err = fm.TagFunds(ctx, deal.DealUuid, prop)
	req.NoError(err)

	total, err = fm.TotalTagged(ctx)
	req.NoError(err)
	req.EqualValues(3, total.Collateral.Int64())
	req.EqualValues(10, total.PubMsg.Int64())

	deal2 := deals[1]
	prop2 := deal2.ClientDealProposal.Proposal
	prop2.ProviderCollateral = abi.NewTokenAmount(4)
	err = fm.TagFunds(ctx, deal2.DealUuid, prop2)
	req.NoError(err)

	total, err = fm.TotalTagged(ctx)
	req.NoError(err)
	req.EqualValues(7, total.Collateral.Int64())
	req.EqualValues(20, total.PubMsg.Int64())

	err = fm.UntagFunds(ctx, deal2.DealUuid)
	req.NoError(err)

	total, err = fm.TotalTagged(ctx)
	req.NoError(err)
	req.EqualValues(3, total.Collateral.Int64())
	req.EqualValues(10, total.PubMsg.Int64())
}

type mockApi struct {
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
