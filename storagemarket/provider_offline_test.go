package storagemarket

import (
	"context"
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/require"
)

func TestSimpleOfflineDealHappy(t *testing.T) {
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// first make an offline deal proposal
	// build the deal proposal with the blocking http test server and a completely blocking miner stub
	td := harness.newDealBuilder(t, 1, withOfflineDeal()).withAllMinerCallsBlocking().build()

	// execute deal
	require.NoError(t, td.executeAndSubscribe())

	// wait for Accepted checkpoint
	td.waitForAndAssert(t, ctx, dealcheckpoints.Accepted)

	// import data for offline deal
	require.NoError(t, td.executeAndSubscribeImportOfflineDeal(false))

	// unblock commp -> wait for Transferred checkpoint
	td.unblockCommp()
	td.waitForAndAssert(t, ctx, dealcheckpoints.Transferred)

	// unblock publish -> wait for published checkpoint and assert
	td.unblockPublish()
	td.waitForAndAssert(t, ctx, dealcheckpoints.Published)
	harness.AssertStorageAndFundManagerState(t, ctx, 0, harness.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)

	// unblock publish confirmation -> wait for publish confirmed and assert
	td.unblockWaitForPublish()
	td.waitForAndAssert(t, ctx, dealcheckpoints.PublishConfirmed)
	harness.EventuallyAssertStorageFundState(t, ctx, 0, abi.NewTokenAmount(0), abi.NewTokenAmount(0))

	// unblock adding piece -> wait for piece to be added and assert
	td.unblockAddPiece()
	td.waitForAndAssert(t, ctx, dealcheckpoints.AddedPiece)
	harness.EventuallyAssertNoTagged(t, ctx)

	// assert logs
	lgs, err := harness.Provider.logsDB.Logs(ctx, td.params.DealUUID)
	require.NoError(t, err)
	require.NotEmpty(t, lgs)
}

func TestOfflineDealInsufficientProviderFunds(t *testing.T) {
	ctx := context.Background()

	// setup the provider test harness with configured publish fee per deal
	// that is more than the total wallet balance.
	harness := NewHarness(t, withMinPublishFees(abi.NewTokenAmount(100)), withPublishWalletBal(50))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// first make an offline deal proposal
	// build the deal proposal with the blocking http test server and a completely blocking miner stub
	td := harness.newDealBuilder(t, 1, withOfflineDeal()).withNoOpMinerStub().build()

	// create a deal proposal for the offline deal
	pi, err := harness.Provider.ExecuteDeal(context.Background(), td.params, peer.ID(""))
	require.NoError(t, err)
	require.True(t, pi.Accepted)

	// expect that when the deal data is imported, the import will fail because
	// there are not enough funds for the deal
	pi, err = td.ph.Provider.ImportOfflineDealData(context.Background(), td.params.DealUUID, td.carv2FilePath, false)
	require.NoError(t, err)
	require.False(t, pi.Accepted)
	require.Contains(t, pi.Reason, "insufficient funds")
}

func TestOfflineDealDataCleanup(t *testing.T) {
	ctx := context.Background()

	for _, delAfterImport := range []bool{true, false} {
		t.Run(fmt.Sprintf("delete after import: %t", delAfterImport), func(t *testing.T) {
			harness := NewHarness(t)
			harness.Start(t, ctx)
			defer harness.Stop()

			// first make an offline deal proposal
			td := harness.newDealBuilder(t, 1, withOfflineDeal()).withAllMinerCallsNonBlocking().build()

			// execute deal
			require.NoError(t, td.executeAndSubscribe())

			// wait for Accepted checkpoint
			td.waitForAndAssert(t, ctx, dealcheckpoints.Accepted)

			// import the deal data
			require.NoError(t, td.executeAndSubscribeImportOfflineDeal(delAfterImport))

			// check whether the deal data was removed after add piece
			td.waitForAndAssert(t, ctx, dealcheckpoints.AddedPiece)
			harness.EventuallyAssertNoTagged(t, ctx)
		})
	}
}
