package storagemarket

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/require"
)

func TestSimpleOfflineDealHappy(t *testing.T) {
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t, ctx)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// first make an offline deal proposal
	// build the deal proposal with the blocking http test server and a completely blocking miner stub
	td := harness.newDealBuilder(t, 1, withOfflineDeal()).withAllMinerCallsBlocking().build()

	// create a deal proposal for the offline deal
	pi, _, err := harness.Provider.ExecuteDeal(td.params, peer.ID(""))
	require.NoError(t, err)
	require.True(t, pi.Accepted)

	// execute deal
	require.NoError(t, td.executeAndSubscribeImportOfflineDeal())

	// wait for Accepted checkpoint
	td.waitForAndAssert(t, ctx, dealcheckpoints.Accepted)

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
