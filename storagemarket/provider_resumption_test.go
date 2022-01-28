package storagemarket

import (
	"context"
	"testing"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/stretchr/testify/require"
)

func TestTransferCompletionOnProviderResumption(t *testing.T) {
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t, ctx)
	defer harness.Stop()
	// start the provider test harness
	harness.Start(t, ctx)

	// build the deal proposal with the blocking http test server and a non-blocking miner stub
	td := harness.newDealBuilder(t, 1).withAllMinerCallsNonBlocking().withBlockingHttpServer().build()
	require.NoError(t, td.executeAndSubscribeToNotifs())

	// wait for deal to be transferring
	td.waitForAndAssert(t, ctx, dealcheckpoints.Accepted)

	// now shutdown and resume the provider
	harness.shutdownAndResumeProvider(t, ctx)
	// assert storage and fund manager state after resumption
	harness.AssertStorageAndFundManagerState(t, ctx, td.params.Transfer.Size, harness.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)

	// unblock deal transfer and subscribe to notifications again as we've restarted the provider because the old subscription is invalid.
	require.NoError(t, td.updateSubscription())
	td.unblockTransfer()

	// subscribe again -> deal should finish now
	td.waitForAndAssert(t, ctx, dealcheckpoints.AddedPiece)
	// assert funds and storage are no longer tagged
	harness.EventuallyAssertNoTagged(t, ctx)
}
