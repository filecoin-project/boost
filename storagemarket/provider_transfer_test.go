package storagemarket

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/stretchr/testify/require"
)

func TestSingleDealResumptionDisconnect(t *testing.T) {
	ctx := context.Background()
	fileSize := (100 * 1048576) + 75 // ~100Mib

	// setup the provider test harness with a disconnecting server that disconnects after sending the given number of bytes
	harness := NewHarness(t, ctx, withHttpDisconnectServerAfter(int64(fileSize/101)),
		withHttpTransportOpts([]httptransport.Option{httptransport.BackOffRetryOpt(50*time.Millisecond, 100*time.Millisecond, 2, 1000)}))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// build the deal proposal
	td := harness.newDealBuilder(t, 1, withNormalFileSize(fileSize)).withAllMinerCallsNonBlocking().withDisconnectingHttpServer().build()

	// execute deal and ensure it finishes even with the disconnects
	err := td.executeAndSubscribeToNotifs()
	require.NoError(t, err)
	td.waitForAndAssert(t, ctx, dealcheckpoints.AddedPiece)
}

func TestMultipleDealsConcurrentResumptionDisconnect(t *testing.T) {
	nDeals := 5
	ctx := context.Background()
	fileSize := (100 * 1048576) + 75 // ~100Mib

	// setup the provider test harness with a disconnecting server that disconnects after sending the given number of bytes
	harness := NewHarness(t, ctx, withHttpDisconnectServerAfter(int64(fileSize/101)),
		withHttpTransportOpts([]httptransport.Option{httptransport.BackOffRetryOpt(50*time.Millisecond, 100*time.Millisecond, 2, 1000)}))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	tds := harness.executeNDealsConcurrentAndWaitFor(t, nDeals, func(i int) *testDeal {
		return harness.newDealBuilder(t, i, withNormalFileSize(fileSize)).withAllMinerCallsNonBlocking().withDisconnectingHttpServer().build()
	}, func(_ int, td *testDeal) error {
		return td.waitForCheckpoint(dealcheckpoints.AddedPiece)
	})

	for i := 0; i < nDeals; i++ {
		td := tds[i]
		td.assertPieceAdded(t, ctx)
	}
}

func TestTransferCancelledByUser(t *testing.T) {
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t, ctx)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// build the deal proposal
	td := harness.newDealBuilder(t, 1).withBlockingHttpServer().build()

	// execute deal
	err := td.executeAndSubscribeToNotifs()
	require.NoError(t, err)

	// assert deal is accepted and funds tagged
	require.NoError(t, td.waitForCheckpoint(dealcheckpoints.Accepted))
	harness.AssertStorageAndFundManagerState(t, ctx, td.params.Transfer.Size, harness.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)

	// cancel transfer for the deal
	require.NoError(t, harness.Provider.CancelDealDataTransfer(td.params.DealUUID))

	require.NoError(t, td.waitForError(DealCancelled))

	// assert cleanup of deal
	td.assertEventuallyDealCleanedup(t, ctx)
	// assert db state
	td.assertDealFailedTransferNonRecoverable(t, ctx, DealCancelled)
	// assert storage manager and funds
	harness.EventuallyAssertNoTagged(t, ctx)

	// cancelling the same deal again will error out as deal handler will be absent
	require.ErrorIs(t, harness.Provider.CancelDealDataTransfer(td.params.DealUUID), ErrDealHandlerNotFound)
}

func TestCancelTransferForTransferredDealFails(t *testing.T) {
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t, ctx)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// build the deal proposal
	td := harness.newDealBuilder(t, 1).withPublishBlocking().withPublishConfirmNonBlocking().withAddPieceNonBlocking().withNormalHttpServer().build()

	// execute deal
	err := td.executeAndSubscribeToNotifs()
	require.NoError(t, err)

	// wait for deal to finish transferring
	require.NoError(t, td.waitForCheckpoint(dealcheckpoints.Transferred))

	// cancelling the transfer fails  as deal has already finished transferring
	require.Contains(t, harness.Provider.CancelDealDataTransfer(td.params.DealUUID).Error(), "transfer already complete")

	// deal still finishes
	td.unblockPublish()
	require.NoError(t, td.waitForCheckpoint(dealcheckpoints.AddedPiece))
	td.assertPieceAdded(t, ctx)
}
