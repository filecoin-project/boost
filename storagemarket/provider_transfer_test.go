package storagemarket

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/stretchr/testify/require"
)

const testFileSize = (10 * 1048576) + 75 // ~10Mib

func TestSingleDealResumptionDisconnect(t *testing.T) {
	logging.SetLogLevel("http-transport", "WARN") //nolint:errcheck

	ctx := context.Background()
	fileSize := testFileSize

	// setup the provider test harness with a disconnecting server that disconnects after sending the given number of bytes
	harness := NewHarness(t, withHttpDisconnectServerAfter(int64(fileSize/101)), withHttpTransportOpts([]httptransport.Option{httptransport.BackOffRetryOpt(50*time.Millisecond, 100*time.Millisecond, 2, 1000)}))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// build the deal proposal
	td := harness.newDealBuilder(t, 1, withNormalFileSize(fileSize)).withAllMinerCallsNonBlocking().withDisconnectingHttpServer().build()

	// execute deal and ensure it finishes even with the disconnects
	err := td.executeAndSubscribe()
	require.NoError(t, err)
	td.waitForAndAssert(t, ctx, dealcheckpoints.AddedPiece)

	// assert logs
	lgs, err := harness.Provider.logsDB.Logs(ctx, td.params.DealUUID)
	require.NoError(t, err)
	require.NotEmpty(t, lgs)
}

func TestRetryShutdownRecoverable(t *testing.T) {
	logging.SetLogLevel("http-transport", "WARN") //nolint:errcheck

	ctx := context.Background()
	fileSize := testFileSize

	// setup the provider test harness with a disconnecting server that disconnects after sending the given number of bytes
	harness := NewHarness(t, withHttpDisconnectServerAfter(int64(fileSize/101)))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// build the deal proposal
	td := harness.newDealBuilder(t, 1, withNormalFileSize(fileSize)).withDisconnectingHttpServer().build()

	// execute deal and ensure it finishes even with the disconnects
	err := td.executeAndSubscribe()
	require.NoError(t, err)

	for evt := range td.sub.Out() {
		// wait to receive some bytes and then shutdown
		if evt.(types.ProviderDealState).NBytesReceived > 0 {
			harness.Provider.Stop()
			break
		}
	}

	// deal should have failed with a recoverable error i.e. it should not be marked as failed or complete
	pds, err := harness.DealsDB.ByID(ctx, td.params.DealUUID)
	require.NoError(t, err)
	require.EqualValues(t, dealcheckpoints.Accepted, pds.Checkpoint)
}

func TestTransferCancelledByUser(t *testing.T) {
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// build the deal proposal
	td := harness.newDealBuilder(t, 1).withBlockingHttpServer().build()

	// execute deal
	err := td.executeAndSubscribe()
	require.NoError(t, err)

	// assert deal is accepted and funds tagged
	require.NoError(t, td.waitForCheckpoint(dealcheckpoints.Accepted))
	harness.AssertStorageAndFundManagerState(t, ctx, td.params.Transfer.Size, harness.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)

	// cancel transfer for the deal
	require.NoError(t, harness.Provider.CancelDealDataTransfer(td.params.DealUUID))

	require.NoError(t, td.waitForError(DealCancelled, types.DealRetryFatal))

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
	harness := NewHarness(t)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// build the deal proposal
	td := harness.newDealBuilder(t, 1).withCommpNonBlocking().withPublishBlocking().withPublishConfirmNonBlocking().withAddPieceNonBlocking().withAnnounceNonBlocking().withNormalHttpServer().build()

	// execute deal
	err := td.executeAndSubscribe()
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
