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
	defer harness.Stop()
	// start the provider test harness
	harness.Start(t, ctx)

	// build the deal proposal
	td := harness.newDealBuilder(t, 1, withNormalFileSize(fileSize)).withDisconnectingHttpServer().build()

	// execute deal and ensure it finishes even with the disconnects
	err := td.execute()
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
	defer harness.Stop()
	// start the provider test harness
	harness.Start(t, ctx)

	tds := harness.executeNDealsConcurrentAndWaitfor(t, nDeals, dealcheckpoints.AddedPiece, func(i int) *testDeal {
		return harness.newDealBuilder(t, i, withNormalFileSize(fileSize)).withDisconnectingHttpServer().build()
	})

	for i := 0; i < nDeals; i++ {
		td := tds[i]
		td.assertPieceAdded(t, ctx)
	}
}
