package storagemarket

import (
	"context"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/testutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func generateDeal() *smtypes.ProviderDealState {
	return &smtypes.ProviderDealState{
		DealUuid:     uuid.New(),
		CreatedAt:    time.Now(),
		ClientPeerID: testutil.GeneratePeer(),
	}
}

func TestTransferLimiterBasic(t *testing.T) {
	// Set up the transfer limiter
	ctx := context.Background()
	tl, err := newTransferLimiter(TransferLimiterConfig{
		MaxConcurrent:    1,
		StallCheckPeriod: time.Millisecond,
		StallTimeout:     30 * time.Second,
	})
	require.NoError(t, err)

	go tl.run(ctx)

	// Add a deal to the transfer queue
	deal1 := generateDeal()
	err = tl.waitInQueue(ctx, deal1)
	require.NoError(t, err)

	// Remove the deal from the transfer queue
	tl.complete(deal1.DealUuid)
}

func TestTransferLimiterQueueSize(t *testing.T) {
	ctx := context.Background()
	tl, err := newTransferLimiter(TransferLimiterConfig{
		MaxConcurrent:    1,
		StallCheckPeriod: time.Millisecond,
		StallTimeout:     30 * time.Second,
	})
	require.NoError(t, err)

	// Generate two deals and add them to the transfer queue
	deal1 := generateDeal()
	deal2 := generateDeal()

	started := make(chan struct{}, 2)
	go func() {
		err := tl.waitInQueue(ctx, deal1)
		require.NoError(t, err)
		started <- struct{}{}
	}()

	go func() {
		err := tl.waitInQueue(ctx, deal2)
		require.NoError(t, err)
		started <- struct{}{}
	}()

	// Wait a little while to make sure the go routines have had a
	// chance to start
	time.Sleep(100 * time.Millisecond)

	// Expect the first transfer to start
	tl.check(time.Now())
	<-started

	// Expect the second transfer not to start yet, because the transfer queue
	// has a limit of 1
	select {
	case <-started:
		require.Fail(t, "expected second transfer not to start yet")
	default:
	}

	// Complete the first transfer
	tl.complete(deal1.DealUuid)

	// Expect the second transfer to start now that the first has completed
	tl.check(time.Now())
	<-started
}

func TestTransferLimiterStalledTransfer(t *testing.T) {
	ctx := context.Background()
	cfg := TransferLimiterConfig{
		MaxConcurrent:    1,
		StallCheckPeriod: time.Millisecond,
		StallTimeout:     time.Second,
	}
	tl, err := newTransferLimiter(cfg)
	require.NoError(t, err)

	// Generate two deals and add them to the transfer queue
	deal1 := generateDeal()
	deal2 := generateDeal()

	started := make(chan struct{}, 2)
	go func() {
		err := tl.waitInQueue(ctx, deal1)
		require.NoError(t, err)
		started <- struct{}{}
	}()

	go func() {
		err := tl.waitInQueue(ctx, deal2)
		require.NoError(t, err)
		started <- struct{}{}
	}()

	// Wait a little while to make sure the go routines have had a
	// chance to start
	time.Sleep(100 * time.Millisecond)

	// Expect the first transfer to start
	tl.check(time.Now())
	<-started

	// Expect the second transfer not to start yet, because the transfer queue
	// has a limit of 1
	select {
	case <-started:
		require.Fail(t, "expected second transfer not to start yet")
	default:
	}

	// Tell the transfer limiter that some progress has been made in the
	// transfer for deal 1
	tl.setBytes(deal1.DealUuid, 1)

	// Simulate a check after half of the stall timeout has elapsed
	tl.check(time.Now().Add(cfg.StallTimeout / 2))

	// Expect the second transfer not to start yet, because the stall timeout
	// has not expired
	select {
	case <-started:
		require.Fail(t, "expected second transfer not to start yet")
	default:
	}

	// Once the stall duration has elapsed with no updates to the first
	// transfer, expect the second transfer to be allowed to start
	tl.check(time.Now().Add(cfg.StallTimeout))
	<-started
}

func TestTransferLimiterPriorityOldestFirst(t *testing.T) {
	ctx := context.Background()
	cfg := TransferLimiterConfig{
		MaxConcurrent:    1,
		StallCheckPeriod: time.Millisecond,
		StallTimeout:     30 * time.Second,
	}
	tl, err := newTransferLimiter(cfg)
	require.NoError(t, err)

	// Generate deals and add them to the transfer queue in the reverse order
	// than they were generated
	dealCount := 100
	deals := make([]*smtypes.ProviderDealState, 0, dealCount)
	for i := 0; i < dealCount; i++ {
		deals = append(deals, generateDeal())
	}
	dealsReversed := make(chan *smtypes.ProviderDealState, dealCount)
	for i := len(deals) - 1; i >= 0; i-- {
		dealsReversed <- deals[i]
	}

	started := make(chan *smtypes.ProviderDealState, 3)
	for i := 0; i < len(deals); i++ {
		// The order in which the go routines run is non-deterministic,
		// but by adding deals in newest-to-oldest order, we can assume that
		// it's extremely unlikely the deals will be started in
		// oldest-to-newest order
		go func() {
			dl := <-dealsReversed
			err := tl.waitInQueue(ctx, dl)
			require.NoError(t, err)
			started <- dl
		}()
	}

	// Wait for all the deals to be added to the transfer queue
	require.Eventually(t, func() bool {
		return len(dealsReversed) == 0
	}, time.Second, time.Millisecond)

	// Start processing transfers
	go tl.run(ctx)

	// Expect the deals to be started in order from oldest to newest
	for i := 0; i < len(deals); i++ {
		dl := <-started
		require.Equal(t, deals[i].DealUuid, dl.DealUuid)

		// Make space in the queue for the next deal to be started
		tl.complete(dl.DealUuid)
	}
}

func TestTransferLimiterPriorityNoPeerFirst(t *testing.T) {
	ctx := context.Background()
	cfg := TransferLimiterConfig{
		MaxConcurrent:    2,
		StallCheckPeriod: time.Millisecond,
		StallTimeout:     30 * time.Second,
	}
	tl, err := newTransferLimiter(cfg)
	require.NoError(t, err)

	// Generate three deals, where the first two have the same peer
	p := testutil.GeneratePeer()
	deal1 := generateDeal()
	deal1.ClientPeerID = p
	deal2 := generateDeal()
	deal2.ClientPeerID = p
	deal3 := generateDeal()

	deals := make(chan *smtypes.ProviderDealState, 3)
	deals <- deal1
	deals <- deal2
	deals <- deal3

	started := make(chan *smtypes.ProviderDealState, len(deals))
	for i := 0; i < 3; i++ {
		go func() {
			dl := <-deals
			err := tl.waitInQueue(ctx, dl)
			require.NoError(t, err)
			started <- dl
		}()
	}

	// Wait for all the deals to be added to the transfer queue
	require.Eventually(t, func() bool {
		return len(deals) == 0
	}, time.Second, time.Millisecond)

	// The queue size limit is 2.
	// The oldest to newest order is deal1, deal2, deal3
	// However we expect deal1 and deal3 to be started first.
	// This is because deal1 and deal2 have the same peer ID, and
	// the algorithm should favour deal3 because it has a different
	// peer ID.
	tl.check(time.Now())
	dl := <-started
	require.True(t, dl.DealUuid == deal1.DealUuid || dl.DealUuid == deal3.DealUuid)
	dl = <-started
	require.True(t, dl.DealUuid == deal1.DealUuid || dl.DealUuid == deal3.DealUuid)

	// Complete the first deal transfer to make space for another transfer
	tl.complete(deal1.DealUuid)

	// Expect deal2 to start last
	tl.check(time.Now())
	dl = <-started
	require.Equal(t, deal2.DealUuid, dl.DealUuid)
}
