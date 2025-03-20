package storagemarket

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/boost/transport/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func generateDeal() *smtypes.ProviderDealState {
	return generateDealWithHost(fmt.Sprintf("foo.bar:%d", rand.Uint64()))
}

func generateDealWithHost(host string) *smtypes.ProviderDealState {
	transferParams := types.HttpRequest{
		URL: fmt.Sprintf("http://%s/piece/%s", host, uuid.New()),
	}
	json, err := json.Marshal(transferParams)
	if err != nil {
		panic(err)
	}
	return &smtypes.ProviderDealState{
		DealUuid:  uuid.New(),
		CreatedAt: time.Now(),
		Transfer: smtypes.Transfer{
			Type:   "http",
			Params: json,
		},
	}
}

func TestTransferLimiterBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Set up the transfer limiter
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

// Verifies that a new transfer is blocked until the number of ongoing
// transfers falls below the limit
func TestTransferLimiterQueueSize(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	// Wait till both go-routines call waitInQueue
	require.Eventually(t, func() bool { return tl.transfersCount() == 2 }, time.Second, time.Millisecond)

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

// Verifies that if a transfer stalls, another transfer is allowed to start,
// even if that means the total number of transfers breaks the soft limit
func TestTransferLimiterStalledTransfer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

	// Wait till both go-routines call waitInQueue
	require.Eventually(t, func() bool { return tl.transfersCount() == 2 }, time.Second, time.Millisecond)

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

// Verifies that transfers are prioritized in order from oldest to newest
func TestTransferLimiterPriorityOldestFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
		dl := generateDeal()
		dl.CreatedAt = time.Now().Add(-time.Hour).Round(time.Minute).Add(time.Duration(i) * time.Second)
		deals = append(deals, dl)
	}
	dealsReversed := make(chan *smtypes.ProviderDealState, len(deals))
	for i := len(deals) - 1; i >= 0; i-- {
		dealsReversed <- deals[i]
	}

	started := make(chan *smtypes.ProviderDealState)
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
	require.Eventually(t, func() bool { return tl.transfersCount() == dealCount }, time.Second, time.Millisecond)

	// Expect the deals to be started in order from oldest to newest
	for i := 0; i < len(deals); i++ {
		go tl.check(time.Now())

		dl := <-started
		require.Equal(t, deals[i].DealUuid, dl.DealUuid)

		// Make space in the queue for the next deal to be started
		tl.complete(dl.DealUuid)
	}
}

// Verifies that the prioritization favours transfers to peers that don't
// already have an ongoing transfer.
// eg there is
// - an ongoing transfer to peer A
// - a queued transfer to peer A
// - a queued transfer to peer B
// The next transfer should be the one to peer B
func TestTransferLimiterPriorityNoExistingTransferToPeerFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := TransferLimiterConfig{
		MaxConcurrent:    2,
		StallCheckPeriod: time.Millisecond,
		StallTimeout:     30 * time.Second,
	}
	tl, err := newTransferLimiter(cfg)
	require.NoError(t, err)

	// Generate three deals, where the first two have the same peer
	p := testutil.GeneratePeer()
	h := "myhost.com"
	deal1 := generateDealWithHost(h)
	deal1.ClientPeerID = p
	deal2 := generateDealWithHost(h)
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
	require.Eventually(t, func() bool { return len(deals) == 0 }, time.Second, time.Millisecond)

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

// Verifies that a new transfer will not be started if there are already
// transfers to that same peer that are stalled, and the soft limit has
// been reached
func TestTransferLimiterStalledTransferHardLimited(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := TransferLimiterConfig{
		MaxConcurrent:    2,
		StallCheckPeriod: time.Millisecond,
		StallTimeout:     time.Second,
	}
	tl, err := newTransferLimiter(cfg)
	require.NoError(t, err)

	// Generate a deal and add to the transfer queue
	h := "myhost.com"
	deal1 := generateDealWithHost(h)

	started := make(chan struct{}, 3)
	go func() {
		err := tl.waitInQueue(ctx, deal1)
		require.NoError(t, err)
		started <- struct{}{}
	}()

	// Wait till go-routine calls waitInQueue
	require.Eventually(t, func() bool { return tl.transfersCount() == 1 }, time.Second, time.Millisecond)

	// Expect the first transfer to start
	tl.check(time.Now())
	<-started

	// Mark the first transfer as stalled
	tl.check(time.Now().Add(cfg.StallTimeout))

	// Generate a second deal to the same peer
	deal2 := generateDealWithHost(h)
	deal2.ClientPeerID = deal1.ClientPeerID

	go func() {
		err := tl.waitInQueue(ctx, deal2)
		require.NoError(t, err)
		started <- struct{}{}
	}()

	// Wait till go-routine calls waitInQueue
	require.Eventually(t, func() bool { return tl.transfersCount() == 2 }, time.Second, time.Millisecond)

	// It should be allowed to start, because even though there's a stalled
	// transfer to the peer, we're still below the soft limit
	tl.check(time.Now().Add(cfg.StallTimeout))
	<-started

	// Generate a third deal to the same peer
	deal3 := generateDealWithHost(h)
	deal3.ClientPeerID = deal1.ClientPeerID

	go func() {
		tl.waitInQueue(ctx, deal3) //nolint:errcheck
		started <- struct{}{}
	}()

	// Wait till go-routine calls waitInQueue
	require.Eventually(t, func() bool { return tl.transfersCount() == 3 }, time.Second, time.Millisecond)

	tl.check(time.Now().Add(cfg.StallTimeout))

	// Expect the third transfer not to start yet, because there's a stalled
	// transfer to the same peer in the queue and we've reached the soft limit
	select {
	case <-started:
		require.Fail(t, "expected third transfer not to start yet")
	default:
	}
}
