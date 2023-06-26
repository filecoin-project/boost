package storagemarket

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/google/uuid"
)

type transfer struct {
	started   chan struct{}
	deal      *smtypes.ProviderDealState
	host      string
	updatedAt time.Time
	bytes     uint64
}

func (t *transfer) isStarted() bool {
	select {
	case <-t.started:
		return true
	default:
		return false
	}
}

type TransferLimiterConfig struct {
	// The maximum number of concurrent transfers (soft limit - see comment below)
	MaxConcurrent uint64
	// The period between checking if a connection has stalled
	StallCheckPeriod time.Duration
	// The time that can elapse before a download is considered stalled
	StallTimeout time.Duration
}

// transferLimiter maintains a queue of transfers with a soft upper limit on
// the number of concurrent transfers.
//
// To prevent slow or stalled transfers from blocking up the queue there are
// a couple of mitigations:
//
// The queue is ordered such that we
//   - start transferring data for the oldest deal first
//   - prefer to start transfers with peers that don't have any ongoing transfer
//   - once the soft limit is reached, don't allow any new transfers with peers
//     that have existing stalled transfers
//
// Note that peers are distinguished by their host (eg foo.bar:8080) not by
// libp2p peer ID.
//
// For example, if there is
// - one active transfer with peer A
// - one pending transfer (peer A)
// - one pending transfer (peer B)
// the algorithm will prefer to start a transfer with peer B than peer A.
//
// This helps to ensure that slow peers don't block the transfer queue.
//
// The limit on the number of concurrent transfers is soft:
// eg if there is a limit of 5 concurrent transfers and there are
// - three active transfers
// - two stalled transfers
// then two more transfers are permitted to start (as long as they're not with
// one of the stalled peers)
type transferLimiter struct {
	cfg TransferLimiterConfig

	lk    sync.RWMutex
	xfers map[uuid.UUID]*transfer
}

func newTransferLimiter(cfg TransferLimiterConfig) (*transferLimiter, error) {
	if cfg.MaxConcurrent == 0 {
		return nil, fmt.Errorf("maximum active concurrent transfers must be > 0")
	}
	if cfg.StallCheckPeriod == 0 {
		return nil, fmt.Errorf("transfer stall check period must be > 0")
	}
	if cfg.StallTimeout == 0 {
		return nil, fmt.Errorf("transfer stall timeout must be > 0")
	}

	return &transferLimiter{
		cfg:   cfg,
		xfers: make(map[uuid.UUID]*transfer),
	}, nil
}

func (tl *transferLimiter) run(ctx context.Context) {
	// Periodically check for stalled transfers
	ticker := time.NewTicker(tl.cfg.StallCheckPeriod)
	defer ticker.Stop()

	// Note: The first tick will occur after one stall check period (not
	// immediately).
	for {
		select {
		case t := <-ticker.C:
			tl.check(t)

		case <-ctx.Done():
			return
		}
	}
}

func (tl *transferLimiter) check(now time.Time) {
	// Take a copy of the transfers map.
	// We do this to avoid lock contention with the SetBytes message which
	// is called with high frequency when there are a lot of concurrent
	// transfers (every time data is received).
	tl.lk.Lock()
	xfers := make(map[uuid.UUID]*transfer, len(tl.xfers))
	for id, xfer := range tl.xfers {
		cp := *xfer
		xfers[id] = &cp
	}
	tl.lk.Unlock()

	// Count how many transfers are active (not stalled)
	var activeCount uint64
	transferringPeers := make(map[string]struct{}, len(xfers))
	stalledPeers := make(map[string]struct{}, len(xfers))
	unstartedXfers := make([]*transfer, 0, len(xfers))
	for _, xfer := range xfers {
		if !xfer.isStarted() {
			// Build a list of unstarted transfers (needed later)
			unstartedXfers = append(unstartedXfers, xfer)

			// Skip transfers that haven't started
			continue
		}

		// Build the set of peers that have an ongoing transfer (needed later)
		transferringPeers[xfer.host] = struct{}{}

		// Check each transfer to see if it has stalled
		if now.Sub(xfer.updatedAt) < tl.cfg.StallTimeout {
			activeCount++
		} else {
			stalledPeers[xfer.host] = struct{}{}
		}
	}

	// Check if there are already enough active transfers
	if activeCount >= tl.cfg.MaxConcurrent {
		return
	}

	// Sort unstarted transfers by creation date (oldest first)
	sort.Slice(unstartedXfers, func(i, j int) bool {
		return unstartedXfers[i].deal.CreatedAt.Before(unstartedXfers[j].deal.CreatedAt)
	})

	// Gets the next transfer that should be started
	nextTransfer := func() *transfer {
		var next *transfer

		// Iterate over unstarted transfers from oldest to newest
		startedCount := tl.startedCount(xfers)
		for _, xfer := range unstartedXfers {
			// Skip transfers that have already been started.
			// Note: A previous call to nextTransfer may have started the
			// transfer.
			if xfer.isStarted() {
				continue
			}

			// If there is already a transfer to the same peer and it's stalled,
			// allow a new transfer with that peer, but only up to the soft
			// limit
			_, isStalledPeer := stalledPeers[xfer.host]
			if isStalledPeer && startedCount >= tl.cfg.MaxConcurrent {
				continue
			}

			// Default to choosing the oldest unstarted transfer
			if next == nil {
				next = xfer
			}

			// If there are no transfers with the peer that sent the storage deal,
			// start a transfer.
			// This helps ensure that a slow peer doesn't block up the transfer
			// queue, because we'll favour opening a transfer to a new peer
			// over a peer that already has a transfer (which may be slow).
			if _, ok := transferringPeers[xfer.host]; !ok {
				return xfer
			}
		}

		return next
	}

	// Start new transfers until we reach the limit
	for i := activeCount; i < tl.cfg.MaxConcurrent; i++ {
		next := nextTransfer()
		if next == nil {
			return
		}

		// Update the list of peers with active transfers
		transferringPeers[next.host] = struct{}{}

		// Signal that the transfer has started
		next.updatedAt = time.Now()
		close(next.started)
	}
}

// Count how many transfers have been started but not completed
func (tl *transferLimiter) startedCount(xfers map[uuid.UUID]*transfer) uint64 {
	var count uint64
	for _, xfer := range xfers {
		if xfer.isStarted() {
			count++
		}
	}
	return count
}

// Count how many transfers there are in total
func (tl *transferLimiter) transfersCount() int {
	tl.lk.RLock()
	defer tl.lk.RUnlock()

	return len(tl.xfers)
}

// Wait for the next open spot in the transfer queue
func (tl *transferLimiter) waitInQueue(ctx context.Context, deal *smtypes.ProviderDealState) error {
	host, err := deal.Transfer.Host()
	if err != nil {
		return fmt.Errorf("getting host from Transfer params for deal %s: %w", deal.DealUuid, err)
	}
	xfer := &transfer{
		deal:    deal,
		host:    host,
		started: make(chan struct{}),
	}

	// Add the transfer to the queue
	tl.lk.Lock()
	tl.xfers[deal.DealUuid] = xfer
	tl.lk.Unlock()

	// Wait for the signal that the transfer can start
	select {
	case <-xfer.started:
		return nil
	case <-ctx.Done():
		tl.complete(deal.DealUuid)
		return ctx.Err()
	}
}

// Called when a transfer has completed (or errored out)
func (tl *transferLimiter) complete(dealUuid uuid.UUID) {
	tl.lk.Lock()
	defer tl.lk.Unlock()

	delete(tl.xfers, dealUuid)
}

// Called each time the transfer progresses
func (tl *transferLimiter) setBytes(dealUuid uuid.UUID, bytes uint64) {
	tl.lk.Lock()
	defer tl.lk.Unlock()

	xfer, ok := tl.xfers[dealUuid]
	if ok {
		xfer.updatedAt = time.Now()
		xfer.bytes = bytes
	}
}

func (tl *transferLimiter) isStalled(dealUuid uuid.UUID) bool {
	now := time.Now()

	tl.lk.RLock()
	defer tl.lk.RUnlock()

	xfer, ok := tl.xfers[dealUuid]
	if !ok {
		return false
	}

	if !xfer.isStarted() {
		return false
	}

	return now.Sub(xfer.updatedAt) >= tl.cfg.StallTimeout
}

type HostTransferStats struct {
	Host      string
	Total     int
	Started   int
	Stalled   int
	DealUuids []uuid.UUID
}

func (tl *transferLimiter) stats() []*HostTransferStats {
	now := time.Now()

	tl.lk.RLock()
	defer tl.lk.RUnlock()

	// Count the number of transfers, started and stalled per host
	stats := make(map[string]*HostTransferStats)
	for _, xfer := range tl.xfers {
		hostStats, ok := stats[xfer.host]
		if !ok {
			hostStats = &HostTransferStats{Host: xfer.host}
			stats[xfer.host] = hostStats
		}

		hostStats.Total++
		hostStats.DealUuids = append(hostStats.DealUuids, xfer.deal.DealUuid)
		if !xfer.isStarted() {
			continue
		}
		hostStats.Started++
		if now.Sub(xfer.updatedAt) >= tl.cfg.StallTimeout {
			hostStats.Stalled++
		}
	}

	// Sort the stats by host
	statsArr := make([]*HostTransferStats, 0, len(stats))
	for _, s := range stats {
		statsArr = append(statsArr, s)
	}
	sort.Slice(statsArr, func(i, j int) bool {
		return statsArr[i].Host < statsArr[j].Host
	})
	return statsArr
}
