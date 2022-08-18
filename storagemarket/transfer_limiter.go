package storagemarket

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type transfer struct {
	started   chan struct{}
	deal      *smtypes.ProviderDealState
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
	// If no data is sent within the stall timeout the connection is assumed to have stalled
	StallTimeout time.Duration
}

//
// transferLimiter maintains a queue of transfers with a soft upper limit on
// the number of concurrent transfers.
//
// To prevent slow or stalled transfers from blocking up the queue there are
// a couple of mitigations:
//
// The queue is ordered such that we
// - start transferring data for the oldest deal first
// - prefer to start transfers with peers that don't have any ongoing transfer
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
// then two more transfers are permitted to start.
//
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
	tl.lk.Lock()
	defer tl.lk.Unlock()

	// Count how many transfers are active (not stalled)
	var activeCount uint64
	transferringPeers := make(map[peer.ID]struct{}, len(tl.xfers))
	unstartedXfers := make([]*transfer, 0, len(tl.xfers))
	for _, xfer := range tl.xfers {
		if !xfer.isStarted() {
			// Build a list of unstarted transfers (needed later)
			unstartedXfers = append(unstartedXfers, xfer)

			// Skip transfers that haven't started
			continue
		}

		// Build the set of peers that have an ongoing transfer (needed later)
		transferringPeers[xfer.deal.ClientPeerID] = struct{}{}

		// Check each transfer to see if it has stalled
		if now.Sub(xfer.updatedAt) < tl.cfg.StallTimeout {
			activeCount++
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
		// Iterate over transfers from oldest to newest
		for _, xfer := range unstartedXfers {
			// Skip transfers that have already been started
			if xfer.isStarted() {
				continue
			}

			// Default to choosing the oldest unstarted transfer
			if next == nil {
				next = xfer
			}

			// If there are no transfers with the peer that sent the storage deal,
			// start a transfer.
			// This helps ensure that a slow peer doesn't block up the transfer
			// queue.
			if _, ok := transferringPeers[xfer.deal.ClientPeerID]; !ok {
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
		transferringPeers[next.deal.ClientPeerID] = struct{}{}

		// Signal that the transfer has started
		next.updatedAt = time.Now()
		close(next.started)
	}
}

// Wait for the next open spot in the transfer queue
func (tl *transferLimiter) waitInQueue(ctx context.Context, deal *smtypes.ProviderDealState) error {
	xfer := &transfer{
		deal:    deal,
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
	// TODO: avoid locking contention with the check function
	tl.lk.Lock()
	defer tl.lk.Unlock()

	xfer, ok := tl.xfers[dealUuid]
	if ok {
		xfer.updatedAt = time.Now()
		xfer.bytes = bytes
	}
}
