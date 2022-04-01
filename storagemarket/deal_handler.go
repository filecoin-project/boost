package storagemarket

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/atomic"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"

	"github.com/google/uuid"
)

// dealHandler keeps track of the deal while it's executing
type dealHandler struct {
	providerCtx context.Context
	dealUuid    uuid.UUID
	bus         event.Bus

	// Transfer cancellation state
	transferCtx             context.Context
	transferCancel          context.CancelFunc
	tdOnce                  sync.Once // ensures the transferDone channel is closed only once
	transferDone            chan error
	transferCancelledByUser atomic.Bool

	transferMu       sync.Mutex
	transferFinished bool
	transferErr      error

	updateSubsLk sync.RWMutex
	updateSubs   map[*updatesSubscription]struct{}
}

func newDealHandler(ctx context.Context, dealUuid uuid.UUID) *dealHandler {
	// Create a deal handler
	bus := eventbus.NewBus()

	transferCtx, cancel := context.WithCancel(ctx)
	return &dealHandler{
		providerCtx: ctx,
		dealUuid:    dealUuid,
		bus:         bus,

		transferCtx:    transferCtx,
		transferCancel: cancel,
		transferDone:   make(chan error, 1),

		updateSubs: make(map[*updatesSubscription]struct{}),
	}
}

// updatesSubscription wraps event.Subscription so that we can add an onClose
// callback
type updatesSubscription struct {
	event.Subscription
	onClose func(*updatesSubscription)
}

func (s *updatesSubscription) Close() error {
	s.onClose(s)
	return s.Subscription.Close()
}

// subscribeUpdates subscribes to deal status updates
func (d *dealHandler) subscribeUpdates() (event.Subscription, error) {
	sub, err := d.bus.Subscribe(new(types.ProviderDealState), eventbus.BufSize(256))
	if err != nil {
		return nil, fmt.Errorf("failed to create deal update subscriber to %s: %w", d.dealUuid, err)
	}

	// create an updatesSubscription that will delete itself from the map of
	// all update subscriptions when it is closed
	updatesSub := &updatesSubscription{
		Subscription: sub,
		onClose: func(s *updatesSubscription) {
			d.updateSubsLk.Lock()
			defer d.updateSubsLk.Unlock()
			delete(d.updateSubs, s)
		},
	}

	// Add the updatesSubscription to the map of all update subscriptions
	d.updateSubsLk.Lock()
	defer d.updateSubsLk.Unlock()
	d.updateSubs[updatesSub] = struct{}{}

	return updatesSub, nil
}

// hasUpdateSubscribers indicates if anyone is subscribed to updates.
// This is useful if we want to check if anyone is listening before doing an
// expensive operation to publish an event.
func (d *dealHandler) hasUpdateSubscribers() bool {
	d.updateSubsLk.RLock()
	defer d.updateSubsLk.RUnlock()
	return len(d.updateSubs) > 0
}

// TransferCancelledByUser returns true if the user explicitly cancelled the transfer by calling `dealhandler.cancelTransfer()`
func (dh *dealHandler) TransferCancelledByUser() bool {
	return dh.transferCancelledByUser.Load()
}

// cancelTransfer idempotently cancels the context associated with the transfer so the transfer errors out and then waits
// for the transfer to fail. If the transfer is already cancelled, this is a no-op.
func (dh *dealHandler) cancelTransfer() error {
	dh.transferCancelledByUser.Store(true)
	dh.transferMu.Lock()
	defer dh.transferMu.Unlock()

	if dh.transferFinished {
		return dh.transferErr
	}

	dh.transferCancel()

	select {
	case err := <-dh.transferDone:
		dh.transferFinished = true
		dh.transferErr = err
		return err
	case <-dh.providerCtx.Done():
		return nil
	}
}

// transferCancelled idempotently marks the transfer as cancelled with the given error.
func (dh *dealHandler) transferCancelled(err error) {
	dh.tdOnce.Do(func() {
		dh.transferDone <- err
		close(dh.transferDone)
	})
}

func (dh *dealHandler) close() {
	dh.transferCancel()
	dh.transferCancelled(errors.New("deal handler closed"))
}
