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
}

func (d *dealHandler) subscribeUpdates() (event.Subscription, error) {
	sub, err := d.bus.Subscribe(new(types.ProviderDealState), eventbus.BufSize(256))
	if err != nil {
		return nil, fmt.Errorf("failed to create deal update subscriber to %s: %w", d.dealUuid, err)
	}
	return sub, nil
}

func (dh *dealHandler) TransferCancelledByUser() bool {
	return dh.transferCancelledByUser.Load()
}

func (dh *dealHandler) cancel() error {
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
