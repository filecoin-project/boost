package storagemarket

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
)

type transferCancelResp struct {
	err error // err=nil means that the transfer cancellation was successful.
}

// dealHandler keeps track of the deal while it's executing
type dealHandler struct {
	ctx context.Context

	// Transfer synchronization state
	transferWriteOnce    sync.Once
	transferCtx          context.Context
	transferCancel       context.CancelFunc
	transferredCancelled chan transferCancelResp // channel the deal execution go-routine will write to when transfer is stopped.

	dealUuid uuid.UUID
	bus      event.Bus
}

func (d *dealHandler) cancelTransfer(ctx context.Context) error {
	d.transferCancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case tc, ok := <-d.transferredCancelled:
		if !ok {
			return errors.New("cannot cancel transfer anymore")
		}
		return tc.err
	}
}

func (d *dealHandler) subscribeUpdates() (event.Subscription, error) {
	sub, err := d.bus.Subscribe(new(types.ProviderDealState), eventbus.BufSize(256))
	if err != nil {
		return nil, fmt.Errorf("failed to create deal update subscriber to %s: %w", d.dealUuid, err)
	}
	return sub, nil
}

func (d *dealHandler) close() {
	d.transferCancel()
	d.transferWriteOnce.Do(func() {
		close(d.transferredCancelled)
	})
}
