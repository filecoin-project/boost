package transport

import (
	"context"
	"sync"

	"github.com/filecoin-project/boost/transport/types"

	"github.com/libp2p/go-libp2p-core/event"
)

type TransportHandler struct {
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once

	dealInfo *types.TransportDealInfo
	Sub      event.Subscription
}

func NewHandler(ctx context.Context, cancel context.CancelFunc, dInfo *types.TransportDealInfo, sub event.Subscription, wg sync.WaitGroup) *TransportHandler {
	return &TransportHandler{
		ctx:      ctx,
		cancel:   cancel,
		dealInfo: dInfo,
		Sub:      sub,
		wg:       wg,
	}
}

// Close shuts down the transfer for the given deal. It is the caller's responsibility to call Close after it no longer needs the transfer.
func (t *TransportHandler) Close() error {
	t.closeOnce.Do(func() {
		// cancel the context associated with the transfer
		if t.cancel != nil {
			t.cancel()
		}

		// wait for all go-routines associated with the transfer to return
		t.wg.Wait()

		// close the subscription
		if t.Sub != nil {
			_ = t.Sub.Close()
		}
	})

	return nil
}
