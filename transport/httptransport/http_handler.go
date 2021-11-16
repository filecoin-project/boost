package httptransport

import (
	"context"
	"sync"

	"github.com/filecoin-project/boost/transport/types"

	"github.com/libp2p/go-libp2p-core/event"
)

type TransportHandler struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once

	sub event.Subscription

	dealInfo *types.TransportDealInfo
	transfer *transfer
}

// Close shuts down the transfer for the given deal. It is the caller's responsibility to call Close after it no longer needs the transfer.
func (t *TransportHandler) Close() error {
	t.closeOnce.Do(func() {
		// cancel the context associated with the transfer
		if t.cancel != nil {
			t.cancel()
		}
		// wait for all go-routines associated with the transfer to return
		if t.transfer != nil {
			t.transfer.wg.Wait()
		}
		// close the subscription
		if t.sub != nil {
			_ = t.sub.Close()
		}
	})

	return nil
}

func (t *TransportHandler) Sub() event.Subscription {
	return t.sub
}
