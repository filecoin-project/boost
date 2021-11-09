package storagemarket

import (
	"context"
	"fmt"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"

	"github.com/google/uuid"
)

// dealHandler keeps track of the deal while it's executing
type dealHandler struct {
	dealUuid uuid.UUID
	ctx      context.Context
	stop     context.CancelFunc
	stopped  chan struct{}
	bus      event.Bus
	pub      event.Emitter
}

func (d *dealHandler) cancel(ctx context.Context) {
	d.stop()
	select {
	case <-ctx.Done():
		return
	case <-d.stopped:
		return
	}
}

func (d *dealHandler) subscribeUpdates() (event.Subscription, error) {
	sub, err := d.bus.Subscribe(new(types.ProviderDealInfo), eventbus.BufSize(256))
	if err != nil {
		return nil, fmt.Errorf("failed to create deal update subscriber to %s: %w", d.dealUuid, err)
	}
	return sub, nil
}
