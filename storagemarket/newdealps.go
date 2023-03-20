package storagemarket

import (
	"fmt"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
)

// newDealPS provides a nicer interface to keep track of "new deal" events
type newDealPS struct {
	newDealsBus event.Bus
	NewDeals    event.Emitter
}

func newDealPubsub() (*newDealPS, error) {
	bus := eventbus.NewBus()
	emitter, err := bus.Emitter(&types.ProviderDealState{}, eventbus.Stateful)
	if err != nil {
		return nil, fmt.Errorf("failed to create event emitter: %w", err)
	}

	return &newDealPS{
		newDealsBus: bus,
		NewDeals:    emitter,
	}, nil
}

func (m *newDealPS) subscribe() (event.Subscription, error) {
	sub, err := m.newDealsBus.Subscribe(new(types.ProviderDealState), eventbus.BufSize(256))
	if err != nil {
		return nil, fmt.Errorf("failed to create subscriber to new deals: %w", err)
	}
	return sub, nil
}
