package storagemarket

import (
	"sync"

	"github.com/google/uuid"

	"github.com/libp2p/go-libp2p-core/event"
)

type DealHandler struct {
	cancelSync sync.Once

	DealUuid uuid.UUID
	// caller should close this when done with the deal
	Subscription event.Subscription
}

func newDealHandler(dealUuid uuid.UUID, sub event.Subscription) *DealHandler {
	dh := &DealHandler{
		DealUuid:     dealUuid,
		Subscription: sub,
	}
	return dh
}

// Shutsdown/Cancels the deal.
func (dh *DealHandler) Close() error {
	dh.cancelSync.Do(func() {
		_ = dh.Subscription.Close()

		// TODO Pass down the cancel to the deal go-routine in Boost
		// wait for the deal to get cancelled
	})
	return nil
}
