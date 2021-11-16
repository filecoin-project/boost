package transport

import (
	"context"

	"github.com/libp2p/go-libp2p-core/event"

	"github.com/filecoin-project/boost/transport/types"
)

type Transport interface {
	Execute(ctx context.Context, transportInfo []byte, dealInfo *types.TransportDealInfo) (th Handler, isComplete bool, err error)
}

type Handler interface {
	Sub() event.Subscription
	Close() error
}
