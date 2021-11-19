package transport

import (
	"context"

	"github.com/filecoin-project/boost/transport/types"
)

type Transport interface {
	Execute(ctx context.Context, transportInfo []byte, dealInfo *types.TransportDealInfo) (th Handler, err error)
}

type Handler interface {
	Sub() chan types.TransportEvent
	Close()
}
