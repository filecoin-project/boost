package transport

import (
	"context"

	"github.com/filecoin-project/boost/transport/types"
)

type Transport interface {
	Execute(ctx context.Context, transportInfo []byte, dealInfo *types.TransportDealInfo) (th *TransportHandler, isComplete bool, err error)
}
