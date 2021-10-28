package datatransfer

import (
	"context"
	"net/url"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/libp2p/go-libp2p-core/event"
)

type Transport interface {
	Execute(ctx context.Context, URL *url.URL, OutputFilePath string, ExpectedDealSize abi.PaddedPieceSize) (event.Subscription, error)
}
