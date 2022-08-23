package modules

import (
	"context"
	"fmt"

	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	"github.com/filecoin-project/boost/retrievalmarket/types"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/fx"
)

func NewTransportsListener(cfg *config.Boost) func(h host.Host) (*lp2pimpl.TransportsListener, error) {
	return func(h host.Host) (*lp2pimpl.TransportsListener, error) {
		protos := []types.Protocol{}

		// Get the libp2p addresses from the Host
		if len(h.Addrs()) > 0 {
			protos = append(protos, types.Protocol{
				Name:      "libp2p",
				Addresses: h.Addrs(),
			})
		}

		// If there's an http retrieval address specified, add HTTP to the list
		// of supported protocols
		if cfg.Dealmaking.HTTPRetrievalMultiAddr != "" {
			maddr, err := multiaddr.NewMultiaddr(cfg.Dealmaking.HTTPRetrievalMultiAddr)
			if err != nil {
				msg := "HTTPRetrievalURL must be in multi-address format. "
				msg += "Could not parse '%s' as multiaddr: %w"
				return nil, fmt.Errorf(msg, cfg.Dealmaking.HTTPRetrievalMultiAddr, err)
			}

			protos = append(protos, types.Protocol{
				Name:      "http",
				Addresses: []multiaddr.Multiaddr{maddr},
			})
		}

		return lp2pimpl.NewTransportsListener(h, protos), nil
	}
}

func HandleRetrievalTransports(lc fx.Lifecycle, l *lp2pimpl.TransportsListener) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Debug("starting retrieval transports listener")
			l.Start()
			return nil
		},
		OnStop: func(context.Context) error {
			log.Debug("stopping retrieval transports listener")
			l.Stop()
			return nil
		},
	})
}
