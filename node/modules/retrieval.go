package modules

import (
	"context"
	"fmt"

	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/protocolproxy"
	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	"github.com/filecoin-project/boost/retrievalmarket/types"
	"github.com/ipfs/go-bitswap/network"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/fx"
)

var bitswapProtocols = []protocol.ID{
	network.ProtocolBitswap,
	network.ProtocolBitswapNoVers,
	network.ProtocolBitswapOneOne,
	network.ProtocolBitswapOneZero,
}

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
		if cfg.Dealmaking.HTTPRetrievalMultiaddr != "" {
			maddr, err := multiaddr.NewMultiaddr(cfg.Dealmaking.HTTPRetrievalMultiaddr)
			if err != nil {
				msg := "HTTPRetrievalURL must be in multi-address format. "
				msg += "Could not parse '%s' as multiaddr: %w"
				return nil, fmt.Errorf(msg, cfg.Dealmaking.HTTPRetrievalMultiaddr, err)
			}

			protos = append(protos, types.Protocol{
				Name:      "http",
				Addresses: []multiaddr.Multiaddr{maddr},
			})
		}
		if cfg.Dealmaking.BitswapPeerID != "" {
			protos = append(protos, types.Protocol{
				Name:      "bitswap",
				Addresses: h.Addrs(),
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

func NewProtocolProxy(cfg *config.Boost) func(h host.Host) (*protocolproxy.ProtocolProxy, error) {
	return func(h host.Host) (*protocolproxy.ProtocolProxy, error) {
		peerConfig := map[peer.ID][]protocol.ID{}
		if cfg.Dealmaking.BitswapPeerID != "" {
			bsPeerID, err := peer.Decode(cfg.Dealmaking.BitswapPeerID)
			if err != nil {
				return nil, err
			}
			peerConfig[bsPeerID] = bitswapProtocols
		}
		return protocolproxy.NewProtocolProxy(h, peerConfig)
	}
}

func HandleProtocolProxy(lc fx.Lifecycle, pp *protocolproxy.ProtocolProxy) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Info("starting libp2p protocol proxy")
			pp.Start(ctx)
			return nil
		},
		OnStop: func(context.Context) error {
			log.Info("stopping libp2p protocol proxy")
			pp.Close()
			return nil
		},
	})
}
