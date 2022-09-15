package modules

import (
	"context"
	"fmt"

	"github.com/filecoin-project/boost/loadbalancer"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	"github.com/filecoin-project/boost/retrievalmarket/types"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-bitswap/network"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
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

func NewLoadBalancer(cfg *config.Boost) func(h host.Host) (*loadbalancer.LoadBalancer, error) {
	return func(h host.Host) (*loadbalancer.LoadBalancer, error) {
		peerConfig := map[peer.ID][]protocol.ID{}
		if cfg.Dealmaking.BitswapPeerID != "" {
			bsPeerID, err := peer.Decode(cfg.Dealmaking.BitswapPeerID)
			if err != nil {
				return nil, err
			}
			peerConfig[bsPeerID] = []protocol.ID{
				network.ProtocolBitswap,
				network.ProtocolBitswapNoVers,
				network.ProtocolBitswapOneOne,
				network.ProtocolBitswapOneZero,
			}
		}
		return loadbalancer.NewLoadBalancer(h, peerConfig)
	}
}

func HandleLoadBalancer(lc fx.Lifecycle, lb *loadbalancer.LoadBalancer) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Debug("starting load balancer")
			lb.Start(ctx)
			return nil
		},
		OnStop: func(context.Context) error {
			log.Debug("stopping load balancer")
			return lb.Close()
		},
	})
}

func NewSetBitswapPeerIDFunc(r lotus_repo.LockedRepo, lb *loadbalancer.LoadBalancer) (dtypes.SetBitswapPeerIDFunc, error) {
	return func(p peer.ID) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.BitswapPeerID = peer.Encode(p)
		})
		if err != nil {
			return
		}
		peerConfig := map[peer.ID][]protocol.ID{}
		peerConfig[p] = []protocol.ID{
			network.ProtocolBitswap,
			network.ProtocolBitswapNoVers,
			network.ProtocolBitswapOneOne,
			network.ProtocolBitswapOneZero,
		}
		lb.UpdatePeerConfig(peerConfig)
		return
	}, nil
}

func NewGetBitswapPeerIDFunc(r lotus_repo.LockedRepo) (dtypes.GetBitswapPeerIDFunc, error) {
	return func() (p peer.ID, err error) {
		var pString string
		err = mutateCfg(r, func(cfg *config.Boost) {
			pString = cfg.Dealmaking.BitswapPeerID
		})
		if err != nil {
			return
		}
		if pString == "" {
			err = fmt.Errorf("no bitswap peer id set")
			return
		}
		p, err = peer.Decode(pString)
		return
	}, nil
}
