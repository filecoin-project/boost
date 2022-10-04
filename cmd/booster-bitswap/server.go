package main

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/protocolproxy"
	bsnetwork "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/server"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	"github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

type BlockFilter interface {
	IsFiltered(c cid.Cid) (bool, error)
}

type BitswapServer struct {
	remoteStore blockstore.Blockstore
	blockFilter BlockFilter
	ctx         context.Context
	cancel      context.CancelFunc
	proxy       peer.AddrInfo
	server      *server.Server
	host        host.Host
}

func NewBitswapServer(remoteStore blockstore.Blockstore, host host.Host, blockFilter BlockFilter) *BitswapServer {
	return &BitswapServer{remoteStore: remoteStore, host: host, blockFilter: blockFilter}
}

const protectTag = "bitswap-server-to-proxy"

func (s *BitswapServer) Start(ctx context.Context, proxy peer.AddrInfo) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.proxy = proxy

	// Connect to the proxy over libp2p
	log.Infow("connecting to proxy", "proxy", proxy)
	err := s.host.Connect(s.ctx, proxy)
	if err != nil {
		return fmt.Errorf("connecting to proxy %s: %w", proxy, err)
	}
	s.host.ConnManager().Protect(proxy.ID, protectTag)

	host, err := protocolproxy.NewForwardingHost(s.host, proxy)
	if err != nil {
		return err
	}

	// start a bitswap session on the provider
	nilRouter, err := nilrouting.ConstructNilRouting(s.ctx, nil, nil, nil)
	if err != nil {
		return err
	}
	bsopts := []server.Option{server.MaxOutstandingBytesPerPeer(1 << 20), server.WithPeerBlockRequestFilter(func(p peer.ID, c cid.Cid) bool {
		filtered, err := s.blockFilter.IsFiltered(c)
		// peer request block filter expects a true if the request should be fulfilled, so
		// we only return true for cids that aren't filtered and have no errors
		return !filtered && err == nil
	})}
	net := bsnetwork.NewFromIpfsHost(host, nilRouter)
	s.server = server.New(s.ctx, net, s.remoteStore, bsopts...)
	net.Start(s.server)

	go s.keepProxyConnectionAlive(s.ctx, proxy)

	log.Infow("bitswap server running", "multiaddrs", host.Addrs(), "peerId", host.ID())
	return nil
}

func (s *BitswapServer) Stop() error {
	s.host.ConnManager().Unprotect(s.proxy.ID, protectTag)
	s.cancel()
	return s.server.Close()
}

func (s *BitswapServer) keepProxyConnectionAlive(ctx context.Context, proxy peer.AddrInfo) {
	// Periodically ensure that the connection over libp2p to the proxy is alive
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	connected := true
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := s.host.Connect(ctx, proxy)
			if err != nil {
				connected = false
				log.Warnw("failed to connect to proxy", "address", proxy)
			} else if !connected {
				log.Infow("reconnected to proxy", "address", proxy)
				connected = true
			}
		}
	}
}
