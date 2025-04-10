package main

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/protocolproxy"
	bsnetwork "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/boxo/bitswap/server"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	nilrouting "github.com/libp2p/go-libp2p-routing-helpers"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Filter interface {
	FulfillRequest(p peer.ID, c cid.Cid) (bool, error)
}

type BitswapServer struct {
	remoteStore blockstore.Blockstore
	filter      Filter
	ctx         context.Context
	cancel      context.CancelFunc
	proxy       *peer.AddrInfo
	server      *server.Server
	host        host.Host
}

type BitswapServerOptions struct {
	EngineBlockstoreWorkerCount int
	EngineTaskWorkerCount       int
	TaskWorkerCount             int
	TargetMessageSize           int
	MaxOutstandingBytesPerPeer  int
}

func NewBitswapServer(
	remoteStore blockstore.Blockstore,
	host host.Host,
	filter Filter,
) *BitswapServer {
	return &BitswapServer{remoteStore: remoteStore, host: host, filter: filter}
}

const protectTag = "bitswap-server-to-proxy"

func (s *BitswapServer) Start(ctx context.Context, proxy *peer.AddrInfo, opts *BitswapServerOptions) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.proxy = proxy

	host := s.host
	if proxy != nil {
		// If there's a proxy host, connect to the proxy over libp2p
		log.Infow("connecting to proxy", "proxy", proxy)
		err := s.host.Connect(s.ctx, *proxy)
		if err != nil {
			return fmt.Errorf("connecting to proxy %s: %w", proxy, err)
		}
		s.host.ConnManager().Protect(proxy.ID, protectTag)

		// Create a forwarding host that registers routes with the proxy
		host = protocolproxy.NewForwardingHost(s.host, *proxy)
	}

	// Start a bitswap server on the provider
	nilRouter := nilrouting.Null{}
	bsopts := []server.Option{
		server.EngineBlockstoreWorkerCount(opts.EngineBlockstoreWorkerCount),
		server.EngineTaskWorkerCount(opts.EngineTaskWorkerCount),
		server.MaxOutstandingBytesPerPeer(opts.MaxOutstandingBytesPerPeer),
		server.TaskWorkerCount(opts.TaskWorkerCount),
		server.WithTargetMessageSize(opts.TargetMessageSize),
		server.WithPeerBlockRequestFilter(func(p peer.ID, c cid.Cid) bool {
			fulfill, err := s.filter.FulfillRequest(p, c)
			// peer request filter expects a true if the request should be fulfilled, so
			// we only return true for requests that aren't filtered and have no errors
			if err != nil {
				log.Errorf("error running bitswap filter: %s", err.Error())
				return false
			}
			return fulfill
		}),
	}
	net := bsnetwork.NewFromIpfsHost(host, nilRouter)
	s.server = server.New(s.ctx, net, s.remoteStore, bsopts...)
	net.Start(s.server)

	log.Infow("bitswap server running", "multiaddrs", host.Addrs(), "peerId", host.ID())
	if proxy != nil {
		go s.keepProxyConnectionAlive(s.ctx, *proxy)
		log.Infow("with proxy", "multiaddrs", proxy.Addrs, "peerId", proxy.ID)
	}

	return nil
}

func (s *BitswapServer) Stop() error {
	if s.proxy != nil {
		s.host.ConnManager().Unprotect(s.proxy.ID, protectTag)
	}
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
