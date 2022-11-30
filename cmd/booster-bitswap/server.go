package main

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/cmd/booster-bitswap/filters"
	"github.com/filecoin-project/boost/protocolproxy"
	bsnetwork "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/server"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	"github.com/libp2p/go-libp2p/core/host"
	peer "github.com/libp2p/go-libp2p/core/peer"
)

type Filter interface {
	FulfillRequest(p peer.ID, c cid.Cid, ss filters.ServerState) (bool, error)
}

type BandwidthMeasure interface {
	RecordBytesSent(sent uint64)
}

type BitswapServer struct {
	remoteStore      blockstore.Blockstore
	filter           Filter
	bandwidthMeasure BandwidthMeasure
	ctx              context.Context
	cancel           context.CancelFunc
	proxy            *peer.AddrInfo
	server           *server.Server
	host             host.Host
}

func NewBitswapServer(remoteStore blockstore.Blockstore, host host.Host, filter Filter, bandwidthMeasure BandwidthMeasure) *BitswapServer {
	return &BitswapServer{remoteStore: remoteStore, host: host, filter: filter, bandwidthMeasure: bandwidthMeasure}
}

const protectTag = "bitswap-server-to-proxy"

func (s *BitswapServer) Start(ctx context.Context, proxy *peer.AddrInfo) error {
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
	nilRouter, err := nilrouting.ConstructNilRouting(s.ctx, nil, nil, nil)
	if err != nil {
		return err
	}
	bsopts := []server.Option{
		server.MaxOutstandingBytesPerPeer(1 << 20),
		server.WithPeerBlockRequestFilter(func(p peer.ID, c cid.Cid) bool {
			ss := filters.ServerState{}
			// TODO: this really isn't ideal - we should find a way to get these values passed into the peer filter directly
			if s.server != nil {
				ss = filters.ServerState{
					TotalRequestsInProgress:   s.server.TotalWants(),
					RequestsInProgressForPeer: s.server.WantCountForPeer(p),
				}
			}
			fulfill, err := s.filter.FulfillRequest(p, c, ss)
			// peer request filter expects a true if the request should be fulfilled, so
			// we only return true for requests that aren't filtered and have no errors
			return fulfill && err == nil
		}),
		server.WithOnDataSentListener(func(p peer.ID, dataSentTotal uint64, blockSentTotal uint64) {
			s.bandwidthMeasure.RecordBytesSent(dataSentTotal)
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
