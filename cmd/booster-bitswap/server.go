package main

import (
	"context"

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
	server      *server.Server
	host        host.Host
}

func NewBitswapServer(remoteStore blockstore.Blockstore, host host.Host, blockFilter BlockFilter) *BitswapServer {
	return &BitswapServer{remoteStore: remoteStore, host: host, blockFilter: blockFilter}
}

func (s *BitswapServer) Start(ctx context.Context, balancer peer.AddrInfo) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	host, err := protocolproxy.NewForwardingHost(ctx, s.host, balancer)
	if err != nil {
		return err
	}

	// start a bitswap session on the provider
	nilRouter, err := nilrouting.ConstructNilRouting(ctx, nil, nil, nil)
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
	s.server = server.New(ctx, net, s.remoteStore, bsopts...)
	net.Start(s.server)

	log.Infow("bitswap server running", "multiaddrs", host.Addrs(), "peerId", host.ID())
	return nil
}

func (s *BitswapServer) Stop() error {
	s.cancel()
	return s.server.Close()
}
