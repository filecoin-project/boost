package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	bsnetwork "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/server"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

var ErrNotFound = errors.New("not found")

type BitswapServer struct {
	port        int
	remoteStore blockstore.Blockstore

	ctx    context.Context
	cancel context.CancelFunc
	server *server.Server
}

func NewBitswapServer(path string, port int, remoteStore blockstore.Blockstore) *BitswapServer {
	return &BitswapServer{port: port, remoteStore: remoteStore}
}

func (s *BitswapServer) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	// setup libp2p host
	privKey, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)

	host, err := libp2p.New(
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", s.port),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic", s.port),
		),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),
		libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.Identity(privKey),
		libp2p.ResourceManager(network.NullResourceManager),
	)
	if err != nil {
		return err
	}

	// start a bitswap session on the provider
	nilRouter, err := nilrouting.ConstructNilRouting(ctx, nil, nil, nil)
	if err != nil {
		return err
	}
	bsopts := []server.Option{server.MaxOutstandingBytesPerPeer(1 << 20)}
	net := bsnetwork.NewFromIpfsHost(host, nilRouter)
	s.server = server.New(ctx, net, s.remoteStore, bsopts...)
	net.Start(s.server)

	fmt.Printf("bitswap server running on SP, addrs: %s, peerID: %s\n", host.Addrs(), host.ID())
	log.Infow("bitswap server running on SP", "multiaddrs", host.Addrs(), "peerId", host.ID())
	return nil
}

func (s *BitswapServer) Stop() error {
	s.cancel()
	return s.server.Close()
}
