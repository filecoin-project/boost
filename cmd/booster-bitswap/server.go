package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/filecoin-project/boost/loadbalancer"
	bsnetwork "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/server"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/network"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

type BitswapServer struct {
	port        int
	remoteStore blockstore.Blockstore
	papi        PeerIDAPI

	ctx    context.Context
	cancel context.CancelFunc
	server *server.Server
}

type PeerIDAPI interface {
	DealsGetBitswapPeerID(ctx context.Context) (peer.ID, error)
	DealsSetBitswapPeerID(ctx context.Context, p peer.ID) error
}

func NewBitswapServer(port int, remoteStore blockstore.Blockstore, papi PeerIDAPI) *BitswapServer {
	return &BitswapServer{port: port, remoteStore: remoteStore, papi: papi}
}

func (s *BitswapServer) Start(ctx context.Context, dataDir string, balancer peer.AddrInfo, overrideExistingPeerID bool) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	if dataDir == "" {
		return fmt.Errorf("dataDir must be set")
	}

	if err := os.MkdirAll(dataDir, 0744); err != nil {
		return err
	}

	peerkey, err := loadPeerKey(dataDir)
	if err != nil {
		return err
	}

	selfPid, err := peer.IDFromPrivateKey(peerkey)
	if err != nil {
		return err
	}
	existingPid, err := s.papi.DealsGetBitswapPeerID(ctx)
	peerIDNotSet := err != nil && err.Error() == "no bitswap peer id set"
	if err != nil && !peerIDNotSet {
		return err
	}
	matchesPid := existingPid == selfPid
	log.Infow("get/set peer id of bitswap from boost", "local", selfPid.String(), "boost", existingPid.String(), "boost not set", peerIDNotSet, "override", overrideExistingPeerID)
	// error if a peer id is set that is different and we aren't overriding
	if !peerIDNotSet && !matchesPid && !overrideExistingPeerID {
		return errors.New("bitswap peer id does not match boost node configuration. use --override-peer-id to force a change")
	}
	if peerIDNotSet || (!matchesPid && overrideExistingPeerID) {
		err = s.papi.DealsSetBitswapPeerID(ctx, selfPid)
		if err != nil {
			return err
		}
	}

	host, err := libp2p.New(
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", s.port),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic", s.port),
		),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),
		libp2p.Muxer("/mplex/6.7.0", mplex.DefaultTransport),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
		libp2p.Identity(peerkey),
		libp2p.ResourceManager(network.NullResourceManager),
	)
	if err != nil {
		return err
	}

	host, err = loadbalancer.NewServiceNode(ctx, host, balancer)
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

	log.Infow("bitswap server running", "multiaddrs", host.Addrs(), "peerId", host.ID())
	return nil
}

func (s *BitswapServer) Stop() error {
	s.cancel()
	return s.server.Close()
}

func loadPeerKey(dataDir string) (crypto.PrivKey, error) {
	var peerkey crypto.PrivKey
	keyPath := filepath.Join(dataDir, "peerkey")
	keyFile, err := os.ReadFile(keyPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		log.Infof("Generating new peer key...")

		key, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}
		peerkey = key

		data, err := crypto.MarshalPrivateKey(key)
		if err != nil {
			return nil, err
		}

		if err := os.WriteFile(keyPath, data, 0600); err != nil {
			return nil, err
		}
	} else {
		key, err := crypto.UnmarshalPrivateKey(keyFile)
		if err != nil {
			return nil, err
		}

		peerkey = key
	}

	if peerkey == nil {
		panic("sanity check: peer key is uninitialized")
	}

	return peerkey, nil
}
