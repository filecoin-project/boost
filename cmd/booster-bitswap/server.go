package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	indexbs "github.com/filecoin-project/boost/cmd/booster-bitswap/indexbs"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-state-types/abi"
	bsnetwork "github.com/ipfs/go-bitswap/network"
	"github.com/ipfs/go-bitswap/server"
	"github.com/ipfs/go-cid"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/network"
	crypto "github.com/libp2p/go-libp2p-crypto"
	"github.com/libp2p/go-libp2p/p2p/muxer/mplex"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-tcp-transport"
	"github.com/multiformats/go-multihash"
)

var ErrNotFound = errors.New("not found")

type BitswapServer struct {
	port int
	api  BitswapServerAPI

	ctx    context.Context
	cancel context.CancelFunc
	server *server.Server
}

type BitswapServerAPI interface {
	PiecesContainingMultihash(mh multihash.Multihash) ([]cid.Cid, error)
	GetMaxPieceOffset(pieceCid cid.Cid) (uint64, error)
	GetPieceInfo(pieceCID cid.Cid) (*piecestore.PieceInfo, error)
	IsUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error)
	UnsealSectorAt(ctx context.Context, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error)
}

func NewBitswapServer(path string, port int, api BitswapServerAPI) *BitswapServer {
	return &BitswapServer{port: port, api: api}
}

func (s *BitswapServer) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	sf := indexbs.PieceSelectorF(func(c cid.Cid, pieceCIDs []cid.Cid) (cid.Cid, error) {
		for _, pieceCID := range pieceCIDs {
			pi, err := s.api.GetPieceInfo(pieceCID)
			if err != nil {
				return cid.Undef, fmt.Errorf("failed to get piece info: %w", err)
			}
			isUnsealed := s.pieceInUnsealedSector(s.ctx, *pi)
			if isUnsealed {
				return pieceCID, nil
			}
		}

		return cid.Undef, indexbs.ErrNoPieceSelected
	})

	rbs, err := indexbs.NewIndexBackedBlockstore(s.api, sf, 100)
	if err != nil {
		return fmt.Errorf("failed to create index backed blockstore: %w", err)
	}

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
	s.server = server.New(ctx, bsnetwork.NewFromIpfsHost(r.h, nilRouter), rbs, bsopts...)

	fmt.Printf("bitswap server running on SP, addrs: %s, peerID: %s", r.h.Addrs(), r.h.ID())
	log.Infow("bitswap server running on SP", "multiaddrs", r.h.Addrs(), "peerId", r.h.ID())
	return nil
}

func (s *BitswapServer) Stop() error {
	s.cancel()
	return s.server.Close()
}

func (s *BitswapServer) pieceInUnsealedSector(ctx context.Context, pieceInfo piecestore.PieceInfo) bool {
	for _, di := range pieceInfo.Deals {
		isUnsealed, err := s.api.IsUnsealed(ctx, di.SectorID, di.Offset.Unpadded(), di.Length.Unpadded())
		if err != nil {
			log.Errorf("failed to find out if sector %d is unsealed, err=%s", di.SectorID, err)
			continue
		}
		if isUnsealed {
			return true
		}
	}

	return false
}
