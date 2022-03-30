package bitswap

import (
	"context"
	"fmt"

	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"

	logging "github.com/ipfs/go-log/v2"

	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/libp2p/go-libp2p-core/host"

	"github.com/ipfs/go-bitswap"
	nilrouting "github.com/ipfs/go-ipfs-routing/none"

	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/indexbs"
	legacy_retrievalmarket "github.com/filecoin-project/go-fil-markets/retrievalmarket"
	bsnetwork "github.com/ipfs/go-bitswap/network"
)

var log = logging.Logger("boost-bitswap")

type RetrievalServer struct {
	dagst    *dagstore.DAGStore
	rp       legacy_retrievalmarket.RetrievalProvider
	h        host.Host
	bsServer exchange.Interface
}

func NewRetrievalServer(d *dagstore.DAGStore, rp legacy_retrievalmarket.RetrievalProvider, h host.Host) *RetrievalServer {
	return &RetrievalServer{
		dagst: d,
		rp:    rp,
		h:     h,
	}
}

func (r *RetrievalServer) Start(ctx context.Context) error {
	sf := indexbs.ShardSelectorF(func(c cid.Cid, shards []shard.Key) (shard.Key, error) {
		for _, sk := range shards {
			pieceCid, err := cid.Parse(sk.String())
			if err != nil {
				return shard.Key{}, fmt.Errorf("failed to parse cid")
			}
			b, err := r.rp.IsFreeAndUnsealed(ctx, c, pieceCid)
			if err != nil {
				return shard.Key{}, fmt.Errorf("failed to verify is piece is free and unsealed")
			}
			if b {
				return sk, nil
			}
		}

		return shard.Key{}, indexbs.ErrNoShardSelected
	})

	rbs, err := indexbs.NewIndexBackedBlockstore(r.dagst, sf, 100)
	if err != nil {
		return fmt.Errorf("failed to create index backed blockstore: %w", err)
	}

	// start a bitswap session on the provider
	nilRouter, err := nilrouting.ConstructNilRouting(ctx, nil, nil, nil)
	if err != nil {
		return err
	}
	bsopts := []bitswap.Option{bitswap.MaxOutstandingBytesPerPeer(1 << 20)}
	bsServer := bitswap.New(ctx, bsnetwork.NewFromIpfsHost(r.h, nilRouter), rbs, bsopts...)
	r.bsServer = bsServer

	fmt.Printf("bitswap server running on SP, addrs: %s, peerID: %s", r.h.Addrs(), r.h.ID())
	log.Infow("bitswap server running on SP", "multiaddrs", r.h.Addrs(), "peerId", r.h.ID())
	return nil
}

func (r *RetrievalServer) Stop(_ context.Context) error {
	return r.bsServer.Close()
}
