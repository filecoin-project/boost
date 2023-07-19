package lib

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/retrievalmarket/server"
	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/indexbs"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// IndexBackedBlockstoreDagstore implements the dagstore interface needed
// by the IndexBackedBlockstore.
// The implementation of ShardsContainingCid handles identity cids.
type IndexBackedBlockstoreDagstore struct {
	dagstore.Interface
}

var _ dagstore.Interface = (*IndexBackedBlockstoreDagstore)(nil)

func NewIndexBackedBlockstoreDagstore(ds dagstore.Interface) indexbs.IdxBstoreDagstore {
	return &IndexBackedBlockstoreDagstore{Interface: ds}
}

// ShardsContainingCid checks the db for shards containing the given cid.
// If there are no shards with that cid, it checks if the shard is an identity
// cid, and gets the shards containing the identity cid's child cids.
// This is for the case where the identity cid was not stored in the original
// CAR file's index (but the identity cid's child cids are in the index).
func (i *IndexBackedBlockstoreDagstore) ShardsContainingCid(ctx context.Context, c cid.Cid) ([]shard.Key, error) {
	shards, err := i.Interface.ShardsContainingMultihash(ctx, c.Hash())
	if err == nil {
		return shards, nil
	}

	var idErr error
	piecesWithTargetBlock, idErr := server.GetCommonPiecesFromIdentityCidLinks(ctx, func(ctx context.Context, mh multihash.Multihash) ([]cid.Cid, error) {
		return i.piecesContainingBlock(ctx, mh)
	}, c)
	if idErr != nil {
		return nil, fmt.Errorf("getting common pieces for cid %s: %w", c, idErr)
	}
	if len(piecesWithTargetBlock) == 0 {
		// No pieces found for cid: return the original error from the call to
		// ShardsContainingMultihash above
		return nil, fmt.Errorf("getting pieces for cid %s: %w", c, err)
	}

	shards = make([]shard.Key, 0, len(piecesWithTargetBlock))
	for _, pcid := range piecesWithTargetBlock {
		shards = append(shards, shard.KeyFromCID(pcid))
	}
	return shards, nil
}

func (i *IndexBackedBlockstoreDagstore) piecesContainingBlock(ctx context.Context, mh multihash.Multihash) ([]cid.Cid, error) {
	shards, err := i.Interface.ShardsContainingMultihash(ctx, mh)
	if err != nil {
		return nil, fmt.Errorf("finding shards containing child mh %s: %w", mh, err)
	}
	pcids := make([]cid.Cid, 0, len(shards))
	for _, s := range shards {
		pcid, err := cid.Parse(s.String())
		if err != nil {
			return nil, fmt.Errorf("parsing shard into cid: %w", err)
		}
		pcids = append(pcids, pcid)
	}
	return pcids, nil
}
