package stores

import (
	"context"
	"io"

	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"

	"github.com/filecoin-project/dagstore"

	"github.com/filecoin-project/go-fil-markets/storagemarket"
)

type ClosableBlockstore interface {
	bstore.Blockstore
	io.Closer
}

// DAGStoreWrapper hides the details of the DAG store implementation from
// the other parts of go-fil-markets.
type DAGStoreWrapper interface {
	// RegisterShard loads a CAR file into the DAG store and builds an
	// index for it, sending the result on the supplied channel on completion
	RegisterShard(ctx context.Context, pieceCid cid.Cid, carPath string, eagerInit bool, resch chan dagstore.ShardResult) error

	// LoadShard fetches the data for a shard and provides a blockstore
	// interface to it.
	//
	// The blockstore must be closed to release the shard.
	LoadShard(ctx context.Context, pieceCid cid.Cid) (ClosableBlockstore, error)

	// MigrateDeals migrates the supplied storage deals into the DAG store.
	MigrateDeals(ctx context.Context, deals []storagemarket.MinerDeal) (bool, error)

	// Close closes the dag store wrapper.
	Close() error
}

// RegisterShardSync calls the DAGStore RegisterShard method and waits
// synchronously in a dedicated channel until the registration has completed
// fully.
func RegisterShardSync(ctx context.Context, ds DAGStoreWrapper, pieceCid cid.Cid, carPath string, eagerInit bool) error {
	resch := make(chan dagstore.ShardResult, 1)
	if err := ds.RegisterShard(ctx, pieceCid, carPath, eagerInit, resch); err != nil {
		return err
	}

	// TODO: Can I rely on RegisterShard to return an error if the context times out?
	select {
	case <-ctx.Done():
		return ctx.Err()
	case res := <-resch:
		return res.Error
	}
}
