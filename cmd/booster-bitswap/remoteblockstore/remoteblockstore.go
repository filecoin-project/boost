package remoteblockstore

import (
	"context"
	"errors"

	"github.com/filecoin-project/boost/tracing"
	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel/attribute"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

var log = logging.Logger("remote-blockstore")

var _ blockstore.Blockstore = (*RemoteBlockstore)(nil)

type RemoteBlockstoreAPI interface {
	BlockstoreGet(ctx context.Context, c cid.Cid) ([]byte, error)
	BlockstoreHas(ctx context.Context, c cid.Cid) (bool, error)
	BlockstoreGetSize(ctx context.Context, c cid.Cid) (int, error)
}

// RemoteBlockstore is a read-only blockstore over all cids across all pieces on a provider.
type RemoteBlockstore struct {
	api RemoteBlockstoreAPI
}

func NewRemoteBlockstore(api RemoteBlockstoreAPI) blockstore.Blockstore {
	return &RemoteBlockstore{
		api: api,
	}
}

func (ro *RemoteBlockstore) Get(ctx context.Context, c cid.Cid) (b blocks.Block, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "rbls.get")
	defer span.End()
	span.SetAttributes(attribute.String("cid", c.String()))

	log.Debugw("Get", "cid", c)
	data, err := ro.api.BlockstoreGet(ctx, c)
	log.Debugw("Get response", "cid", c, "error", err)
	if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(data, c)
}

func (ro *RemoteBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	ctx, span := tracing.Tracer.Start(ctx, "rbls.has")
	defer span.End()
	span.SetAttributes(attribute.String("cid", c.String()))

	log.Debugw("Has", "cid", c)
	has, err := ro.api.BlockstoreHas(ctx, c)
	log.Debugw("Has response", "cid", c, "has", has, "error", err)
	return has, err
}

func (ro *RemoteBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "rbls.get_size")
	defer span.End()
	span.SetAttributes(attribute.String("cid", c.String()))

	log.Debugw("GetSize", "cid", c)
	size, err := ro.api.BlockstoreGetSize(ctx, c)
	log.Debugw("GetSize response", "cid", c, "size", size, "error", err)
	return size, err
}

// --- UNSUPPORTED BLOCKSTORE METHODS -------
func (ro *RemoteBlockstore) DeleteBlock(context.Context, cid.Cid) error {
	return errors.New("unsupported operation DeleteBlock")
}
func (ro *RemoteBlockstore) HashOnRead(_ bool) {}
func (ro *RemoteBlockstore) Put(context.Context, blocks.Block) error {
	return errors.New("unsupported operation Put")
}
func (ro *RemoteBlockstore) PutMany(context.Context, []blocks.Block) error {
	return errors.New("unsupported operation PutMany")
}
func (ro *RemoteBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errors.New("unsupported operation AllKeysChan")
}
