package remoteblockstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/filecoin-project/boost/metrics"
	"github.com/filecoin-project/boost/tracing"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"go.opencensus.io/stats"
	"go.opentelemetry.io/otel/attribute"
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
	stats.Record(ctx, metrics.BitswapRblsGetRequestCount.M(1))

	log.Debugw("Get", "cid", c)
	data, err := ro.api.BlockstoreGet(ctx, c)
	err = normalizeError(err)
	log.Debugw("Get response", "cid", c, "size", len(data), "error", err)
	if err != nil {
		log.Infow("Get failed", "cid", c, "error", err)
		stats.Record(ctx, metrics.BitswapRblsGetFailResponseCount.M(1))
		return nil, err
	}
	log.Infow("Get", "cid", c, "size", len(data))
	stats.Record(ctx, metrics.BitswapRblsGetSuccessResponseCount.M(1))
	return blocks.NewBlockWithCid(data, c)
}

func (ro *RemoteBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	ctx, span := tracing.Tracer.Start(ctx, "rbls.has")
	defer span.End()
	span.SetAttributes(attribute.String("cid", c.String()))
	stats.Record(ctx, metrics.BitswapRblsHasRequestCount.M(1))

	log.Debugw("Has", "cid", c)
	has, err := ro.api.BlockstoreHas(ctx, c)
	log.Debugw("Has response", "cid", c, "has", has, "error", err)
	if err != nil {
		stats.Record(ctx, metrics.BitswapRblsHasFailResponseCount.M(1))
	} else {
		stats.Record(ctx, metrics.BitswapRblsHasSuccessResponseCount.M(1))
	}
	return has, err
}

func (ro *RemoteBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "rbls.get_size")
	defer span.End()
	span.SetAttributes(attribute.String("cid", c.String()))
	stats.Record(ctx, metrics.BitswapRblsGetSizeRequestCount.M(1))

	log.Debugw("GetSize", "cid", c)
	size, err := ro.api.BlockstoreGetSize(ctx, c)
	err = normalizeError(err)
	log.Debugw("GetSize response", "cid", c, "size", size, "error", err)
	if err != nil {
		stats.Record(ctx, metrics.BitswapRblsGetSizeFailResponseCount.M(1))
	} else {
		stats.Record(ctx, metrics.BitswapRblsGetSizeSuccessResponseCount.M(1))
	}
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

const (
	ipldNotFoundUndefCid = "ipld: could not find node"
	ipldNotFoundPrefix   = "ipld: could not find "
)

// go-bitswap expects that if a block is not found, the blockstore methods
// will return an ipld.ErrNotFound
// However the remote blockstore calls across an RPC boundary, which turns
// errors into strings. Therefore we need to parse the string and return
// the ipld.ErrNotFound type where appropriate.
func normalizeError(err error) error {
	if err == nil {
		return nil
	}
	errMsg := err.Error()

	// First check for ErrNotFound with an undefined cid
	idx := strings.Index(errMsg, ipldNotFoundUndefCid)
	if idx != -1 {
		rest := errMsg[:idx]
		if len(rest) > 2 && rest[len(rest)-2:] != ": " {
			rest += ": "
		}
		return fmt.Errorf("%s%w", rest, format.ErrNotFound{})
	}

	// Check for ErrNotFound with a cid
	idx = strings.Index(errMsg, ipldNotFoundPrefix)
	if idx != -1 {
		cidStr := errMsg[idx+len(ipldNotFoundPrefix):]
		c, e := cid.Parse(cidStr)
		if e != nil {
			return err
		}

		rest := errMsg[:idx]
		if len(rest) > 2 && rest[len(rest)-2:] != ": " {
			rest += ": "
		}
		return fmt.Errorf("%s%w", rest, format.ErrNotFound{Cid: c})
	}

	// Check for any error with the string "not found"
	idx = strings.Index(strings.ToLower(errMsg), "not found")
	if idx != -1 {
		return format.ErrNotFound{}
	}

	return err
}
