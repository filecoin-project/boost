package dagstore

import (
	"context"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/dagstore"
)

// Blockstore promotes a dagstore.ReadBlockstore to a full closeable Blockstore,
// stubbing out the write methods with erroring implementations.
type Blockstore struct {
	dagstore.ReadBlockstore
	io.Closer
}

var _ bstore.Blockstore = (*Blockstore)(nil)

func (b *Blockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return xerrors.Errorf("DeleteBlock called but not implemented")
}

func (b *Blockstore) Put(ctx context.Context, block blocks.Block) error {
	return xerrors.Errorf("Put called but not implemented")
}

func (b *Blockstore) PutMany(ctx context.Context, blocks []blocks.Block) error {
	return xerrors.Errorf("PutMany called but not implemented")
}
