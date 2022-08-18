package remoteblockstore

import (
	"context"
	"errors"

	blocks "github.com/ipfs/go-block-format"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

var logbs = logging.Logger("remote-blockstore")

var ErrBlockNotFound = errors.New("block not found")
var ErrNotFound = errors.New("not found")

var _ blockstore.Blockstore = (*RemoteBlockstore)(nil)

// ErrNoPieceSelected means that the piece selection function rejected all of the given pieces.
var ErrNoPieceSelected = errors.New("no piece selected")

// PieceSelectorF helps select a piece to fetch a cid from if the given cid is present in multiple pieces.
// It should return `ErrNoPieceSelected` if none of the given piece is selected.
type PieceSelectorF func(c cid.Cid, pieceCids []cid.Cid) (cid.Cid, error)

type RemoteBlockstoreAPI interface {
	BoostGetBlock(ctx context.Context, c cid.Cid) ([]byte, error)
}

// RemoteBlockstore is a read only blockstore over all cids across all pieces on a provider.
type RemoteBlockstore struct {
	api RemoteBlockstoreAPI
}

func NewRemoteBlockstore(api RemoteBlockstoreAPI) blockstore.Blockstore {
	return &RemoteBlockstore{
		api: api,
	}
}

func (ro *RemoteBlockstore) Get(ctx context.Context, c cid.Cid) (b blocks.Block, err error) {
	data, err := ro.api.BoostGetBlock(ctx, c)
	if err != nil {
		return nil, err
	}
	return blocks.NewBlock(data), nil
}

func (ro *RemoteBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, err := ro.api.BoostGetBlock(ctx, c)
	if err != nil {
		if format.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (ro *RemoteBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	data, err := ro.api.BoostGetBlock(ctx, c)
	if err != nil {
		return 0, err
	}
	return len(data), nil
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
