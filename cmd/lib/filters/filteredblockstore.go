package filters

import (
	"context"
	"fmt"

	blockstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

type FilteredBlockstore struct {
	blockstore.Blockstore
	filter Filter
}

func NewFilteredBlockstore(bstore blockstore.Blockstore, filter Filter) blockstore.Blockstore {
	return &FilteredBlockstore{Blockstore: bstore, filter: filter}
}

func (f *FilteredBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	ok, err := f.filter.FulfillRequest("", c)
	if err != nil {
		err = fmt.Errorf("filter error for cid %s: %w", c, err)
		log.Errorf("%s", err)
		return false, err
	}
	if !ok {
		log.Debugf("filter rejected Has cid %s", c)
		return false, nil
	}
	return f.Blockstore.Has(ctx, c)
}

func (f *FilteredBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	ok, err := f.filter.FulfillRequest("", c)
	if err != nil {
		err = fmt.Errorf("filter error for cid %s: %w", c, err)
		log.Errorf("%s", err)
		return nil, err
	}
	if !ok {
		log.Debugf("filter rejected Get cid %s", c)
		return nil, fmt.Errorf("cid %s rejected by filter: %w", c, ipld.ErrNotFound{Cid: c})
	}
	return f.Blockstore.Get(ctx, c)
}

func (f *FilteredBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	ok, err := f.filter.FulfillRequest("", c)
	if err != nil {
		err = fmt.Errorf("filter error for cid %s: %w", c, err)
		log.Errorf("%s", err)
		return 0, err
	}
	if !ok {
		log.Debugf("filter rejected GetSize cid %s", c)
		return 0, fmt.Errorf("cid %s rejected by filter: %w", c, ipld.ErrNotFound{Cid: c})
	}
	return f.Blockstore.GetSize(ctx, c)
}
