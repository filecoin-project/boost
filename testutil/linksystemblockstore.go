package testutil

// borrowed from github.com/filecoin-project/lassie/pkg/internal/itest/linksystemutil/linksystemblockstore.go

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var _ blockstore.Blockstore = (*LinkSystemBlockstore)(nil)

type LinkSystemBlockstore struct {
	lsys linking.LinkSystem
}

func NewLinkSystemBlockstore(lsys linking.LinkSystem) *LinkSystemBlockstore {
	return &LinkSystemBlockstore{lsys}
}

func (lsbs *LinkSystemBlockstore) DeleteBlock(ctx context.Context, c cid.Cid) error {
	return errors.New("not supported")
}

func (lsbs *LinkSystemBlockstore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, err := lsbs.lsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (lsbs *LinkSystemBlockstore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	rdr, err := lsbs.lsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	_, err = io.Copy(&buf, rdr)
	if err != nil {
		return nil, err
	}
	return blocks.NewBlockWithCid(buf.Bytes(), c)
}

func (lsbs *LinkSystemBlockstore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	rdr, err := lsbs.lsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
	if err != nil {
		return 0, err
	}
	i, err := io.Copy(io.Discard, rdr)
	if err != nil {
		return 0, err
	}
	return int(i), nil
}

func (lsbs *LinkSystemBlockstore) Put(ctx context.Context, blk blocks.Block) error {
	w, wc, err := lsbs.lsys.StorageWriteOpener(linking.LinkContext{Ctx: ctx})
	if err != nil {
		return err
	}
	if _, err = io.Copy(w, bytes.NewReader(blk.RawData())); err != nil {
		return err
	}
	return wc(cidlink.Link{Cid: blk.Cid()})
}

func (lsbs *LinkSystemBlockstore) PutMany(ctx context.Context, blks []blocks.Block) error {
	for _, blk := range blks {
		if err := lsbs.Put(ctx, blk); err != nil {
			return err
		}
	}
	return nil
}

func (lsbs *LinkSystemBlockstore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	return nil, errors.New("not supported")
}

func (lsbs *LinkSystemBlockstore) HashOnRead(enabled bool) {
	lsbs.lsys.TrustedStorage = !enabled
}
