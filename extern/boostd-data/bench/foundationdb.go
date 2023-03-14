package main

import (
	"context"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/shared/cliutil"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

var foundationCmd = &cli.Command{
	Name:   "foundation",
	Before: before,
	Flags:  commonFlags,
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)
		db := NewFoundationDB()
		return run(ctx, db, runOptsFromCctx(cctx))
	},
}

type FoundationDB struct {
	//db fdb.Database
}

func NewFoundationDB() *FoundationDB {
	//fdb.MustAPIVersion(720)
	//db := fdb.MustOpenDefault()
	//return &FoundationDB{db: db}
	return nil
}

func (db *FoundationDB) Name() string {
	return "Foundation DB"
}

func (db *FoundationDB) Init(ctx context.Context) error {
	//ret, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
	//	tr.Set(fdb.Key("hello"), []byte("world"))
	//	return tr.Get(fdb.Key("foo")).MustGet(), nil
	//})
	//if err != nil {
	//	return err
	//}
	//
	//fmt.Printf("hello is now world, foo was: %s\n", string(ret.([]byte)))
	//
	return nil
}

func (db *FoundationDB) Cleanup(ctx context.Context) error {
	return nil
}

func (db *FoundationDB) GetBlockSample(ctx context.Context, count int) ([]pieceBlock, error) {
	//TODO implement me
	panic("implement me")
}

func (db *FoundationDB) AddIndexRecords(ctx context.Context, pieceCid cid.Cid, recs []model.Record) error {
	return nil
}

func (db *FoundationDB) PiecesContainingMultihash(ctx context.Context, m multihash.Multihash) ([]cid.Cid, error) {
	//TODO implement me
	panic("implement me")
}

func (db *FoundationDB) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash multihash.Multihash) (*model.OffsetSize, error) {
	//TODO implement me
	panic("implement me")
}

func (db *FoundationDB) GetIterableIndex(ctx context.Context, pieceCid cid.Cid) (index.IterableIndex, error) {
	//TODO implement me
	panic("implement me")
}
