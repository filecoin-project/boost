//go:build !nofoundationdb

package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
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
		db, err := NewFoundationDB()
		if err != nil {
			return err
		}
		return run(ctx, db, runOptsFromCctx(cctx))
	},
}

const (
	MHToPiecePrefix = "mp"
	PieceToMHIndex  = "pm"
)

type FoundationDB struct {
	db fdb.Database
}

func NewFoundationDB() (*FoundationDB, error) {
	fdb.MustAPIVersion(630)
	db, err := fdb.OpenDefault()
	if err != nil {
		return nil, err
	}

	return &FoundationDB{db: db}, nil
}

func (db *FoundationDB) Name() string {
	return "Foundation DB"
}

func (db *FoundationDB) Init(ctx context.Context) error {
	// kv store, so nothing to do
	return nil
}

func (db *FoundationDB) Cleanup(ctx context.Context) error {
	_, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		start, end := tuple.Tuple{MHToPiecePrefix}.FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})

		start, end = tuple.Tuple{PieceToMHIndex}.FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: start, End: end})

		return nil, nil
	})

	return err
}

func (db *FoundationDB) GetBlockSample(ctx context.Context, count int) ([]pieceBlock, error) {
	var blocks []pieceBlock

	_, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for i := 0; i < count; i++ {
			// todo smarter sampling? There is a multihash prefix here...
			randomPrefix := make([]byte, 4)
			_, _ = rand.Read(randomPrefix)

			start := tuple.Tuple{PieceToMHIndex, randomPrefix}.FDBKey()
			end := tuple.Tuple{PieceToMHIndex, []byte{0xff}}.FDBKey() // todo does this work?
			rangeResult, err := tr.GetRange(fdb.KeyRange{Begin: start, End: end}, fdb.RangeOptions{}).GetSliceWithError()
			if err != nil {
				return nil, err
			}

			if len(rangeResult) == 0 {
				continue
			}

			kv := rangeResult[0]

			keyTuple, err := tuple.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}

			pieceCid, err := cid.Parse(keyTuple[1])
			if err != nil {
				return nil, err
			}

			payloadMH, err := multihash.Cast(keyTuple[2].([]byte))
			if err != nil {
				return nil, err
			}

			blocks = append(blocks, pieceBlock{
				PieceCid:         pieceCid,
				PayloadMultihash: payloadMH,
			})
		}

		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	return blocks, nil
}

func (db *FoundationDB) AddIndexRecords(ctx context.Context, pieceCid cid.Cid, recs []model.Record) error { // good
	if len(recs) == 0 {
		return nil
	}

	_, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		pieceCidBytes := pieceCid.Bytes()

		for _, rec := range recs {
			mhashBytes := []byte(rec.Cid.Hash())

			offsetSizeBytes := make([]byte, 16)
			binary.BigEndian.PutUint64(offsetSizeBytes, rec.Offset)
			binary.BigEndian.PutUint64(offsetSizeBytes[8:], rec.Size)

			// Add payload to pieces index

			// note: we could also hold all piece cids under a single key, and update with the atomic AddIfFits operation
			// but that is more complicated as we need to make sure the value doesn't get too large. Also makes deletes slower.
			payloadToPiecesKey := tuple.Tuple{MHToPiecePrefix, mhashBytes, pieceCidBytes}.FDBKey()
			tr.Set(payloadToPiecesKey, []byte{})

			// Add piece to block info index
			pieceBlockKey := tuple.Tuple{PieceToMHIndex, pieceCidBytes, mhashBytes}.FDBKey()
			tr.Set(pieceBlockKey, offsetSizeBytes)
		}

		return nil, nil
	})

	return err
}

func (db *FoundationDB) PiecesContainingMultihash(ctx context.Context, m multihash.Multihash) ([]cid.Cid, error) { // good
	var pieceCids []cid.Cid

	_, err := db.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		start, end := tuple.Tuple{MHToPiecePrefix, []byte(m)}.FDBRangeKeys()
		rangeResult, err := tr.GetRange(fdb.KeyRange{Begin: start, End: end}, fdb.RangeOptions{}).GetSliceWithError() // todo iterator?
		if err != nil {
			return nil, err
		}

		for _, kv := range rangeResult {
			tp, err := tuple.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}
			pieceCid, err := cid.Parse(tp[2])
			if err != nil {
				return nil, err
			}
			pieceCids = append(pieceCids, pieceCid)
		}
		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	return pieceCids, nil
}

func (db *FoundationDB) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash multihash.Multihash) (*model.OffsetSize, error) {
	var offsetSize *model.OffsetSize

	_, err := db.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		pieceBlockKey := tuple.Tuple{PieceToMHIndex, pieceCid.Bytes(), []byte(hash)}.Pack()
		value, err := tr.Get(fdb.Key(pieceBlockKey)).Get()
		if err != nil {
			return nil, err
		}
		offset := binary.BigEndian.Uint64(value[:8])
		size := binary.BigEndian.Uint64(value[8:])

		offsetSize = &model.OffsetSize{Offset: offset, Size: size}
		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	return offsetSize, nil
}

func (db *FoundationDB) GetIterableIndex(ctx context.Context, pieceCid cid.Cid) (index.IterableIndex, error) {
	var ents []index.Record

	_, err := db.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		start, end := tuple.Tuple{PieceToMHIndex, pieceCid.Bytes()}.FDBRangeKeys()

		for rangeIter := tr.GetRange(fdb.KeyRange{Begin: start, End: end}, fdb.RangeOptions{}).Iterator(); rangeIter.Advance(); {
			kv, err := rangeIter.Get()
			if err != nil {
				return nil, err
			}

			keyTuple, err := tuple.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}

			payloadMH, err := multihash.Cast(keyTuple[2].([]byte))
			if err != nil {
				return nil, err
			}

			offsetSizeBytes := kv.Value
			offset := binary.BigEndian.Uint64(offsetSizeBytes)

			ents = append(ents, index.Record{
				Cid:    cid.NewCidV1(cid.Raw, payloadMH),
				Offset: offset,
			})
		}

		return nil, nil
	})

	if err != nil {
		return nil, err
	}

	mis := make(index.MultihashIndexSorted)
	err = mis.Load(ents)
	if err != nil {
		return nil, err
	}

	return &mis, nil
}
