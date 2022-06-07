package ldb

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/filecoin-project/boost/cmd/boostd-data/model"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	carindex "github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
	"github.com/syndtr/goleveldb/leveldb/opt"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
)

var log = logging.Logger("boostd-data-ldb")

type PieceMetaService struct {
	db *DB
}

type DB struct {
	datastore.Batching
}

func newDB(path string, readonly bool) (*DB, error) {
	ldb, err := levelds.NewDatastore(path, &levelds.Options{
		Compression:         ldbopts.SnappyCompression,
		NoSync:              true,
		Strict:              ldbopts.StrictAll,
		ReadOnly:            readonly,
		CompactionTableSize: 4 * opt.MiB,
	})
	if err != nil {
		return nil, err
	}

	return &DB{ldb}, nil
}

func NewPieceMetaService(repopath string) *PieceMetaService {
	if repopath == "" {
		var err error
		repopath, err = ioutil.TempDir("", "ds-leveldb")
		if err != nil {
			panic(err)
		}
	}

	db, err := newDB(repopath, false)
	if err != nil {
		panic(err)
	}

	log.Debugw("datastore.service", "repo path", repopath)

	return &PieceMetaService{
		db: db,
	}
}

func (s *PieceMetaService) AddDealForPiece(pieceCid cid.Cid, dealInfo model.DealInfo) error {
	log.Debugw("handle.add-deal-for-piece", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.add-deal-for-piece", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	panic("not implemented")

	return nil
}

// TODO: maybe implement over rpc subscription
// TODO: maybe pass ctx in func signature
func (s *PieceMetaService) GetIterableIndex(pieceCid cid.Cid) ([]carindex.Record, error) {
	log.Debugw("handle.get-iterable-index", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-iterable-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	ctx := context.Background()

	cursor, err := s.db.GetPieceCidToCursor(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	records, err := s.db.AllRecords(ctx, cursor)
	if err != nil {
		return nil, err
	}

	return records, nil
}

func (s *PieceMetaService) GetOffset(pieceCid cid.Cid, hash mh.Multihash) (uint64, error) {
	log.Debugw("handle.get-offset", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-offset", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	ctx := context.Background()

	cursor, err := s.db.GetPieceCidToCursor(ctx, pieceCid)
	if err != nil {
		return 0, err
	}

	return s.db.GetOffset(ctx, string(cursor)+"/", hash)
}

func (s *PieceMetaService) GetPieceDeals(pieceCid cid.Cid) ([]model.DealInfo, error) {
	log.Debugw("handle.get-piece-deals", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-piece-deals", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return nil, nil
}

// Get all pieces that contain a multihash (used when retrieving by payload CID)
func (s *PieceMetaService) PiecesContainingMultihash(m mh.Multihash) ([]cid.Cid, error) {
	log.Debugw("handle.pieces-containing-mh", "mh", m)

	defer func(now time.Time) {
		log.Debugw("handled.pieces-containing-mh", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return nil, nil
}
