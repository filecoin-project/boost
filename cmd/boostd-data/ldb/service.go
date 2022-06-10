package ldb

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/filecoin-project/boost/cmd/boostd-data/model"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
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

func NewPieceMetaService(_repopath string) *PieceMetaService {
	// tests
	repopath := _repopath
	if _repopath == "" {
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

	// tests
	if _repopath == "" {
		// prepare db
		log.Debug("preparing db with next cursor")
		db.SetNextCursor(context.Background(), 100)
	}

	log.Debugw("new piece meta service", "repo path", repopath)

	return &PieceMetaService{
		db: db,
	}
}

func (s *PieceMetaService) AddDealForPiece(pieceCid cid.Cid, dealInfo model.DealInfo) error {
	log.Debugw("handle.add-deal-for-piece", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.add-deal-for-piece", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return nil
}

func (s *PieceMetaService) GetRecords(pieceCid cid.Cid) ([]carindex.Record, error) {
	log.Debugw("handle.get-iterable-index", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-iterable-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	ctx := context.Background()

	cursor, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
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

	cursor, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return 0, err
	}

	return s.db.GetOffset(ctx, fmt.Sprintf("%d", cursor)+"/", hash)
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

func (s *PieceMetaService) AddIndex(pieceCid cid.Cid, records []model.Record) error {
	log.Debugw("handle.add-index", "records", len(records))

	defer func(now time.Time) {
		log.Debugw("handled.add-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	var recs []carindex.Record
	for _, r := range records {
		recs = append(recs, carindex.Record{
			Cid:    r.Cid,
			Offset: r.Offset,
		})
		//fmt.Println("got r: ", r.Cid, r.Offset)
	}
	// --- first  ---:

	// TODO first: see inverted index in dagstore today

	// --- second ---:

	ctx := context.Background()

	// get and set next cursor (handle synchronization, maybe with CAS)
	cursor, keyCursorPrefix, err := s.db.NextCursor(ctx)
	if err != nil {
		return fmt.Errorf("couldnt generate next cursor: %w", err)
	}

	// alloacte metadata for pieceCid
	err = s.db.SetNextCursor(ctx, cursor+1)
	if err != nil {
		return err
	}

	mis := make(index.MultihashIndexSorted)
	err = mis.Load(recs)
	if err != nil {
		return err
	}

	var subject index.Index
	subject = &mis

	// process index and store entries
	switch idx := subject.(type) {
	case index.IterableIndex:
		err := idx.ForEach(func(m multihash.Multihash, offset uint64) error {

			return s.db.AddOffset(ctx, keyCursorPrefix, m, offset)
		})
		if err != nil {
			return err
		}

	default:
		return errors.New(fmt.Sprintf("wanted %v but got %v\n", multicodec.CarMultihashIndexSorted, idx.Codec()))
	}

	// TODO: mark that indexing is complete ; metadata value for each piece
	// pieceCid -> {cursor ; isIndexed ; []dealInfo }
	// put pieceCid in pieceCid->cursor table
	// right now we store just cursor
	err = s.db.SetPieceCidToMetadata(ctx, pieceCid, cursor)
	if err != nil {
		return err
	}

	err = s.db.Sync(ctx, datastore.NewKey(keyCursorPrefix))
	if err != nil {
		return err
	}

	return nil
}
