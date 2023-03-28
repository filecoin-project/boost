package ldb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/filecoin-project/boost/cmd/boostd-data/model"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("boostd-data-ldb")

type Store struct {
	sync.Mutex
	db *DB
}

func NewStore(_repopath string) *Store {
	// tests
	repopath := _repopath
	if _repopath == "" {
		var err error
		repopath, err = os.MkdirTemp("", "ds-leveldb")
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

	return &Store{
		db: db,
	}
}

func (s *Store) AddDealForPiece(pieceCid cid.Cid, dealInfo model.DealInfo) error {
	log.Debugw("handle.add-deal-for-piece", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.add-deal-for-piece", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	ctx := context.Background()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return err
	}

	md.Deals = append(md.Deals, dealInfo)

	err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) GetRecords(pieceCid cid.Cid) ([]model.Record, error) {
	log.Debugw("handle.get-iterable-index", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-iterable-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	ctx := context.Background()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	records, err := s.db.AllRecords(ctx, md.Cursor)
	if err != nil {
		return nil, err
	}

	return records, nil
}

func (s *Store) GetOffset(pieceCid cid.Cid, hash mh.Multihash) (uint64, error) {
	log.Debugw("handle.get-offset", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-offset", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	ctx := context.Background()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return 0, err
	}

	return s.db.GetOffset(ctx, fmt.Sprintf("%d", md.Cursor)+"/", hash)
}

func (s *Store) GetPieceDeals(pieceCid cid.Cid) ([]model.DealInfo, error) {
	log.Debugw("handle.get-piece-deals", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-piece-deals", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	ctx := context.Background()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	return md.Deals, nil
}

// Get all pieces that contain a multihash (used when retrieving by payload CID)
func (s *Store) PiecesContainingMultihash(m mh.Multihash) ([]cid.Cid, error) {
	log.Debugw("handle.pieces-containing-mh", "mh", m)

	defer func(now time.Time) {
		log.Debugw("handled.pieces-containing-mh", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	ctx := context.Background()
	return s.db.GetPieceCidsByMultihash(ctx, m)
}

func (s *Store) GetIndex(pieceCid cid.Cid) ([]model.Record, error) {
	log.Warnw("handle.get-index", "pieceCid", pieceCid)

	defer func(now time.Time) {
		log.Warnw("handled.get-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	ctx := context.Background()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	records, err := s.db.AllRecords(ctx, md.Cursor)
	if err != nil {
		return nil, err
	}

	log.Warnw("handle.get-index.records", "len(records)", len(records))

	return records, nil
}

func (s *Store) AddIndex(pieceCid cid.Cid, records []model.Record) error {
	log.Debugw("handle.add-index", "records", len(records))

	defer func(now time.Time) {
		log.Debugw("handled.add-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	ctx := context.Background()

	var recs []carindex.Record
	for _, r := range records {
		recs = append(recs, carindex.Record{
			Cid:    r.Cid,
			Offset: r.Offset,
		})
	}

	err := s.db.SetMultihashesToPieceCid(ctx, recs, pieceCid)
	if err != nil {
		return fmt.Errorf("failed to add entry from mh to pieceCid: %w", err)
	}

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

	// mark that indexing is complete
	md := model.Metadata{
		Cursor:    cursor,
		IndexedAt: time.Now(),
	}

	err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md)
	if err != nil {
		return err
	}

	err = s.db.Sync(ctx, datastore.NewKey(fmt.Sprintf("%d", cursor)))
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) IndexedAt(pieceCid cid.Cid) (time.Time, error) {
	log.Debugw("handle.indexed-at", "pieceCid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.indexed-at", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	ctx := context.Background()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil && err != ds.ErrNotFound {
		return time.Time{}, err
	}

	return md.IndexedAt, nil
}
