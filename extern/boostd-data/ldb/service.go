package ldb

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/filecoin-project/boost/tracing"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	carindex "github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("boostd-data-ldb")

type Store struct {
	sync.Mutex
	db       *DB
	repopath string
}

var _ types.ServiceImpl = (*Store)(nil)

func NewStore(repopath string) *Store {
	return &Store{repopath: repopath}
}

func (s *Store) Start(ctx context.Context) error {
	repopath := s.repopath
	if repopath == "" {
		// used by tests
		var err error
		repopath, err = ioutil.TempDir("", "ds-leveldb")
		if err != nil {
			return fmt.Errorf("creating leveldb tmp dir: %w", err)
		}
	}

	var err error
	s.db, err = newDB(repopath, false)
	if err != nil {
		return err
	}

	// Prepare db with a cursor
	log.Debug("preparing db with next cursor")
	s.db.SetNextCursor(ctx, 100)

	log.Debugw("new leveldb piece directory service", "repo path", repopath)
	return nil
}

func (s *Store) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	log.Debugw("handle.add-deal-for-piece", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.add_deal_for_piece")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.add-deal-for-piece", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

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

func (s *Store) GetRecords(ctx context.Context, pieceCid cid.Cid) ([]model.Record, error) {
	log.Debugw("handle.get-iterable-index", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_records")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.get-iterable-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

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

func (s *Store) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	log.Debugw("handle.get-offset-size", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_offset_size")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.get-offset-size", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	return s.db.GetOffsetSize(ctx, fmt.Sprintf("%d", md.Cursor)+"/", hash)
}

func (s *Store) GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error) {
	log.Debugw("handle.get-piece-deals", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_piece_deals")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.get-piece-deals", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	return md.Deals, nil
}

// Get all pieces that contain a multihash (used when retrieving by payload CID)
func (s *Store) PiecesContainingMultihash(ctx context.Context, m mh.Multihash) ([]cid.Cid, error) {
	log.Debugw("handle.pieces-containing-mh", "mh", m)

	ctx, span := tracing.Tracer.Start(ctx, "store.pieces_containing_multihash")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.pieces-containing-mh", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	return s.db.GetPieceCidsByMultihash(ctx, m)
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) ([]model.Record, error) {
	log.Warnw("handle.get-index", "pieceCid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_index")
	defer span.End()

	defer func(now time.Time) {
		log.Warnw("handled.get-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

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

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record) error {
	log.Debugw("handle.add-index", "records", len(records))

	ctx, span := tracing.Tracer.Start(ctx, "store.add_index")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.add-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

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

	// allocate metadata for pieceCid
	err = s.db.SetNextCursor(ctx, cursor+1)
	if err != nil {
		return err
	}

	// process index and store entries
	for _, rec := range records {
		err := s.db.AddIndexRecord(ctx, keyCursorPrefix, rec)
		if err != nil {
			return err
		}
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

func (s *Store) IndexedAt(ctx context.Context, pieceCid cid.Cid) (time.Time, error) {
	log.Debugw("handle.indexed-at", "pieceCid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.indexed_at")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.indexed-at", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil && !errors.Is(err, ds.ErrNotFound) {
		return time.Time{}, err
	}

	return md.IndexedAt, nil
}
