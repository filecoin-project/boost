package couchbase

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/boostd-data/model"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	carindex "github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("boostd-data-cb")

type Store struct {
	db         *DB
	pieceLocks [1024]sync.RWMutex
}

func NewStore(ctx context.Context) (*Store, error) {
	db, err := newDB(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating new couchbase store: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	log.Debugw("handle.add-deal-for-piece", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.add-deal-for-piece", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return s.db.AddDealForPiece(ctx, pieceCid, dealInfo)
}

func (s *Store) GetOffset(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (uint64, error) {
	log.Debugw("handle.get-offset", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-offset", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return s.db.GetOffset(ctx, pieceCid, hash)
}

func (s *Store) GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error) {
	log.Debugw("handle.get-piece-deals", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-piece-deals", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting piece deals for piece %s: %w", pieceCid, err)
	}

	return md.Deals, nil
}

// Get all pieces that contain a multihash (used when retrieving by payload CID)
func (s *Store) PiecesContainingMultihash(ctx context.Context, m mh.Multihash) ([]cid.Cid, error) {
	log.Debugw("handle.pieces-containing-mh", "mh", m)

	defer func(now time.Time) {
		log.Debugw("handled.pieces-containing-mh", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return s.db.GetPieceCidsByMultihash(ctx, m)
}

// TODO: Why do we have both GetRecords and GetIndex?
func (s *Store) GetRecords(ctx context.Context, pieceCid cid.Cid) ([]model.Record, error) {
	log.Debugw("handle.get-iterable-index", "piece-cid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.get-iterable-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.pieceLocks[toStripedLockIndex(pieceCid)].RLock()
	defer s.pieceLocks[toStripedLockIndex(pieceCid)].RUnlock()

	records, err := s.db.AllRecords(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	return records, nil
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) ([]model.Record, error) {
	log.Warnw("handle.get-index", "pieceCid", pieceCid)

	defer func(now time.Time) {
		log.Warnw("handled.get-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.pieceLocks[toStripedLockIndex(pieceCid)].RLock()
	defer s.pieceLocks[toStripedLockIndex(pieceCid)].RUnlock()

	records, err := s.db.AllRecords(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	log.Warnw("handle.get-index.records", "len(records)", len(records))

	return records, nil
}

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record) error {
	log.Debugw("handle.add-index", "records", len(records))

	start := time.Now()
	defer func() { log.Debugw("handled.add-index", "took", time.Since(start).String()) }()

	s.pieceLocks[toStripedLockIndex(pieceCid)].Lock()
	defer s.pieceLocks[toStripedLockIndex(pieceCid)].Unlock()

	var recs []carindex.Record
	for _, r := range records {
		recs = append(recs, carindex.Record{
			Cid:    r.Cid,
			Offset: r.Offset,
		})
	}

	setMhStart := time.Now()
	err := s.db.SetMultihashesToPieceCid(ctx, recs, pieceCid)
	if err != nil {
		return fmt.Errorf("failed to add entry from mh to pieceCid: %w", err)
	}
	log.Debugw("handled.add-index SetMultihashesToPieceCid", "took", time.Since(setMhStart).String())

	mis := make(index.MultihashIndexSorted)
	err = mis.Load(recs)
	if err != nil {
		return err
	}

	var subject index.Index
	subject = &mis

	// process index and store entries
	addOffsetsStart := time.Now()
	idx, ok := subject.(index.IterableIndex)
	if !ok {
		return errors.New(fmt.Sprintf("wanted %v but got %v\n", multicodec.CarMultihashIndexSorted, idx.Codec()))
	}
	if err := s.db.AddOffsets(ctx, pieceCid, idx); err != nil {
		return err
	}
	log.Debugw("handled.add-index AddOffsets", "took", time.Since(addOffsetsStart).String())

	// mark that indexing is complete
	md := model.Metadata{
		IndexedAt: time.Now(),
		Deals:     []model.DealInfo{},
	}

	err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) IndexedAt(ctx context.Context, pieceCid cid.Cid) (time.Time, error) {
	log.Debugw("handle.indexed-at", "pieceCid", pieceCid)

	defer func(now time.Time) {
		log.Debugw("handled.indexed-at", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil && !isNotFoundErr(err) {
		return time.Time{}, err
	}

	return md.IndexedAt, nil
}

func toStripedLockIndex(pieceCid cid.Cid) byte {
	bz := pieceCid.Bytes()
	return bz[len(bz)-1]
}
