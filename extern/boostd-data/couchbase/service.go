package couchbase

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/boost/tracing"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("boostd-data-cb")

const stripedLockSize = 1024

type Store struct {
	settings   DBSettings
	db         *DB
	pieceLocks [stripedLockSize]sync.RWMutex
}

var _ types.ServiceImpl = (*Store)(nil)

func NewStore(settings DBSettings) *Store {
	return &Store{settings: settings}
}

func (s *Store) Start(ctx context.Context) error {
	db, err := newDB(ctx, s.settings)
	if err != nil {
		return fmt.Errorf("starting couchbase service: %w", err)
	}

	s.db = db
	return nil
}

func (s *Store) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	log.Debugw("handle.add-deal-for-piece", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(context.Background(), "store.add_deal_for_piece")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.add-deal-for-piece", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return s.db.AddDealForPiece(ctx, pieceCid, dealInfo)
}

func (s *Store) MarkIndexErrored(ctx context.Context, pieceCid cid.Cid, err error) error {
	log.Debugw("handle.mark-piece-index-errored", "piece-cid", pieceCid, "err", err)

	ctx, span := tracing.Tracer.Start(context.Background(), "store.mark-piece-index-errored")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.mark-piece-index-errored", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return s.db.MarkIndexErrored(ctx, pieceCid, err)
}

func (s *Store) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	log.Debugw("handle.get-offset-size", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_offset_size")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.get-offset-size", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return s.db.GetOffsetSize(ctx, pieceCid, hash)
}

func (s *Store) GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error) {
	log.Debugw("handle.get-piece-deals", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_piece_deals")
	defer span.End()

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

	ctx, span := tracing.Tracer.Start(ctx, "store.pieces_containing_multihash")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.pieces-containing-mh", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return s.db.GetPieceCidsByMultihash(ctx, m)
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) ([]model.Record, error) {
	log.Debugw("handle.get-index", "pieceCid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_index")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.get-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.pieceLocks[toStripedLockIndex(pieceCid)].RLock()
	defer s.pieceLocks[toStripedLockIndex(pieceCid)].RUnlock()

	records, err := s.db.AllRecords(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	log.Debugw("handle.get-index.records", "len(records)", len(records))

	return records, nil
}

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record) error {
	log.Debugw("handle.add-index", "records", len(records))

	ctx, span := tracing.Tracer.Start(ctx, "store.add_index")
	defer span.End()

	start := time.Now()
	defer func() { log.Debugw("handled.add-index", "took", time.Since(start).String()) }()

	s.pieceLocks[toStripedLockIndex(pieceCid)].Lock()
	defer s.pieceLocks[toStripedLockIndex(pieceCid)].Unlock()

	// Add a mapping from multihash -> piece cid so that clients can look up
	// which pieces contain a multihash
	mhs := make([]mh.Multihash, 0, len(records))
	for _, r := range records {
		mhs = append(mhs, r.Cid.Hash())
	}

	setMhStart := time.Now()
	err := s.db.SetMultihashesToPieceCid(ctx, mhs, pieceCid)
	if err != nil {
		return fmt.Errorf("failed to add entry from mh to pieceCid: %w", err)
	}
	log.Debugw("handled.add-index SetMultihashesToPieceCid", "took", time.Since(setMhStart).String())

	// Add a mapping from piece cid -> offset / size of each block so that
	// clients can get the block info for all blocks in a piece
	addOffsetsStart := time.Now()
	if err := s.db.AddIndexRecords(ctx, pieceCid, records); err != nil {
		return err
	}
	log.Debugw("handled.add-index AddIndexRecords", "took", time.Since(addOffsetsStart).String())

	// get the metadata for the piece
	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		if !isNotFoundErr(err) {
			return fmt.Errorf("getting piece cid metadata for piece %s: %w", pieceCid, err)
		}
		// there isn't yet any metadata, so create new metadata
		md = model.Metadata{}
	}

	// Mark indexing as complete
	md.IndexedAt = time.Now()
	if md.Deals == nil {
		md.Deals = []model.DealInfo{}
	}

	err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md)
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

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil && !isNotFoundErr(err) {
		return time.Time{}, err
	}

	return md.IndexedAt, nil
}

func toStripedLockIndex(pieceCid cid.Cid) uint16 {
	bz := pieceCid.Bytes()
	return binary.BigEndian.Uint16(bz[len(bz)-2:]) % stripedLockSize
}
