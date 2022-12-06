package couchbase

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/tracing"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("boostd-data-cb")

type CouchbaseMetadata struct {
	model.Metadata
	BlockCount int `json:"b"`
}

type Store struct {
	settings DBSettings
	db       *DB
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

func (s *Store) SetCarSize(ctx context.Context, pieceCid cid.Cid, size uint64) error {
	log.Debugw("handle.set-car-size", "piece-cid", pieceCid, "size", size)

	ctx, span := tracing.Tracer.Start(context.Background(), "store.set-car-size")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.set-car-size", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	err := s.db.SetCarSize(ctx, pieceCid, size)
	return normalizePieceCidError(pieceCid, err)
}

func (s *Store) MarkIndexErrored(ctx context.Context, pieceCid cid.Cid, err error) error {
	log.Debugw("handle.mark-piece-index-errored", "piece-cid", pieceCid, "err", err)

	ctx, span := tracing.Tracer.Start(ctx, "store.mark-piece-index-errored")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.mark-piece-index-errored", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	err = s.db.MarkIndexErrored(ctx, pieceCid, err)
	return normalizePieceCidError(pieceCid, err)
}

func (s *Store) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	log.Debugw("handle.get-offset-size", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_offset_size")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.get-offset-size", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		err = normalizePieceCidError(pieceCid, err)
		return nil, fmt.Errorf("getting piece metadata for piece %s: %w", pieceCid, err)
	}

	ofsz, err := s.db.GetOffsetSize(ctx, pieceCid, hash, md.BlockCount)
	return ofsz, normalizePieceCidError(pieceCid, err)
}

func (s *Store) GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error) {
	log.Debugw("handle.get-piece-metadata", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_piece_metadata")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.get-piece-metadata", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		err = normalizePieceCidError(pieceCid, err)
		return model.Metadata{}, fmt.Errorf("getting piece metadata for piece %s: %w", pieceCid, err)
	}

	return md.Metadata, nil
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
		err = normalizePieceCidError(pieceCid, err)
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

	pcids, err := s.db.GetPieceCidsByMultihash(ctx, m)
	return pcids, normalizeMultihashError(m, err)
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) ([]model.Record, error) {
	log.Debugw("handle.get-index", "pieceCid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_index")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.get-index", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		err = normalizePieceCidError(pieceCid, err)
		return nil, fmt.Errorf("getting piece cid %s metadata: %w", pieceCid, err)
	}

	records, err := s.db.AllRecords(ctx, pieceCid, md.BlockCount)
	if err != nil {
		err = normalizePieceCidError(pieceCid, err)
		return nil, fmt.Errorf("getting all records for piece %s: %w", pieceCid, err)
	}

	log.Debugw("handle.get-index.records", "len(records)", len(records))

	return records, nil
}

func (s *Store) IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	t, err := s.IndexedAt(ctx, pieceCid)
	if err != nil {
		return false, err
	}
	return !t.IsZero(), nil
}

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record) error {
	log.Debugw("handle.add-index", "records", len(records))

	ctx, span := tracing.Tracer.Start(ctx, "store.add_index")
	defer span.End()

	start := time.Now()
	defer func() { log.Debugw("handled.add-index", "took", time.Since(start).String()) }()

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

	return s.db.MarkIndexingComplete(ctx, pieceCid, len(records))
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

func (s *Store) ListPieces(ctx context.Context) ([]cid.Cid, error) {
	log.Debugw("handle.list-pieces")

	ctx, span := tracing.Tracer.Start(ctx, "store.list_pieces")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.list-pieces", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	return s.db.ListPieces(ctx)
}

func normalizePieceCidError(pieceCid cid.Cid, err error) error {
	if err == nil {
		return nil
	}
	if isNotFoundErr(err) {
		return fmt.Errorf("piece %s: %s", pieceCid, types.ErrNotFound)
	}
	return err
}

func normalizeMultihashError(m mh.Multihash, err error) error {
	if err == nil {
		return nil
	}
	if isNotFoundErr(err) {
		return fmt.Errorf("multihash %s: %s", m, types.ErrNotFound)
	}
	return err
}

// Remove Single deal for pieceCID. If []Deals is empty then Metadata is removed as well
func (s *Store) RemoveDealForPiece(ctx context.Context, pieceCid cid.Cid, dealId string) error {
	log.Debugw("handle.remove-deal-for-piece", "piece-cid", pieceCid, "deal-uuid", dealId)

	ctx, span := tracing.Tracer.Start(ctx, "store.remove_deal_for_piece")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.remove-deal-for-piece", "took", time.Since(now).String())
	}(time.Now())

	return s.db.RemoveDeals(ctx, dealId, pieceCid)
}

// Remove all Metadata for pieceCID. To be used manually in case of failure
// in RemoveDealForPiece
func (s *Store) RemovePieceMetadata(ctx context.Context, pieceCid cid.Cid) error {
	log.Debugw("handle.remove-piece-metadata", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.remove_piece_metadata")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.remove-piece-metadata", "took", time.Since(now).String())
	}(time.Now())

	if err := s.db.RemoveMetadata(ctx, pieceCid); err != nil {
		return err
	}

	return nil
}

// Removes all MultiHashes for pieceCID. To be used manually in case of failure
// in RemoveDealForPiece or RemovePieceMetadata. Metadata for the piece must be
// present in the database
func (s *Store) RemoveIndexes(ctx context.Context, pieceCid cid.Cid) error {
	log.Debugw("handle.remove-indexes", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.remove_indexes")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handle.remove-indexes", "took", time.Since(now).String())
	}(time.Now())

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return err
	}

	if err := s.db.RemoveIndexes(ctx, pieceCid, md.BlockCount); err != nil {
		return err
	}

	return nil
}
