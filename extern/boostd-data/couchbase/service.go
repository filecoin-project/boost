package couchbase

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
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

	ctx, span := tracing.Tracer.Start(ctx, "store.add_deal_for_piece")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.add-deal-for-piece", "took", time.Since(now).String())
	}(time.Now())

	return s.db.AddDealForPiece(ctx, pieceCid, dealInfo)
}

func (s *Store) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	log.Debugw("handle.get-offset-size", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_offset_size")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.get-offset-size", "took", time.Since(now).String())
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
		log.Debugw("handled.get-piece-metadata", "took", time.Since(now).String())
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
		log.Debugw("handled.get-piece-deals", "took", time.Since(now).String())
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
		log.Debugw("handled.pieces-containing-mh", "took", time.Since(now).String())
	}(time.Now())

	pcids, err := s.db.GetPieceCidsByMultihash(ctx, m)
	return pcids, normalizeMultihashError(m, err)
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) (<-chan types.IndexRecord, error) {
	log.Debugw("handle.get-index", "pieceCid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_index")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.get-index", "took", time.Since(now).String())
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

	recs := make(chan types.IndexRecord, len(records))
	for _, r := range records {
		recs <- types.IndexRecord{Record: r}
	}
	close(recs)

	return recs, nil
}

func (s *Store) IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	t, err := s.IndexedAt(ctx, pieceCid)
	if err != nil {
		return false, err
	}
	return !t.IsZero(), nil
}

func (s *Store) IsCompleteIndex(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	log.Debugw("handle.is-complete-index", "pieceCid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.is_incomplete_index")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.is-complete-index", "took", time.Since(now).String())
	}(time.Now())

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return false, normalizePieceCidError(pieceCid, err)
	}

	return md.CompleteIndex, nil
}

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record, isCompleteIndex bool) <-chan types.AddIndexProgress {
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

	progress := make(chan types.AddIndexProgress, 1)
	go func() {
		defer close(progress)
		progress <- types.AddIndexProgress{Progress: 0}

		setMhStart := time.Now()
		err := s.db.SetMultihashesToPieceCid(ctx, mhs, pieceCid)
		if err != nil {
			progress <- types.AddIndexProgress{Err: err.Error()}
			return
		}
		log.Debugw("handled.add-index SetMultihashesToPieceCid", "took", time.Since(setMhStart).String())
		progress <- types.AddIndexProgress{Progress: 0.5}

		// Add a mapping from piece cid -> offset / size of each block so that
		// clients can get the block info for all blocks in a piece
		addOffsetsStart := time.Now()
		if err := s.db.AddIndexRecords(ctx, pieceCid, records); err != nil {
			progress <- types.AddIndexProgress{Err: err.Error()}
			return
		}
		log.Debugw("handled.add-index AddIndexRecords", "took", time.Since(addOffsetsStart).String())
		progress <- types.AddIndexProgress{Progress: 1}
	}()

	return progress
}

func (s *Store) IndexedAt(ctx context.Context, pieceCid cid.Cid) (time.Time, error) {
	log.Debugw("handle.indexed-at", "pieceCid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.indexed_at")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.indexed-at", "took", time.Since(now).String())
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
		log.Debugw("handled.list-pieces", "took", time.Since(now).String())
	}(time.Now())

	return s.db.ListPieces(ctx)
}

func (s *Store) NextPiecesToCheck(ctx context.Context) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.next_pieces_to_check")
	defer span.End()

	return s.db.NextPiecesToCheck(ctx)
}

func (s *Store) FlagPiece(ctx context.Context, pieceCid cid.Cid, _ bool) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.flag_piece")
	span.SetAttributes(attribute.String("pieceCid", pieceCid.String()))
	defer span.End()

	return s.db.FlagPiece(ctx, pieceCid)
}

func (s *Store) UnflagPiece(ctx context.Context, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.unflag_piece")
	span.SetAttributes(attribute.String("pieceCid", pieceCid.String()))
	defer span.End()

	return s.db.UnflagPiece(ctx, pieceCid)
}

func (s *Store) FlaggedPiecesList(ctx context.Context, cursor *time.Time, offset int, limit int) ([]model.FlaggedPiece, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.flagged_pieces")
	var spanCursor int
	if cursor != nil {
		spanCursor = int(cursor.UnixMilli())
	}
	span.SetAttributes(attribute.Int("cursor", spanCursor))
	span.SetAttributes(attribute.Int("offset", offset))
	span.SetAttributes(attribute.Int("limit", limit))
	defer span.End()

	return s.db.FlaggedPiecesList(ctx, cursor, offset, limit)
}

func (s *Store) FlaggedPiecesCount(ctx context.Context) (int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.flagged_pieces_count")
	defer span.End()

	return s.db.FlaggedPiecesCount(ctx)
}

// RemoveDealForPiece removes Single deal for pieceCID. If []Deals is empty then Metadata is removed as well
func (s *Store) RemoveDealForPiece(ctx context.Context, pieceCid cid.Cid, dealId string) error {
	log.Debugw("handle.remove-deal-for-piece", "piece-cid", pieceCid, "deal-uuid", dealId)

	ctx, span := tracing.Tracer.Start(ctx, "store.remove_deal_for_piece")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.remove-deal-for-piece", "took", time.Since(now).String())
	}(time.Now())

	return s.db.RemoveDealForPiece(ctx, dealId, pieceCid)
}

// RemovePieceMetadata removes all Metadata for pieceCID. To be used manually in case of failure
// in RemoveDealForPiece
func (s *Store) RemovePieceMetadata(ctx context.Context, pieceCid cid.Cid) error {
	log.Debugw("handle.remove-piece-metadata", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.remove_piece_metadata")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.remove-piece-metadata", "took", time.Since(now).String())
	}(time.Now())

	if err := s.db.RemovePieceMetadata(ctx, pieceCid); err != nil {
		return err
	}

	return nil
}

// RemoveIndexes removes all MultiHashes for pieceCID. To be used manually in case of failure
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
