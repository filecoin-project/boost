package ldb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/metrics"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/tracing"
	"github.com/filecoin-project/boost/extern/boostd-data/svc/types"
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	carindex "github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

// The current piece metadata version. This version will be used when doing
// data migrations (migrations are not yet implemented in version 1).
const pieceMetadataVersion = "1"

var log = logging.Logger("boostd-data-ldb")

type LeveldbFlaggedMetadata struct {
	CreatedAt       time.Time       `json:"c"`
	UpdatedAt       time.Time       `json:"u"`
	HasUnsealedCopy bool            `json:"huc"`
	MinerAddr       address.Address `json:"m"`
}

type LeveldbMetadata struct {
	model.Metadata
	Cursor uint64 `json:"c"`
}

func newLeveldbMetadata() LeveldbMetadata {
	return LeveldbMetadata{
		Metadata: model.Metadata{Version: pieceMetadataVersion},
	}
}

type Store struct {
	sync.Mutex
	db       *DB
	repopath string
	ctx      context.Context
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
		repopath, err = os.MkdirTemp("", "ds-leveldb")
		if err != nil {
			return fmt.Errorf("creating leveldb tmp dir: %w", err)
		}
	}

	var err error
	s.db, err = newDB(s.repopath, false)
	if err != nil {
		return err
	}

	// Prepare db with a cursor
	err = s.db.InitCursor(ctx)
	if err != nil {
		return err
	}

	s.ctx = ctx

	log.Debugw("new leveldb local index directory service", "repo path", repopath)
	return nil
}

func (s *Store) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	log.Debugw("handle.add-deal-for-piece", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.add_deal_for_piece")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.add_deal_for_piece"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.add-deal-for-piece", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	// Get the existing deals for the piece
	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			stats.Record(s.ctx, metrics.FailureAddDealForPieceCount.M(1))
			return fmt.Errorf("getting piece cid metadata for piece %s: %w", pieceCid, err)
		}
		// there isn't yet any metadata, so create new metadata
		md = newLeveldbMetadata()
	}

	// Check if the deal has already been added
	for _, dl := range md.Deals {
		if dl == dealInfo {
			stats.Record(s.ctx, metrics.SuccessAddDealForPieceCount.M(1))
			return nil
		}
	}

	// Add the deal to the list
	md.Deals = append(md.Deals, dealInfo)

	// Write the piece metadata back to the db
	err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureAddDealForPieceCount.M(1))
		return err
	}

	stats.Record(s.ctx, metrics.SuccessAddDealForPieceCount.M(1))
	return nil
}

func (s *Store) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	log.Debugw("handle.get-offset-size", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_offset_size")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.get_offset_size"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.get-offset-size", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureGetOffsetSizeCount.M(1))
		return nil, normalizePieceCidError(pieceCid, err)
	}

	out, err := s.db.GetOffsetSize(ctx, fmt.Sprintf("%d", md.Cursor)+"/", hash)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureGetOffsetSizeCount.M(1))
	} else {
		stats.Record(s.ctx, metrics.SuccessGetOffsetSizeCount.M(1))
	}
	return out, err
}

func (s *Store) GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error) {
	log.Debugw("handle.get-piece-metadata", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_piece_metadata")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.get_piece_metadata"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.get-piece-metadata", "took", time.Since(now).String())
	}(time.Now())

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		err = normalizePieceCidError(pieceCid, err)
		stats.Record(s.ctx, metrics.FailureGetPieceMetadataCount.M(1))
		return model.Metadata{}, fmt.Errorf("getting piece metadata for piece %s: %w", pieceCid, err)
	}

	stats.Record(s.ctx, metrics.SuccessGetPieceMetadataCount.M(1))
	return md.Metadata, nil
}

func (s *Store) GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error) {
	log.Debugw("handle.get-piece-deals", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_piece_deals")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.get_piece_deals"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.get-piece-deals", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		err = normalizePieceCidError(pieceCid, err)
		stats.Record(s.ctx, metrics.FailureGetPieceDealsCount.M(1))
		return nil, fmt.Errorf("getting piece deals for piece %s: %w", pieceCid, err)
	}

	stats.Record(s.ctx, metrics.SuccessGetPieceDealsCount.M(1))
	return md.Deals, nil
}

// Get all pieces that contain a multihash (used when retrieving by payload CID)
func (s *Store) PiecesContainingMultihash(ctx context.Context, m mh.Multihash) ([]cid.Cid, error) {
	log.Debugw("handle.pieces-containing-mh", "mh", m)

	ctx, span := tracing.Tracer.Start(ctx, "store.pieces_containing_multihash")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.piece_containing_multihash"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.pieces-containing-mh", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	pcs, err := s.db.GetPieceCidsByMultihash(ctx, m)
	if err != nil {
		stats.Record(s.ctx, metrics.FailurePiecesContainingMultihashCount.M(1))
	} else {
		stats.Record(s.ctx, metrics.SuccessPiecesContainingMultihashCount.M(1))
	}
	return pcs, normalizeMultihashError(m, err)
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) (<-chan types.IndexRecord, error) {
	log.Warnw("handle.get-index", "pieceCid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_index")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.get_index"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Warnw("handled.get-index", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureGetIndexCount.M(1))
		return nil, normalizePieceCidError(pieceCid, err)
	}

	records, err := s.db.AllRecords(ctx, md.Cursor)
	if err != nil {
		err = normalizePieceCidError(pieceCid, err)
		stats.Record(s.ctx, metrics.FailureGetIndexCount.M(1))
		return nil, fmt.Errorf("getting all records for cursor %d: %w", md.Cursor, err)
	}

	log.Warnw("handle.get-index.records", "len(records)", len(records))

	recs := make(chan types.IndexRecord, len(records))
	for _, r := range records {
		recs <- types.IndexRecord{Record: r}
	}
	close(recs)

	stats.Record(s.ctx, metrics.SuccessGetIndexCount.M(1))
	return recs, nil
}

func (s *Store) IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.is_indexed"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()
	t, err := s.IndexedAt(ctx, pieceCid)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureIsIndexedCount.M(1))
		return false, err
	}
	stats.Record(s.ctx, metrics.SuccessIsIndexedCount.M(1))
	return !t.IsZero(), nil
}

func (s *Store) IsCompleteIndex(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	log.Debugw("handle.is-complete-index", "pieceCid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.is_incomplete_index")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.is_complete_index"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.is-complete-index", "took", time.Since(now).String())
	}(time.Now())

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureIsCompleteIndexCount.M(1))
		return false, normalizePieceCidError(pieceCid, err)
	}

	stats.Record(s.ctx, metrics.SuccessIsCompleteIndexCount.M(1))
	return md.CompleteIndex, nil
}

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record, isCompleteIndex bool) <-chan types.AddIndexProgress {
	log.Debugw("handle.add-index", "records", len(records))

	ctx, span := tracing.Tracer.Start(ctx, "store.add_index")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.add_index"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.add-index", "took", time.Since(now).String())
	}(time.Now())

	progress := make(chan types.AddIndexProgress, 1)
	go func() {
		defer close(progress)

		s.Lock()
		defer s.Unlock()
		failureMetrics := true
		defer func() {
			if failureMetrics {
				stats.Record(s.ctx, metrics.FailureAddIndexCount.M(1))
			} else {
				stats.Record(s.ctx, metrics.SuccessAddIndexCount.M(1))
			}
		}()

		var recs []carindex.Record
		for _, r := range records {
			recs = append(recs, carindex.Record{
				Cid:    r.Cid,
				Offset: r.Offset,
			})
		}

		err := s.db.SetMultihashesToPieceCid(ctx, recs, pieceCid)
		if err != nil {
			progress <- types.AddIndexProgress{Err: err.Error()}
			return
		}
		progress <- types.AddIndexProgress{Progress: 0.45}

		var cursor uint64
		var keyCursorPrefix string

		// get the metadata for the piece
		// allocate a new cursor only if metadata doesn't exist. This is required to be able
		// to handle multiple AddIndex calls for the same pieceCid
		md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
		if err != nil {
			if !errors.Is(err, ds.ErrNotFound) {
				progress <- types.AddIndexProgress{Err: err.Error()}
				return
			}
			// there isn't yet any metadata, so create new metadata
			md = newLeveldbMetadata()

			// get and set next cursor (handle synchronization, maybe with CAS)
			cursor, keyCursorPrefix, err = s.db.NextCursor(ctx)
			if err != nil {
				progress <- types.AddIndexProgress{Err: err.Error()}
				return
			}

			// allocate metadata for pieceCid
			err = s.db.SetNextCursor(ctx, cursor+1)
			if err != nil {
				progress <- types.AddIndexProgress{Err: err.Error()}
				return
			}

			md.Cursor = cursor
			md.CompleteIndex = isCompleteIndex
		} else {
			cursor = md.Cursor
			keyCursorPrefix = KeyCursorPrefix(cursor)
		}

		// process index and store entries
		for _, rec := range records {
			err := s.db.AddIndexRecord(ctx, keyCursorPrefix, rec)
			if err != nil {
				progress <- types.AddIndexProgress{Err: err.Error()}
				return
			}
		}
		progress <- types.AddIndexProgress{Progress: 0.9}

		// mark indexing as complete
		md.IndexedAt = time.Now()

		err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md)
		if err != nil {
			progress <- types.AddIndexProgress{Err: err.Error()}
			return
		}
		progress <- types.AddIndexProgress{Progress: 0.95}

		err = s.db.Sync(ctx, ds.NewKey(fmt.Sprintf("%d", cursor)))
		if err != nil {
			progress <- types.AddIndexProgress{Err: err.Error()}
			return
		}
		progress <- types.AddIndexProgress{Progress: 1}
		failureMetrics = false
	}()

	return progress
}

func (s *Store) IndexedAt(ctx context.Context, pieceCid cid.Cid) (time.Time, error) {
	log.Debugw("handle.indexed-at", "pieceCid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.indexed_at")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.indexed_at"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.indexed-at", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil && !errors.Is(err, ds.ErrNotFound) {
		stats.Record(s.ctx, metrics.FailureIndexedAtCount.M(1))
		return time.Time{}, err
	}

	stats.Record(s.ctx, metrics.SuccessIndexedAtCount.M(1))
	return md.IndexedAt, nil
}

func (s *Store) PiecesCount(ctx context.Context, maddr address.Address) (int, error) {
	log.Debugw("handle.pieces-count")

	ctx, span := tracing.Tracer.Start(ctx, "store.pieces_count")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.pieces_count"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.pieces-count", "took", time.Since(now).String())
	}(time.Now())

	out, err := s.db.PiecesCount(ctx, maddr)
	if err != nil {
		stats.Record(s.ctx, metrics.FailurePiecesCountCount.M(1))
		return 0, err
	}
	stats.Record(s.ctx, metrics.SuccessPiecesCountCount.M(1))
	return out, nil
}

func (s *Store) ScanProgress(ctx context.Context, maddr address.Address) (*types.ScanProgress, error) {
	log.Debugw("handle.scan-progress")

	ctx, span := tracing.Tracer.Start(ctx, "store.scan_progress")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.scan_progress"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.scan-progress", "took", time.Since(now).String())
	}(time.Now())

	out, err := s.db.ScanProgress(ctx, maddr)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureScanProgressCount.M(1))
		return nil, err
	}
	stats.Record(s.ctx, metrics.SuccessScanProgressCount.M(1))
	return out, nil
}

func (s *Store) ListPieces(ctx context.Context) ([]cid.Cid, error) {
	log.Debugw("handle.list-pieces")

	ctx, span := tracing.Tracer.Start(ctx, "store.list_pieces")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.list_pieces"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.list-pieces", "took", time.Since(now).String())
	}(time.Now())

	out, err := s.db.ListPieces(ctx)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureListPiecesCount.M(1))
		return nil, err
	}
	stats.Record(s.ctx, metrics.SuccessListPiecesCount.M(1))
	return out, nil
}

func (s *Store) NextPiecesToCheck(ctx context.Context, maddr address.Address) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.next_pieces_to_check")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.next_pieces_to_check"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.next-pieces-to-check", "took", time.Since(now).String())
	}(time.Now())

	out, err := s.db.NextPiecesToCheck(ctx, maddr)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureNextPiecesToCheckCount.M(1))
		return nil, err
	}
	stats.Record(s.ctx, metrics.SuccessNextPiecesToCheckCount.M(1))
	return out, nil
}

func (s *Store) FlagPiece(ctx context.Context, pieceCid cid.Cid, hasUnsealedCopy bool, maddr address.Address) error {
	log.Debugw("handle.flag-piece", "piece-cid", pieceCid, "hasUnsealedCopy", hasUnsealedCopy)

	ctx, span := tracing.Tracer.Start(ctx, "store.flag_piece")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.flag_piece"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.flag-piece", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	now := time.Now()

	// Get the existing deals for the piece
	fm, err := s.db.GetPieceCidToFlagged(ctx, pieceCid, maddr)
	if err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			stats.Record(s.ctx, metrics.FailureFlagPieceCount.M(1))
			return fmt.Errorf("getting piece cid flagged metadata for piece %s: %w", pieceCid, err)
		}
		// there isn't yet any flagged metadata, so create new metadata
		fm = LeveldbFlaggedMetadata{CreatedAt: now, MinerAddr: maddr}
	}

	fm.UpdatedAt = now
	fm.HasUnsealedCopy = hasUnsealedCopy

	// Write the piece metadata back to the db
	err = s.db.SetPieceCidToFlagged(ctx, pieceCid, maddr, fm)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureFlagPieceCount.M(1))
		return err
	}

	stats.Record(s.ctx, metrics.SuccessFlagPieceCount.M(1))
	return nil
}

func (s *Store) UnflagPiece(ctx context.Context, pieceCid cid.Cid, maddr address.Address) error {
	log.Debugw("handle.unflag-piece", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.unflag_piece")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.unflag_piece"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.unflag-piece", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	err := s.db.DeletePieceCidToFlagged(ctx, pieceCid, maddr)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureUnflagPieceCount.M(1))
		return fmt.Errorf("deleting piece cid flagged metadata for piece %s: %w", pieceCid, err)
	}
	stats.Record(s.ctx, metrics.SuccessUnflagPieceCount.M(1))
	return nil
}

func (s *Store) FlaggedPiecesList(ctx context.Context, filter *types.FlaggedPiecesListFilter, cursor *time.Time, offset int, limit int) ([]model.FlaggedPiece, error) {
	log.Debugw("handle.flagged-pieces-list")

	ctx, span := tracing.Tracer.Start(ctx, "store.flagged_pieces_list")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.flagged_piece_list"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.flagged-pieces-list", "took", time.Since(now).String())
	}(time.Now())

	out, err := s.db.ListFlaggedPieces(ctx, filter, cursor, offset, limit)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureFlaggedPiecesListCount.M(1))
		return nil, err
	}
	stats.Record(s.ctx, metrics.SuccessFlaggedPiecesListCount.M(1))
	return out, nil
}

func (s *Store) FlaggedPiecesCount(ctx context.Context, filter *types.FlaggedPiecesListFilter) (int, error) {
	log.Debugw("handle.flagged-pieces-count")

	ctx, span := tracing.Tracer.Start(ctx, "store.flagged_pieces_count")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.flagged_pieces_count"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.flagged-pieces-count", "took", time.Since(now).String())
	}(time.Now())

	out, err := s.db.FlaggedPiecesCount(ctx, filter)
	if err != nil {
		stats.Record(s.ctx, metrics.FailureFlaggedPiecesCountCount.M(1))
		return 0, err
	}
	stats.Record(s.ctx, metrics.SuccessFlaggedPiecesCountCount.M(1))
	return out, nil
}

func normalizePieceCidError(pieceCid cid.Cid, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ds.ErrNotFound) {
		return fmt.Errorf("piece %s: %s", pieceCid, types.ErrNotFound)
	}
	return err
}

func normalizeMultihashError(m mh.Multihash, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ds.ErrNotFound) {
		return fmt.Errorf("multihash %s: %s", m, types.ErrNotFound)
	}
	return err
}

// RemoveDealForPiece remove Single deal for pieceCID. If []Deals is empty then Metadata is removed as well
func (s *Store) RemoveDealForPiece(ctx context.Context, pieceCid cid.Cid, dealUuid string) error {
	log.Debugw("handle.remove-deal-for-piece", "piece-cid", pieceCid, "deal-uuid", dealUuid)

	ctx, span := tracing.Tracer.Start(ctx, "store.remove_deal_for_piece")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.remove_deal_for_piece"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.remove-deal-for-piece", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	failureMetrics := true
	defer func() {
		if failureMetrics {
			stats.Record(s.ctx, metrics.FailureRemoveDealForPieceCount.M(1))
		} else {
			stats.Record(s.ctx, metrics.SuccessRemoveDealForPieceCount.M(1))
		}
	}()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			failureMetrics = false
			return nil
		}
		return err
	}

	for i, v := range md.Deals {
		if v.DealUuid == dealUuid {
			md.Deals[i] = md.Deals[len(md.Deals)-1]
			md.Deals = md.Deals[:len(md.Deals)-1]
			break
		}
	}

	if len(md.Deals) == 0 {
		// Remove Metadata if removed deal was last one
		if err := s.db.RemovePieceMetadata(ctx, pieceCid); err != nil {
			return fmt.Errorf("failed to remove the Metadata after removing the last deal: %w", err)
		}
		failureMetrics = false
		return nil
	}

	err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md)
	if err != nil {
		return err
	}

	failureMetrics = false
	return nil
}

// RemovePieceMetadata remove all Metadata for pieceCID
func (s *Store) RemovePieceMetadata(ctx context.Context, pieceCid cid.Cid) error {
	log.Debugw("handle.remove-piece-metadata", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.remove_piece_metadata")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.remove_piece_metadata"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.remove-piece-metadata", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	if err := s.db.RemovePieceMetadata(ctx, pieceCid); err != nil {
		stats.Record(s.ctx, metrics.FailureRemovePieceMetadataCount.M(1))
		return err
	}

	stats.Record(s.ctx, metrics.SuccessRemovePieceMetadataCount.M(1))
	return nil
}

// RemoveIndexes removes all MultiHashes for pieceCID. To be used manually in case of failure
// in RemoveDealForPiece or RemovePieceMetadata. Metadata for the piece must be
// present in the database
func (s *Store) RemoveIndexes(ctx context.Context, pieceCid cid.Cid) error {
	log.Debugw("handle.remove-indexes", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.remove_indexes")
	defer span.End()

	ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, "ldb.remove_indexes"))
	stop := metrics.Timer(ctx, metrics.APIRequestDuration)
	defer stop()

	defer func(now time.Time) {
		log.Debugw("handled.remove-indexes", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	failureMetrics := true
	defer func() {
		if failureMetrics {
			stats.Record(s.ctx, metrics.FailureRemoveIndexesCount.M(1))
		} else {
			stats.Record(s.ctx, metrics.SuccessRemoveIndexesCount.M(1))
		}
	}()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return err
	}

	if err := s.db.RemoveIndexes(ctx, md.Cursor, pieceCid); err != nil {
		return err
	}

	md.IndexedAt = time.Time{}

	if err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md); err == nil {
		failureMetrics = false
	}

	return err
}
