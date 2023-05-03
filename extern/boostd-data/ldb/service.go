package ldb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	carindex "github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
)

// The current piece metadata version. This version will be used when doing
// data migrations (migrations are not yet implemented in version 1).
const pieceMetadataVersion = "1"

var log = logging.Logger("boostd-data-ldb")

type LeveldbFlaggedMetadata struct {
	CreatedAt time.Time `json:"c"`
	UpdatedAt time.Time `json:"u"`
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
	s.db, err = newDB(repopath, false)
	if err != nil {
		return err
	}

	// Prepare db with a cursor
	err = s.db.InitCursor(ctx)
	if err != nil {
		return err
	}

	log.Debugw("new leveldb local index directory service", "repo path", repopath)
	return nil
}

func (s *Store) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	log.Debugw("handle.add-deal-for-piece", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.add_deal_for_piece")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.add-deal-for-piece", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	// Get the existing deals for the piece
	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			return fmt.Errorf("getting piece cid metadata for piece %s: %w", pieceCid, err)
		}
		// there isn't yet any metadata, so create new metadata
		md = newLeveldbMetadata()
	}

	// Check if the deal has already been added
	for _, dl := range md.Deals {
		if dl == dealInfo {
			return nil
		}
	}

	// Add the deal to the list
	md.Deals = append(md.Deals, dealInfo)

	// Write the piece metadata back to the db
	err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	log.Debugw("handle.get-offset-size", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_offset_size")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.get-offset-size", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return nil, normalizePieceCidError(pieceCid, err)
	}

	return s.db.GetOffsetSize(ctx, fmt.Sprintf("%d", md.Cursor)+"/", hash)
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

	s.Lock()
	defer s.Unlock()

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

	s.Lock()
	defer s.Unlock()

	pcs, err := s.db.GetPieceCidsByMultihash(ctx, m)
	return pcs, normalizeMultihashError(m, err)
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) ([]model.Record, error) {
	log.Warnw("handle.get-index", "pieceCid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.get_index")
	defer span.End()

	defer func(now time.Time) {
		log.Warnw("handled.get-index", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return nil, normalizePieceCidError(pieceCid, err)
	}

	records, err := s.db.AllRecords(ctx, md.Cursor)
	if err != nil {
		err = normalizePieceCidError(pieceCid, err)
		return nil, fmt.Errorf("getting all records for cursor %d: %w", md.Cursor, err)
	}

	log.Warnw("handle.get-index.records", "len(records)", len(records))

	return records, nil
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

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record, isCompleteIndex bool) error {
	log.Debugw("handle.add-index", "records", len(records))

	ctx, span := tracing.Tracer.Start(ctx, "store.add_index")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.add-index", "took", time.Since(now).String())
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

	// get the metadata for the piece
	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			return fmt.Errorf("getting piece cid metadata for piece %s: %w", pieceCid, err)
		}
		// there isn't yet any metadata, so create new metadata
		md = newLeveldbMetadata()
	}

	// mark indexing as complete
	md.Cursor = cursor
	md.IndexedAt = time.Now()
	md.CompleteIndex = isCompleteIndex

	err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md)
	if err != nil {
		return err
	}

	err = s.db.Sync(ctx, ds.NewKey(fmt.Sprintf("%d", cursor)))
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
		log.Debugw("handled.indexed-at", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil && !errors.Is(err, ds.ErrNotFound) {
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

	defer func(now time.Time) {
		log.Debugw("handled.next-pieces-to-check", "took", time.Since(now).String())
	}(time.Now())

	return s.db.NextPiecesToCheck(ctx)
}

func (s *Store) FlagPiece(ctx context.Context, pieceCid cid.Cid) error {
	log.Debugw("handle.flag-piece", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.flag_piece")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.flag-piece", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	now := time.Now()

	// Get the existing deals for the piece
	fm, err := s.db.GetPieceCidToFlagged(ctx, pieceCid)
	if err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			return fmt.Errorf("getting piece cid flagged metadata for piece %s: %w", pieceCid, err)
		}
		// there isn't yet any flagged metadata, so create new metadata
		fm = LeveldbFlaggedMetadata{CreatedAt: now}
	}

	fm.UpdatedAt = now

	// Write the piece metadata back to the db
	err = s.db.SetPieceCidToFlagged(ctx, pieceCid, fm)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) UnflagPiece(ctx context.Context, pieceCid cid.Cid) error {
	log.Debugw("handle.unflag-piece", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.unflag_piece")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.unflag-piece", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	err := s.db.DeletePieceCidToFlagged(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("deleting piece cid flagged metadata for piece %s: %w", pieceCid, err)
	}
	return nil
}

func (s *Store) FlaggedPiecesList(ctx context.Context, cursor *time.Time, offset int, limit int) ([]model.FlaggedPiece, error) {
	log.Debugw("handle.flagged-pieces-list")

	ctx, span := tracing.Tracer.Start(ctx, "store.flagged_pieces_list")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.flagged-pieces-list", "took", time.Since(now).String())
	}(time.Now())

	return s.db.ListFlaggedPieces(ctx)
}

func (s *Store) FlaggedPiecesCount(ctx context.Context) (int, error) {
	log.Debugw("handle.flagged-pieces-count")

	ctx, span := tracing.Tracer.Start(ctx, "store.flagged_pieces_count")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.flagged-pieces-count", "took", time.Since(now).String())
	}(time.Now())

	return s.db.FlaggedPiecesCount(ctx)
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

	defer func(now time.Time) {
		log.Debugw("handled.remove-deal-for-piece", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
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
		return nil
	}

	err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md)
	if err != nil {
		return err
	}

	return nil
}

// RemovePieceMetadata remove all Metadata for pieceCID
func (s *Store) RemovePieceMetadata(ctx context.Context, pieceCid cid.Cid) error {
	log.Debugw("handle.remove-piece-metadata", "piece-cid", pieceCid)

	ctx, span := tracing.Tracer.Start(ctx, "store.remove_piece_metadata")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.remove-piece-metadata", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

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
		log.Debugw("handled.remove-indexes", "took", time.Since(now).String())
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		return err
	}

	if err := s.db.RemoveIndexes(ctx, md.Cursor, pieceCid); err != nil {
		return err
	}

	md.IndexedAt = time.Time{}

	err = s.db.SetPieceCidToMetadata(ctx, pieceCid, md)

	return err
}
