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

type LeveldbMetadata struct {
	model.Metadata
	Cursor uint64 `json:"c"`
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
	err = s.db.InitCursor(ctx)
	if err != nil {
		return err
	}

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

	// Get the existing deals for the piece
	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			return fmt.Errorf("getting piece cid metadata for piece %s: %w", pieceCid, err)
		}
		// there isn't yet any metadata, so create new metadata
		md = LeveldbMetadata{}
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

func (s *Store) SetCarSize(ctx context.Context, pieceCid cid.Cid, size uint64) error {
	log.Debugw("handle.set-car-size", "piece-cid", pieceCid, "size", size)

	ctx, span := tracing.Tracer.Start(context.Background(), "store.set-car-size")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.set-car-size", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	err := s.db.SetCarSize(ctx, pieceCid, size)
	return normalizePieceCidError(pieceCid, err)
}

func (s *Store) MarkIndexErrored(ctx context.Context, pieceCid cid.Cid, idxErr error) error {
	log.Debugw("handle.mark-piece-index-errored", "piece-cid", pieceCid, "err", idxErr)

	ctx, span := tracing.Tracer.Start(ctx, "store.mark-piece-index-errored")
	defer span.End()

	defer func(now time.Time) {
		log.Debugw("handled.mark-piece-index-errored", "took", fmt.Sprintf("%s", time.Since(now)))
	}(time.Now())

	s.Lock()
	defer s.Unlock()

	err := s.db.MarkIndexErrored(ctx, pieceCid, idxErr)
	return normalizePieceCidError(pieceCid, err)
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
		return nil, normalizePieceCidError(pieceCid, err)
	}

	return s.db.GetOffsetSize(ctx, fmt.Sprintf("%d", md.Cursor)+"/", hash)
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
		log.Debugw("handled.pieces-containing-mh", "took", fmt.Sprintf("%s", time.Since(now)))
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
		log.Warnw("handled.get-index", "took", fmt.Sprintf("%s", time.Since(now)))
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

	// get the metadata for the piece
	md, err := s.db.GetPieceCidToMetadata(ctx, pieceCid)
	if err != nil {
		if !errors.Is(err, ds.ErrNotFound) {
			return fmt.Errorf("getting piece cid metadata for piece %s: %w", pieceCid, err)
		}
		// there isn't yet any metadata, so create new metadata
		md = LeveldbMetadata{}
	}

	// mark indexing as complete
	md.Cursor = cursor
	md.IndexedAt = time.Now()

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
			return fmt.Errorf("Failed to remove the Metadata after removing the last deal: %w", err)
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

	return nil
}
