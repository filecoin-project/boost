package yugabyte

import (
	"context"
	_ "embed"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
	mh "github.com/multiformats/go-multihash"
	"github.com/yugabyte/gocql"
	"github.com/yugabyte/pgx/v4/pgxpool"
	"golang.org/x/sync/errgroup"
)

var log = logging.Logger("boostd-data-yb")

// The current piece metadata version. This version will be used when doing
// data migrations (migrations are not yet implemented in version 1).
const pieceMetadataVersion = "1"

type DBSettings struct {
	// The cassandra hosts to connect to
	Hosts []string
	// The postgres connect string
	ConnectString string
	// The number of threads to use when inserting into the PayloadToPieces index
	PayloadPiecesParallelism int
}

type StoreOpt func(*Store)

// Use the given keyspace for all operations against the cassandra interface
func WithCassandraKeyspace(ks string) StoreOpt {
	return func(s *Store) {
		s.cluster.Keyspace = ks
	}
}

type Store struct {
	settings  DBSettings
	cluster   *gocql.ClusterConfig
	migrator  *Migrator
	session   *gocql.Session
	db        *pgxpool.Pool
	startOnce sync.Once
}

var _ types.ServiceImpl = (*Store)(nil)

func NewStore(settings DBSettings, migrator *Migrator, opts ...StoreOpt) *Store {
	if settings.PayloadPiecesParallelism == 0 {
		settings.PayloadPiecesParallelism = 16
	}

	cluster := gocql.NewCluster(settings.Hosts...)
	cluster.Keyspace = "idx"
	s := &Store{
		settings: settings,
		cluster:  cluster,
		migrator: migrator,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *Store) Start(ctx context.Context) error {
	var startErr error
	s.startOnce.Do(func() {
		// Create cassandra keyspace
		err := s.CreateKeyspace(ctx)
		if err != nil {
			startErr = fmt.Errorf("creating cassandra keyspace %s: %w", s.cluster.Keyspace, err)
			return
		}

		// Create connection to cassandra interface (with new keyspace)
		session, err := s.cluster.CreateSession()
		if err != nil {
			startErr = fmt.Errorf("creating yugabyte cluster: %w", err)
			return
		}
		s.session = session

		// Create connection pool to postgres interface
		db, err := pgxpool.Connect(ctx, s.settings.ConnectString)
		if err != nil {
			startErr = fmt.Errorf("connecting to database: %w", err)
			return
		}
		s.db = db

		// Create tables
		startErr = s.Create(ctx)
	})

	return startErr
}

func (s *Store) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.add_deal_for_piece")
	defer span.End()

	err := s.createPieceMetadata(ctx, pieceCid)
	if err != nil {
		return err
	}

	qry := `INSERT INTO PieceDeal ` +
		`(DealUuid, PieceCid, IsLegacy, ChainDealID, MinerAddr, SectorID, PieceOffset, PieceLength, CarLength) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ` +
		`IF NOT EXISTS`
	err = s.session.Query(qry,
		dealInfo.DealUuid, pieceCid.Bytes(), dealInfo.IsLegacy, dealInfo.ChainDealID, dealInfo.MinerAddr.String(),
		dealInfo.SectorID, dealInfo.PieceOffset, dealInfo.PieceLength, dealInfo.CarLength).
		WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("inserting deal %s for piece %s", dealInfo.DealUuid, pieceCid)
	}

	return nil
}

func (s *Store) createPieceMetadata(ctx context.Context, pieceCid cid.Cid) error {
	qry := `INSERT INTO PieceMetadata (PieceCid, Version, CreatedAt) VALUES (?, ?, ?) IF NOT EXISTS`
	err := s.session.Query(qry, pieceCid.String(), pieceMetadataVersion, time.Now()).WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("inserting piece metadata for piece %s: %w", pieceCid, err)
	}
	return nil
}

func (s *Store) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.get_offset_size")
	defer span.End()

	var offset, size uint64
	qry := `SELECT BlockOffset, BlockSize FROM PieceBlockOffsetSize WHERE PieceCid = ? AND PayloadMultihash = ?`
	err := s.session.Query(qry, pieceCid.Bytes(), hash).WithContext(ctx).Scan(&offset, &size)
	if err != nil {
		err = normalizePieceCidError(pieceCid, err)
		return nil, fmt.Errorf("getting offset / size: %w", err)
	}

	return &model.OffsetSize{Offset: offset, Size: size}, nil
}

// Get piece metadata with deals
func (s *Store) GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error) {
	md, err := s.getPieceMetadata(ctx, pieceCid)
	if err != nil {
		return md, err
	}

	deals, err := s.GetPieceDeals(ctx, pieceCid)
	if err != nil {
		return md, fmt.Errorf("getting deals for piece %s: %w", pieceCid, err)
	}
	md.Deals = deals

	return md, nil
}

// Get the piece metadata without deal information
func (s *Store) getPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.get_piece_metadata")
	defer span.End()

	// Get piece metadata
	var md model.Metadata
	qry := `SELECT Version, IndexedAt, CompleteIndex FROM PieceMetadata WHERE PieceCid = ?`
	err := s.session.Query(qry, pieceCid.String()).WithContext(ctx).
		Scan(&md.Version, &md.IndexedAt, &md.CompleteIndex)
	if err != nil {
		err = normalizePieceCidError(pieceCid, err)
		return md, fmt.Errorf("getting piece metadata: %w", err)
	}

	return md, nil
}

func (s *Store) GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.get_piece_deals")
	defer span.End()

	// Get deals for piece
	qry := `SELECT DealUuid, IsLegacy, ChainDealID, MinerAddr, ` +
		`SectorID, PieceOffset, PieceLength, CarLength ` +
		`FROM PieceDeal WHERE PieceCid = ?`
	iter := s.session.Query(qry, pieceCid.Bytes()).WithContext(ctx).Iter()

	var deals []model.DealInfo
	var deal model.DealInfo
	var minerAddr string
	for iter.Scan(&deal.DealUuid, &deal.IsLegacy, &deal.ChainDealID, &minerAddr,
		&deal.SectorID, &deal.PieceOffset, &deal.PieceLength, &deal.CarLength) {

		ma, err := address.NewFromString(minerAddr)
		if err != nil {
			return nil, fmt.Errorf("parsing miner address for piece %s / deal %s: %w", pieceCid, deal.DealUuid, err)
		}

		deal.MinerAddr = ma
		deals = append(deals, deal)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("getting piece deals: %w", err)
	}

	// For correctness, we should always return a not found error if there is
	// no piece with the piece cid
	if len(deals) == 0 {
		_, err := s.getPieceMetadata(ctx, pieceCid)
		if err != nil {
			return nil, err
		}
	}

	return deals, nil
}

// Get all pieces that contain a multihash (used when retrieving by payload CID)
func (s *Store) PiecesContainingMultihash(ctx context.Context, m mh.Multihash) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.pieces_containing_multihash")
	defer span.End()

	// Get all piece cids referred to by the multihash
	pcids := make([]cid.Cid, 0, 1)
	var bz []byte
	qry := `SELECT PieceCid FROM PayloadToPieces WHERE PayloadMultihash = ?`
	iter := s.session.Query(qry, trimMultihash(m)).WithContext(ctx).Iter()
	for iter.Scan(&bz) {
		pcid, err := cid.Parse(bz)
		if err != nil {
			return nil, fmt.Errorf("parsing piece cid: %w", err)
		}
		pcids = append(pcids, pcid)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("getting pieces containing multihash %s: %w", m, err)
	}

	// No pieces found for multihash, return a "not found" error
	if len(pcids) == 0 {
		return nil, normalizeMultihashError(m, types.ErrNotFound)
	}

	return pcids, nil
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) (<-chan types.IndexRecord, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.get_index")
	defer span.End()

	qry := `SELECT PayloadMultihash, BlockOffset, BlockSize FROM PieceBlockOffsetSize WHERE PieceCid = ?`
	iter := s.session.Query(qry, pieceCid.Bytes()).WithContext(ctx).Iter()

	scannedRecordCh := make(chan struct{}, 1)
	records := make(chan types.IndexRecord)
	go func() {
		defer close(scannedRecordCh)
		defer close(records)

		var payloadMHBz []byte
		var offset, size uint64
		for iter.Scan(&payloadMHBz, &offset, &size) {
			// The scan was successful, which means there is at least one
			// record
			select {
			case scannedRecordCh <- struct{}{}:
			default:
			}

			// Parse the multihash bytes
			_, pmh, err := multihash.MHFromBytes(payloadMHBz)
			if err != nil {
				records <- types.IndexRecord{Error: err}
				return
			}

			records <- types.IndexRecord{
				Record: model.Record{
					Cid: cid.NewCidV1(cid.Raw, pmh),
					OffsetSize: model.OffsetSize{
						Offset: offset,
						Size:   size,
					},
				},
			}
		}
		if err := iter.Close(); err != nil {
			err = fmt.Errorf("getting piece index for piece %s: %w", pieceCid, err)
			records <- types.IndexRecord{Error: err}
		}
	}()

	// Check if there were any records for this piece cid
	var pieceHasRecords bool
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case _, pieceHasRecords = <-scannedRecordCh:
	}

	if !pieceHasRecords {
		// For correctness, we should always return a not found error if there
		// is no piece with the piece cid. Call getPieceMetadata which returns
		// not found if it can't find the piece.
		_, err := s.getPieceMetadata(ctx, pieceCid)
		if err != nil {
			return nil, err
		}
	}

	return records, nil
}

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, recs []model.Record, isCompleteIndex bool) <-chan types.AddIndexProgress {
	ctx, span := tracing.Tracer.Start(ctx, "store.add_index")
	defer span.End()

	// Set up the progress channel
	progress := make(chan types.AddIndexProgress, 2)
	if len(recs) == 0 {
		// If there are no records, set progress to 100% and close the channel
		progress <- types.AddIndexProgress{Progress: 1}
		close(progress)
		return progress
	}

	// Start by sending a progress update of zero
	progress <- types.AddIndexProgress{Progress: 0}
	lastUpdateTime := time.Now()

	var lastUpdateValue *float64
	updateProgress := func(prg float64) {
		// Don't send updates more than once every few seconds
		if time.Since(lastUpdateTime) < 5*time.Second {
			lastUpdateValue = &prg
			return
		}

		// If the channel is full, don't send this progress update, just
		// wait for the next one.
		select {
		case progress <- types.AddIndexProgress{Progress: prg}:
			lastUpdateTime = time.Now()
			lastUpdateValue = nil
		default:
			lastUpdateValue = &prg
		}
	}

	completeProgress := func(err error) {
		var lastProg *types.AddIndexProgress
		if err != nil {
			// If there was an error, send it as the last progress update
			lastProg = &types.AddIndexProgress{Err: err.Error()}
		} else if lastUpdateValue != nil {
			// If there is an outstanding update that hasn't been sent out
			// yet, make sure it gets sent
			lastProg = &types.AddIndexProgress{Progress: *lastUpdateValue}
		}

		if lastProg != nil {
			select {
			case progress <- *lastProg:
			case <-time.After(5 * time.Second):
			}
		}

		// Close the channel
		close(progress)
	}

	go func() {
		// Add a mapping from multihash -> piece cid so that clients can look up
		// which pieces contain a multihash
		err := s.addMultihashesToPieces(ctx, pieceCid, recs, func(addProgress float64) {
			// The first 45% of progress is for adding multihash -> pieces index
			updateProgress(0.45 * addProgress)
		})
		if err != nil {
			completeProgress(err)
			return
		}

		// Add a mapping from piece cid -> offset / size of each block so that
		// clients can get the block info for all blocks in a piece
		err = s.addPieceInfos(ctx, pieceCid, recs, func(addProgress float64) {
			// From 45% - 90% of progress is for adding piece infos
			updateProgress(0.45 + 0.45*addProgress)
		})
		if err != nil {
			completeProgress(err)
			return
		}

		// Ensure the piece metadata exists
		err = s.createPieceMetadata(ctx, pieceCid)
		if err != nil {
			completeProgress(err)
			return
		}
		updateProgress(0.95)

		// Mark indexing as complete for the piece
		qry := `UPDATE PieceMetadata ` +
			`SET IndexedAt = ?, CompleteIndex = ? ` +
			`WHERE PieceCid = ?`
		err = s.session.Query(qry, time.Now(), isCompleteIndex, pieceCid.String()).WithContext(ctx).Exec()
		if err != nil {
			completeProgress(err)
			return
		}
		updateProgress(1)
		completeProgress(nil)
	}()

	return progress
}

func (s *Store) addMultihashesToPieces(ctx context.Context, pieceCid cid.Cid, recs []model.Record, progress func(addProgress float64)) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.add_index.payloadpiece")
	defer span.End()

	var count float64
	return s.execParallel(ctx, recs, s.settings.PayloadPiecesParallelism, func(rec model.Record) error {
		multihashBytes := rec.Cid.Hash()
		q := `INSERT INTO PayloadToPieces (PayloadMultihash, PieceCid) VALUES (?, ?)`
		err := s.session.Query(q, trimMultihash(multihashBytes), pieceCid.Bytes()).Exec()
		if err != nil {
			return fmt.Errorf("inserting into PayloadToPieces: %w", err)
		}

		count++
		progress(count / float64(len(recs)))
		return nil
	})
}

func (s *Store) addPieceInfos(ctx context.Context, pieceCid cid.Cid, recs []model.Record, progress func(addProgress float64)) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.add_index.pieceinfo")
	defer span.End()

	batchEntries := make([]gocql.BatchEntry, 0, len(recs))
	insertPieceOffsetsQry := `INSERT INTO PieceBlockOffsetSize (PieceCid, PayloadMultihash, BlockOffset, BlockSize) VALUES (?, ?, ?, ?)`
	for _, rec := range recs {
		batchEntries = append(batchEntries, gocql.BatchEntry{
			Stmt:       insertPieceOffsetsQry,
			Args:       []interface{}{pieceCid.Bytes(), rec.Cid.Hash(), rec.Offset, rec.Size},
			Idempotent: true,
		})
	}

	// The Cassandra driver has a 50k limit on batch statements. Keeping
	// batch size small makes sure we're under the limit.
	const batchSize = 5000
	var batch *gocql.Batch
	for allIdx, entry := range batchEntries {
		if batch == nil {
			batch = s.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
		}

		batch.Entries = append(batch.Entries, entry)

		if allIdx == len(batchEntries)-1 || len(batch.Entries) == batchSize {
			err := s.session.ExecuteBatch(batch)
			if err != nil {
				return fmt.Errorf("executing offset / size batch insert for piece %s: %w", pieceCid, err)
			}
			batch = nil

			progress((float64(allIdx+1) / float64(len(batchEntries))))
		}
	}

	return nil
}

func (s *Store) IsCompleteIndex(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.is_incomplete_index")
	defer span.End()

	md, err := s.getPieceMetadata(ctx, pieceCid)
	if err != nil {
		return false, err
	}

	return md.CompleteIndex, nil
}

func (s *Store) IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	t, err := s.IndexedAt(ctx, pieceCid)
	if err != nil {
		if isNotFoundErr(err) {
			return false, nil
		}
		return false, err
	}
	return !t.IsZero(), nil
}

func (s *Store) IndexedAt(ctx context.Context, pieceCid cid.Cid) (time.Time, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.indexed_at")
	defer span.End()

	md, err := s.getPieceMetadata(ctx, pieceCid)
	if err != nil {
		if isNotFoundErr(err) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}

	return md.IndexedAt, nil
}

func (s *Store) PiecesCount(ctx context.Context) (int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.pieces_count")
	defer span.End()

	var count int
	qry := `SELECT COUNT(*) FROM PieceMetadata`

	err := s.session.Query(qry).WithContext(ctx).Scan(&count)
	if err != nil {
		return -1, fmt.Errorf("getting pieces count: %w", err)
	}

	return count, nil
}

func (s *Store) ListPieces(ctx context.Context) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.list_pieces")
	defer span.End()

	iter := s.session.Query("SELECT PieceCid FROM PieceMetadata").WithContext(ctx).Iter()
	var pcids []cid.Cid
	var cstr string
	for iter.Scan(&cstr) {
		c, err := cid.Parse(cstr)
		if err != nil {
			return nil, fmt.Errorf("parsing piece cid: %w", err)
		}

		pcids = append(pcids, c)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("getting piece cids: %w", err)
	}

	return pcids, nil
}

// RemoveDealForPiece removes Single deal for pieceCID.
func (s *Store) RemoveDealForPiece(ctx context.Context, pieceCid cid.Cid, dealId string) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.remove_deal_for_piece")
	defer span.End()

	qry := `DELETE FROM PieceDeal WHERE DealUuid = ?`
	err := s.session.Query(qry, dealId).WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("removing deal %s for piece %s: %w", dealId, pieceCid, err)
	}

	dls, err := s.GetPieceDeals(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("getting remaining deals in remove deal for piece %s: %w", pieceCid, err)
	}

	if len(dls) > 0 {
		return nil
	}

	err = s.RemoveIndexes(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("removing deal: %w", err)
	}

	err = s.RemovePieceMetadata(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("removing deal: %w", err)
	}

	return nil
}

// RemovePieceMetadata removes all Metadata for pieceCID.
func (s *Store) RemovePieceMetadata(ctx context.Context, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.remove_piece_metadata")
	defer span.End()

	qry := `DELETE FROM PieceMetadata WHERE PieceCid = ?`
	err := s.session.Query(qry, pieceCid.String()).WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("removing piece metadata for piece %s: %w", pieceCid, err)
	}

	return nil
}

// RemoveIndexes removes all multihash -> piece cid mappings, and all
// offset / size information for the piece.
func (s *Store) RemoveIndexes(ctx context.Context, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.remove_indexes")
	defer span.End()

	// Get multihashes for piece
	recs, err := s.GetIndex(ctx, pieceCid)
	if err != nil {
		return fmt.Errorf("removing indexes for piece %s: getting recs: %w", pieceCid, err)
	}

	// Delete from multihash -> piece cids index
	var eg errgroup.Group
	for i := 0; i < s.settings.PayloadPiecesParallelism; i++ {
		eg.Go(func() error {
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case rec, ok := <-recs:
					if !ok {
						// Finished adding all the queued items, exit the thread
						return nil
					}

					multihashBytes := rec.Cid.Hash()
					q := `DELETE FROM PayloadToPieces WHERE PayloadMultihash = ? AND PieceCid = ?`
					err := s.session.Query(q, trimMultihash(multihashBytes), pieceCid.Bytes()).Exec()
					if err != nil {
						return fmt.Errorf("deleting from PayloadToPieces: %w", err)
					}
				}
			}

			return ctx.Err()
		})
	}
	err = eg.Wait()
	if err != nil {
		return err
	}

	// Delete from piece offsets index
	qry := `DELETE FROM PieceBlockOffsetSize WHERE PieceCid = ?`
	err = s.session.Query(qry, pieceCid.Bytes()).WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("removing indexes for piece %s: deleting offset / size info: %w", pieceCid, err)
	}

	return nil
}

func (s *Store) execParallel(ctx context.Context, recs []model.Record, parallelism int, f func(record model.Record) error) error {
	queue := make(chan model.Record, len(recs))
	for _, rec := range recs {
		queue <- rec
	}
	close(queue)

	var eg errgroup.Group
	for i := 0; i < parallelism; i++ {
		eg.Go(func() error {
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case rec, ok := <-queue:
					if !ok {
						// Finished adding all the queued items, exit the thread
						return nil
					}

					err := f(rec)
					if err != nil {
						return err
					}
				}
			}

			return ctx.Err()
		})
	}
	return eg.Wait()
}
