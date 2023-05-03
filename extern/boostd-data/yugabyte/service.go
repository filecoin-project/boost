package yugabyte

import (
	"context"
	_ "embed"
	"fmt"
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
	"time"
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

type Store struct {
	settings DBSettings
	cluster  *gocql.ClusterConfig
	session  *gocql.Session
	db       *pgxpool.Pool
}

var _ types.ServiceImpl = (*Store)(nil)

func NewStore(settings DBSettings) *Store {
	if settings.PayloadPiecesParallelism == 0 {
		settings.PayloadPiecesParallelism = 16
	}

	cluster := gocql.NewCluster(settings.Hosts...)
	return &Store{
		settings: settings,
		cluster:  cluster,
	}
}

func (s *Store) Start(ctx context.Context) error {
	session, err := s.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("creating yugabyte cluster: %w", err)
	}
	s.session = session

	db, err := pgxpool.Connect(context.Background(), s.settings.ConnectString)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	s.db = db

	return nil
}

func (s *Store) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.add_deal_for_piece")
	defer span.End()

	err := s.createPieceMetadata(ctx, pieceCid)
	if err != nil {
		return err
	}

	qry := `INSERT INTO idx.PieceDeal ` +
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
	qry := `INSERT INTO idx.PieceMetadata (PieceCid, Version, CreatedAt) VALUES (?, ?, ?) IF NOT EXISTS`
	err := s.session.Query(qry, pieceCid.String(), pieceMetadataVersion, time.Now()).WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("inserting piece metadata for piece %s: %w", pieceCid, err)
	}
	return nil
}

// TODO: Do we need this?
func (s *Store) MarkIndexErrored(ctx context.Context, pieceCid cid.Cid, idxErr string) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.mark-piece-index-errored")
	defer span.End()

	//err := s.db.MarkIndexErrored(ctx, pieceCid, errors.New(idxErr))
	//if err != nil {
	//	return normalizePieceCidError(pieceCid, err)
	//}
	//
	//return s.FlagPiece(ctx, pieceCid)
	return nil
}

func (s *Store) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.get_offset_size")
	defer span.End()

	var offset, size uint64
	qry := `SELECT BlockOffset, BlockSize FROM idx.PieceBlockOffsetSize WHERE PieceCid = ? AND PayloadMultihash = ?`
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
	qry := `SELECT Version, IndexedAt, CompleteIndex, Error, ErrorType ` +
		`FROM idx.PieceMetadata WHERE PieceCid = ?`
	err := s.session.Query(qry, pieceCid.String()).WithContext(ctx).
		Scan(&md.Version, &md.IndexedAt, &md.CompleteIndex, &md.Error, &md.ErrorType)
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
		`FROM idx.PieceDeal WHERE PieceCid = ?`
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

	var bz []byte
	qry := `SELECT PieceCids FROM idx.PayloadToPieces WHERE PayloadMultihash = ?`
	err := s.session.Query(qry, trimMultihash(m)).WithContext(ctx).Scan(&bz)
	if err != nil {
		err = normalizeMultihashError(m, err)
		return nil, fmt.Errorf("getting pieces containing multihash: %w", err)
	}

	return bytesToCids(bz)
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) ([]model.Record, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.get_index")
	defer span.End()

	qry := `SELECT PayloadMultihash, BlockOffset, BlockSize FROM idx.PieceBlockOffsetSize WHERE PieceCid = ?`
	iter := s.session.Query(qry, pieceCid.Bytes()).WithContext(ctx).Iter()

	var records []model.Record
	var payloadMHBz []byte
	var offset, size uint64
	for iter.Scan(&payloadMHBz, &offset, &size) {
		_, pmh, err := multihash.MHFromBytes(payloadMHBz)
		if err != nil {
			return nil, fmt.Errorf("scanning mulithash: %w", err)
		}

		records = append(records, model.Record{
			Cid: cid.NewCidV1(cid.Raw, pmh),
			OffsetSize: model.OffsetSize{
				Offset: offset,
				Size:   size,
			},
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("getting piece index for piece %s: %w", pieceCid, err)
	}

	// For correctness, we should always return a not found error if there is
	// no piece with the piece cid
	if len(records) == 0 {
		_, err := s.getPieceMetadata(ctx, pieceCid)
		if err != nil {
			return nil, err
		}
	}

	return records, nil
}

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, recs []model.Record, isCompleteIndex bool) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.add_index")
	defer span.End()

	if len(recs) == 0 {
		return nil
	}

	// Add a mapping from multihash -> piece cid so that clients can look up
	// which pieces contain a multihash
	err := s.addMultihashesToPieces(ctx, pieceCid, recs)
	if err != nil {
		return err
	}

	// Add a mapping from piece cid -> offset / size of each block so that
	// clients can get the block info for all blocks in a piece
	err = s.addPieceInfos(ctx, pieceCid, recs)
	if err != nil {
		return err
	}

	// Ensure the piece metadata exists
	err = s.createPieceMetadata(ctx, pieceCid)
	if err != nil {
		return err
	}

	// Mark indexing as complete for the piece
	qry := `UPDATE idx.PieceMetadata ` +
		`SET IndexedAt = ?, CompleteIndex = ?, Error = '', ErrorType = '' ` +
		`WHERE PieceCid = ?`
	err = s.session.Query(qry, time.Now(), isCompleteIndex, pieceCid.String()).WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("marking indexing as complete for piece %s", pieceCid)
	}

	return nil
}

func (s *Store) addMultihashesToPieces(ctx context.Context, pieceCid cid.Cid, recs []model.Record) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.add_index.payloadpiece")
	defer span.End()

	queue := make(chan []byte, len(recs))
	for _, rec := range recs {
		queue <- rec.Cid.Hash()
	}
	close(queue)

	var eg errgroup.Group
	for i := 0; i < s.settings.PayloadPiecesParallelism; i++ {
		eg.Go(func() error {
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case multihashBytes, ok := <-queue:
					if !ok {
						// Finished adding all the queued items, exit the thread
						return nil
					}

					q := `INSERT INTO idx.PayloadToPieces (PayloadMultihash, PieceCids) VALUES (?, ?)`
					err := s.session.Query(q, trimMultihash(multihashBytes), pieceCid.Bytes()).Exec()
					if err != nil {
						return fmt.Errorf("inserting into PayloadToPieces: %w", err)
					}
				}
			}

			return ctx.Err()
		})
	}
	return eg.Wait()
}

func (s *Store) addPieceInfos(ctx context.Context, pieceCid cid.Cid, recs []model.Record) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.add_index.pieceinfo")
	defer span.End()

	batchEntries := make([]gocql.BatchEntry, 0, len(recs))
	insertPieceOffsetsQry := `INSERT INTO idx.PieceBlockOffsetSize (PieceCid, PayloadMultihash, BlockOffset, BlockSize) VALUES (?, ?, ?, ?)`
	for _, rec := range recs {
		batchEntries = append(batchEntries, gocql.BatchEntry{
			Stmt:       insertPieceOffsetsQry,
			Args:       []interface{}{pieceCid.Bytes(), rec.Cid.Hash(), rec.Offset, rec.Size},
			Idempotent: true,
		})
	}

	// The Cassandra driver has a 50k limit on batch statements. Keeping
	// batch size small makes sure we're under the limit.
	const batchSize = 49000
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
			continue
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

func (s *Store) ListPieces(ctx context.Context) ([]cid.Cid, error) {
	ctx, span := tracing.Tracer.Start(ctx, "store.list_pieces")
	defer span.End()

	iter := s.session.Query("SELECT PieceCid FROM idx.PieceMetadata").WithContext(ctx).Iter()
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

// RemoveDealForPiece removes Single deal for pieceCID. If []Deals is empty then Metadata is removed as well
func (s *Store) RemoveDealForPiece(ctx context.Context, pieceCid cid.Cid, dealId string) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.remove_deal_for_piece")
	defer span.End()

	return nil
}

// RemovePieceMetadata removes all Metadata for pieceCID. To be used manually in case of failure
// in RemoveDealForPiece
func (s *Store) RemovePieceMetadata(ctx context.Context, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.remove_piece_metadata")
	defer span.End()

	return nil
}

// RemoveIndexes removes all MultiHashes for pieceCID. To be used manually in case of failure
// in RemoveDealForPiece or RemovePieceMetadata. Metadata for the piece must be
// present in the database
func (s *Store) RemoveIndexes(ctx context.Context, pieceCid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "store.remove_indexes")
	defer span.End()

	return nil
}
