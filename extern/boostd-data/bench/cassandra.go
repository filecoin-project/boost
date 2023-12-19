package main

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
	"github.com/yugabyte/gocql"
	"golang.org/x/sync/errgroup"
)

var cassandraCmd = &cli.Command{
	Name:   "cassandra",
	Before: before,
	Subcommands: []*cli.Command{
		initCmd(createCassandra),
		dropCmd(createCassandra),
		loadCmd(createCassandra, &cli.IntFlag{
			Name:  "payload-pieces-parallelism",
			Value: 16,
		}),
		bitswapCmd(createCassandra),
		graphsyncCmd(createCassandra),
	},
}

func createCassandra(ctx context.Context, cctx *cli.Context) (BenchDB, error) {
	return NewCassandraDB(cctx.String("connect-string"), cctx.Int("payload-pieces-parallelism"))
}

type CassandraDB struct {
	session                  *gocql.Session
	payloadPiecesParallelism int
}

func NewCassandraDB(connectString string, payloadPiecesParallelism int) (*CassandraDB, error) {
	cluster := gocql.NewCluster(connectString)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("creating cluster: %w", err)
	}
	return &CassandraDB{
		session:                  session,
		payloadPiecesParallelism: payloadPiecesParallelism,
	}, nil
}

func (c *CassandraDB) Name() string {
	return "Cassandra DB"
}

//go:embed create_tables.cql
var createTablesCQL string

func (c *CassandraDB) Init(ctx context.Context, b bool) error {
	tables := []string{`PayloadToPieces`, `PieceBlockOffsetSize`}
	for _, tbl := range tables {
		qry := `drop table if exists bench.` + tbl
		log.Debug(qry)
		err := c.session.Query(qry).WithContext(ctx).Exec()
		if err != nil {
			log.Warn(err)
		}
	}

	createTablesLines := strings.Split(createTablesCQL, ";")
	for _, line := range createTablesLines {
		line = strings.Trim(line, "\n \t")
		if line == "" {
			continue
		}
		log.Debug(line)
		err := c.session.Query(line).WithContext(ctx).Exec()
		if err != nil {
			return fmt.Errorf("creating tables: executing\n%s\n%w", line, err)
		}
	}

	return nil
}

func (c *CassandraDB) Cleanup(ctx context.Context) error {
	_ = c.session.Query(`drop table if exists bench.PayloadToPieces`).WithContext(ctx).Exec()
	_ = c.session.Query(`drop table if exists bench.PieceBlockOffsetSize`).WithContext(ctx).Exec()
	_ = c.session.Query(`drop keyspace bench`).WithContext(ctx).Exec()
	c.session.Close()
	return nil
}

func (c *CassandraDB) GetBlockSample(ctx context.Context, count int) ([]pieceBlock, error) {
	// TODO: randomize order
	qry := `SELECT PieceCid, PayloadMultihash FROM bench.PieceBlockOffsetSize LIMIT ?`
	iter := c.session.Query(qry, count).WithContext(ctx).Iter()

	var pieceCidBz, payloadMHBz []byte
	pbs := make([]pieceBlock, 0, count)
	for iter.Scan(&pieceCidBz, &payloadMHBz) {
		_, pcid, err := cid.CidFromBytes(pieceCidBz)
		if err != nil {
			return nil, fmt.Errorf("scanning piece cid: %w", err)
		}

		cp := make([]byte, len(payloadMHBz))
		copy(cp, payloadMHBz)
		_, mh, err := multihash.MHFromBytes(cp)
		if err != nil {
			return nil, fmt.Errorf("scanning multihash: %w", err)
		}

		pbs = append(pbs, pieceBlock{
			PieceCid:         pcid,
			PayloadMultihash: mh,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	return pbs, nil
}

func (c *CassandraDB) AddIndexRecords(ctx context.Context, pieceCid cid.Cid, recs []model.Record) error {
	if len(recs) == 0 {
		return nil
	}

	timeQueryBind := time.Now()
	// Add payload to pieces index
	startPayloadToPieces := time.Now()
	queue := make(chan []byte, len(recs))
	for _, rec := range recs {
		queue <- rec.Cid.Hash()
	}
	close(queue)

	var eg errgroup.Group
	for i := 0; i < c.payloadPiecesParallelism; i++ {
		eg.Go(func() error {
			for ctx.Err() == nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case multihashBytes, ok := <-queue:
					if !ok {
						return nil
					}

					q := `INSERT INTO bench.PayloadToPieces (PayloadMultihash, PieceCids) VALUES (?, ?)`
					err := c.session.Query(q, multihashBytes, pieceCid.Bytes()).Exec()
					if err != nil {
						return fmt.Errorf("inserting into PayloadToPieces: %w", err)
					}
				}
			}

			return ctx.Err()
		})
	}
	err := eg.Wait()
	if err != nil {
		return err
	}
	duration := time.Since(startPayloadToPieces)
	log.Debugw("insert payload-to-pieces complete", "duration", duration.String(), "duration-ms", duration.Milliseconds())

	metrics.GetOrRegisterResettingTimer("csql.query.bind", nil).UpdateSince(timeQueryBind)

	timePrepareInsert := time.Now()
	// Add piece to block info index
	startPieceBlockInfo := time.Now()
	batchEntries := make([]gocql.BatchEntry, 0, len(recs))
	insertPieceOffsetsQry := `INSERT INTO bench.PieceBlockOffsetSize (PieceCid, PayloadMultihash, BlockOffset, BlockSize) VALUES (?, ?, ?, ?)`
	for _, rec := range recs {
		batchEntries = append(batchEntries, gocql.BatchEntry{
			Stmt:       insertPieceOffsetsQry,
			Args:       []interface{}{pieceCid.Bytes(), rec.Cid.Hash(), rec.Offset, rec.Size},
			Idempotent: true,
		})
	}
	metrics.GetOrRegisterResettingTimer("csql.prepare.insert", nil).UpdateSince(timePrepareInsert)

	// Cassandra has a 50k limit on batch statements. Keeping batch size small
	// makes sure we're under the limit.
	const batchSize = 49000
	var batch *gocql.Batch
	for allIdx, entry := range batchEntries {
		if batch == nil {
			timeNewBatch := time.Now()
			batch = c.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx).RetryPolicy(&gocql.ExponentialBackoffRetryPolicy{Max: 120 * time.Second, NumRetries: 25})
			metrics.GetOrRegisterResettingTimer("csql.batch.new", nil).UpdateSince(timeNewBatch)
		}

		batch.Entries = append(batch.Entries, entry)

		if allIdx == len(batchEntries)-1 || len(batch.Entries) == batchSize {
			var err error
			for i := 0; i < 30; i++ {
				st := time.Now()
				err = c.session.ExecuteBatch(batch)
				if err == nil {
					metrics.GetOrRegisterResettingTimer("csql.session.execute", nil).UpdateSince(st)
					break
				}
				metrics.GetOrRegisterResettingTimer("csql.session.execute.err", nil).UpdateSince(st)

				log.Warnf("error executing batch: %s", err)
				time.Sleep(1 * time.Second)
			}
			if err != nil {
				return fmt.Errorf("adding index records for piece %s: %w", pieceCid, err)
			}
			batch = nil
			continue
		}
	}
	duration = time.Since(startPieceBlockInfo)
	log.Debugw("insert piece block info complete", "duration", duration.String(), "duration-ms", duration.Milliseconds())

	return nil
}

func (c *CassandraDB) PiecesContainingMultihash(ctx context.Context, m multihash.Multihash) ([]cid.Cid, error) {
	var bz []byte
	qry := `SELECT PieceCids FROM bench.PayloadToPieces WHERE PayloadMultihash = ?`
	err := c.session.Query(qry, m).WithContext(ctx).Scan(&bz)
	if err != nil {
		return nil, fmt.Errorf("getting pieces containing multihash: %w", err)
	}

	return cidsFromBytes(bz)
}

func (c *CassandraDB) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash multihash.Multihash) (*model.OffsetSize, error) {
	var offset, size uint64
	qry := `SELECT BlockOffset, BlockSize FROM bench.PieceBlockOffsetSize WHERE PieceCid = ? AND PayloadMultihash = ?`
	err := c.session.Query(qry, pieceCid.Bytes(), hash).WithContext(ctx).Scan(&offset, &size)
	if err != nil {
		return nil, fmt.Errorf("getting offset / size: %w", err)
	}

	return &model.OffsetSize{Offset: offset, Size: size}, nil
}

func (c *CassandraDB) GetIterableIndex(ctx context.Context, pieceCid cid.Cid) (index.IterableIndex, error) {
	qry := `SELECT PayloadMultihash, BlockOffset FROM bench.PieceBlockOffsetSize WHERE PieceCid = ?`
	iter := c.session.Query(qry, pieceCid.Bytes()).WithContext(ctx).Iter()

	var records []index.Record
	var payloadMHBz []byte
	var offset uint64
	for iter.Scan(&payloadMHBz, &offset) {
		_, pmh, err := multihash.MHFromBytes(payloadMHBz)
		if err != nil {
			return nil, fmt.Errorf("scanning mulithash: %w", err)
		}

		records = append(records, index.Record{
			Cid:    cid.NewCidV1(cid.Raw, pmh),
			Offset: offset,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	mis := make(index.MultihashIndexSorted)
	err := mis.Load(records)
	if err != nil {
		return nil, err
	}

	return &mis, nil
}
