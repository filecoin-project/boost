package main

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/shared/cliutil"
	"github.com/gocql/gocql"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
	"strings"
	"time"
)

var cassandraCmd = &cli.Command{
	Name:   "cassandra",
	Before: before,
	Flags: append(commonFlags, &cli.StringFlag{
		Name:  "connect-string",
		Value: "localhost",
	}),
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)
		db, err := NewCassandraDB(cctx.String("connect-string"))
		if err != nil {
			return err
		}
		return run(ctx, db, runOptsFromCctx(cctx))
	},
	Subcommands: []*cli.Command{
		loadCmd(createCassandra),
		bitswapCmd(createCassandra),
		graphsyncCmd(createCassandra),
	},
}

func createCassandra(ctx context.Context, connectString string) (BenchDB, error) {
	return NewCassandraDB(connectString)
}

type CassandraDB struct {
	session *gocql.Session
}

func NewCassandraDB(connectString string) (*CassandraDB, error) {
	cluster := gocql.NewCluster(connectString)
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("creating cluster: %w", err)
	}
	return &CassandraDB{session: session}, nil
}

func (c *CassandraDB) Name() string {
	return "Cassandra DB"
}

//go:embed create_tables.cql
var createTablesCQL string

func (c *CassandraDB) Init(ctx context.Context) error {
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
		_, pmh, err := multihash.MHFromBytes(payloadMHBz)
		if err != nil {
			return nil, fmt.Errorf("scanning mulithash: %w", err)
		}

		pbs = append(pbs, pieceBlock{
			PieceCid:         pcid,
			PayloadMultihash: pmh,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}

	//log.Debug("got pbs:")
	//for _, pb := range pbs {
	//	log.Debugf("  %s %s", pb.PieceCid, pb.PayloadMultihash)
	//}

	return pbs, nil
}

func (c *CassandraDB) AddIndexRecords(ctx context.Context, pieceCid cid.Cid, recs []model.Record) error {
	if len(recs) == 0 {
		return nil
	}

	batchEntries := make([]gocql.BatchEntry, 0, 2*len(recs))

	// Add payload to pieces index
	for _, rec := range recs {
		batchEntries = append(batchEntries, gocql.BatchEntry{
			Stmt:       `INSERT INTO bench.PayloadToPieces (PayloadMultihash, PieceCids) VALUES (?, ?)`,
			Args:       []interface{}{rec.Cid.Hash(), pieceCid.Bytes()},
			Idempotent: true,
		})
	}

	// Add piece to block info index
	for _, rec := range recs {
		batchEntries = append(batchEntries, gocql.BatchEntry{
			Stmt:       `INSERT INTO bench.PieceBlockOffsetSize (PieceCid, PayloadMultihash, BlockOffset, BlockSize) VALUES (?, ?, ?, ?)`,
			Args:       []interface{}{pieceCid.Bytes(), rec.Cid.Hash(), rec.Offset, rec.Size},
			Idempotent: true,
		})
	}

	// Cassandra has a 50k limit on batch statements. Keeping batch size small
	// makes sure we're under the limit.
	const batchSize = 128
	var batch *gocql.Batch
	for allIdx, entry := range batchEntries {
		if batch == nil {
			batch = c.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx).RetryPolicy(&gocql.ExponentialBackoffRetryPolicy{Max: 120 * time.Second, NumRetries: 25})
		}

		batch.Entries = append(batch.Entries, entry)

		if allIdx == len(batchEntries)-1 || len(batch.Entries) == batchSize {
			var err error
			for i := 0; i < 30; i++ {
				err = c.session.ExecuteBatch(batch)
				if err == nil {
					break
				}

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
