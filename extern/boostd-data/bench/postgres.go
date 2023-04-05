package main

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"strings"

	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/shared/cliutil"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	_ "github.com/lib/pq"
	"github.com/multiformats/go-multihash"
	"github.com/urfave/cli/v2"
)

// The maximum number of parameters allowed in an insert query in PostgreSQL
const PSQLMaxParams = 65535

var createCmd = &cli.Command{
	Name:   "postgres-create",
	Before: before,
	Flags: append(commonFlags, &cli.StringFlag{
		Name:  "connect-string",
		Value: "postgresql://postgres:postgres@localhost?sslmode=disable",
	}),
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)
		db, err := NewPostgresDB(cctx.String("connect-string"))
		if err != nil {
			return err
		}

		log.Infof("Creating db...")
		if err := db.CreateDB(ctx); err != nil {
			return err
		}
		log.Infof("Created db")

		return nil
	},
}

var initCmd = &cli.Command{
	Name:   "postgres-init",
	Before: before,
	Flags: append(commonFlags, &cli.StringFlag{
		Name:  "connect-string",
		Value: "postgresql://postgres:postgres@localhost?sslmode=disable",
	}),
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)
		db, err := NewPostgresDB(cctx.String("connect-string"))
		if err != nil {
			return err
		}

		log.Infof("Initializing...")
		if err := db.Init(ctx); err != nil {
			return err
		}
		log.Infof("Initialized")

		return nil
	},
}

var dropCmd = &cli.Command{
	Name:   "postgres-drop",
	Before: before,
	Flags: append(commonFlags, &cli.StringFlag{
		Name:  "connect-string",
		Value: "postgresql://postgres:postgres@localhost?sslmode=disable",
	}),
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)
		db, err := NewPostgresDB(cctx.String("connect-string"))
		if err != nil {
			return err
		}

		_, _ = db.defDb.ExecContext(ctx, `DROP database bench`)

		return nil
	},
}

var postgresCmd = &cli.Command{
	Name:   "postgres",
	Before: before,
	Flags: append(commonFlags, &cli.StringFlag{
		Name:  "connect-string",
		Value: "postgresql://postgres:postgres@localhost?sslmode=disable",
	}),
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)
		db, err := NewPostgresDB(cctx.String("connect-string"))
		if err != nil {
			return err
		}

		err = db.connect(ctx)
		if err != nil {
			return err
		}

		return run(ctx, db, runOptsFromCctx(cctx))
	},
	Subcommands: []*cli.Command{
		//loadCmd(createPostgres),
		//bitswapCmd(createPostgres),
		//graphsyncCmd(createPostgres),
	},
}

func createPostgres(ctx context.Context, connectString string) (BenchDB, error) {
	db, err := NewPostgresDB(connectString)
	if err != nil {
		return nil, err
	}
	err = db.connect(ctx)
	if err != nil {
		return nil, err
	}
	return db, err
}

type Postgres struct {
	defDb         *sql.DB
	db            *sql.DB
	connectString string
}

func NewPostgresDB(connectString string) (*Postgres, error) {
	defDb, err := sql.Open("postgres", connectString)
	if err != nil {
		return nil, fmt.Errorf("connecting to default database: %w", err)
	}

	return &Postgres{defDb: defDb, connectString: connectString}, nil
}

func (db *Postgres) Name() string {
	return "Postgres DB"
}

//go:embed create_tables.sql
var createTables string

func (db *Postgres) CreateDB(ctx context.Context) error {
	_, err := db.defDb.ExecContext(ctx, `CREATE DATABASE bench`)
	if err != nil {
		return fmt.Errorf("creating database bench: %w", err)
	}

	return nil
}

func (db *Postgres) Init(ctx context.Context) error {
	err := db.connect(ctx)
	if err != nil {
		return fmt.Errorf("connecting to db: %w", err)
	}

	_, err = db.db.ExecContext(ctx, createTables)
	if err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}

	return nil
}

func (db *Postgres) connect(ctx context.Context) error {
	benchConnStr, err := getConnStringWithDb(db.connectString, "bench")
	if err != nil {
		return err
	}
	benchDb, err := sql.Open("postgres", benchConnStr)
	if err != nil {
		return fmt.Errorf("connecting to database Bench: %w", err)
	}
	db.db = benchDb

	return nil
}

func (db *Postgres) Cleanup(ctx context.Context) error {
	err := db.db.Close()
	if err != nil {
		return err
	}

	_, err = db.defDb.ExecContext(ctx, `DROP database bench`)
	return err
}

func (db *Postgres) GetBlockSample(ctx context.Context, count int) ([]pieceBlock, error) {
	qry := `SELECT PieceCid, PayloadMultihash FROM PieceBlockOffsetSize ORDER BY RANDOM() LIMIT $1`
	rows, err := db.db.QueryContext(ctx, qry, count)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pbs := make([]pieceBlock, 0, count)
	for rows.Next() {
		var pieceCidBz, payloadMHBz []byte
		err := rows.Scan(&pieceCidBz, &payloadMHBz)
		if err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

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
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return pbs, nil
}

func (db *Postgres) AddIndexRecords(ctx context.Context, pieceCid cid.Cid, recs []model.Record) error {
	if len(recs) == 0 {
		return nil
	}

	// Add payload to pieces index
	paramsPerRow := 2
	batchSize := (PSQLMaxParams - 1) / paramsPerRow
	for l := 0; l < len(recs); l += batchSize {
		start := l
		end := l + batchSize
		if end > len(recs) {
			end = len(recs)
		}
		chunk := recs[start:end]

		vals := ""
		args := make([]interface{}, 0, len(chunk)*paramsPerRow)
		for i, rec := range chunk {
			if i > 0 {
				vals = vals + ","
			}
			vals = vals + fmt.Sprintf("($%d,$%d)", (i*paramsPerRow)+1, (i*paramsPerRow)+2)
			args = append(args, rec.Cid.Hash(), pieceCid.Bytes())
		}
		_, err := db.db.ExecContext(ctx,
			`INSERT INTO PayloadToPieces (PayloadMultihash, PieceCids) VALUES `+vals+" ON CONFLICT DO NOTHING", args...)
		if err != nil {
			return fmt.Errorf("executing insert: %w", err)
		}
	}

	// Add piece to block info index
	paramsPerRow = 4
	batchSize = (PSQLMaxParams - 1) / paramsPerRow
	for l := 0; l < len(recs); l += batchSize {
		start := l
		end := l + batchSize
		if end > len(recs) {
			end = len(recs)
		}
		chunk := recs[start:end]

		vals := ""
		args := make([]interface{}, 0, len(chunk)*paramsPerRow)
		for i, rec := range chunk {
			if i > 0 {
				vals = vals + ","
			}
			vals = vals + fmt.Sprintf("($%d,$%d,$%d,$%d)", (i*paramsPerRow)+1, (i*paramsPerRow)+2, (i*paramsPerRow)+3, (i*paramsPerRow)+4)
			args = append(args, pieceCid.Bytes(), rec.Cid.Hash(), rec.Offset, rec.Size)
		}
		_, err := db.db.ExecContext(ctx,
			`INSERT INTO PieceBlockOffsetSize (PieceCid, PayloadMultihash, BlockOffset, BlockSize) VALUES `+vals+" ON CONFLICT DO NOTHING", args...)
		if err != nil {
			return fmt.Errorf("executing insert: %w", err)
		}
	}

	return nil
}

func (db *Postgres) PiecesContainingMultihash(ctx context.Context, m multihash.Multihash) ([]cid.Cid, error) {
	var bz []byte
	qry := `SELECT PieceCids FROM PayloadToPieces WHERE PayloadMultihash = $1`
	err := db.db.QueryRowContext(ctx, qry, m).Scan(&bz)
	if err != nil {
		return nil, err
	}

	return cidsFromBytes(bz)
}

func (db *Postgres) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash multihash.Multihash) (*model.OffsetSize, error) {
	var offset, size uint64
	qry := `SELECT BlockOffset, BlockSize FROM PieceBlockOffsetSize WHERE PieceCid = $1 AND PayloadMultihash = $2`
	err := db.db.QueryRowContext(ctx, qry, pieceCid.Bytes(), hash).Scan(&offset, &size)
	if err != nil {
		return nil, err
	}

	return &model.OffsetSize{Offset: offset, Size: size}, nil
}

func (db *Postgres) GetIterableIndex(ctx context.Context, pieceCid cid.Cid) (index.IterableIndex, error) {
	qry := `SELECT PayloadMultihash, BlockOffset FROM PieceBlockOffsetSize WHERE PieceCid = $1`
	rows, err := db.db.QueryContext(ctx, qry, pieceCid.Bytes())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []index.Record
	for rows.Next() {
		var payloadMHBz []byte
		var offset uint64
		err := rows.Scan(&payloadMHBz, &offset)
		if err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		_, pmh, err := multihash.MHFromBytes(payloadMHBz)
		if err != nil {
			return nil, fmt.Errorf("scanning mulithash: %w", err)
		}

		records = append(records, index.Record{
			Cid:    cid.NewCidV1(cid.Raw, pmh),
			Offset: offset,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	mis := make(index.MultihashIndexSorted)
	err = mis.Load(records)
	if err != nil {
		return nil, err
	}

	return &mis, nil
}

func getConnStringWithDb(connString string, dbName string) (string, error) {
	// "postgresql://postgres:postgres@localhost/dbname?sslmode=disable"
	prefixEnd := strings.Index(connString, "://")
	restStart := prefixEnd + 3
	if prefixEnd == -1 || len(connString) <= restStart {
		return "", fmt.Errorf("connect string %s is missing protocol prefix", connString)
	}

	rest := connString[restStart:]
	slashIdx := strings.Index(rest, "/")
	questionIdx := strings.Index(rest, "?")
	if questionIdx != -1 {
		query := rest[questionIdx:]
		if slashIdx != -1 {
			return connString[:restStart] + rest[:slashIdx] + "/" + dbName + query, nil
		}
		return connString[:restStart] + rest[:questionIdx] + "/" + dbName + query, nil
	} else if slashIdx != -1 {
		return connString[:restStart] + rest[:slashIdx] + "/" + dbName, nil
	}

	return connString + "/" + dbName, nil
}
