package db

import (
	"context"
	"database/sql"
	"github.com/ipfs/go-cid"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"time"
)

type Deal struct {
	CreatedAt time.Time
	PieceCid cid.Cid
	PieceSize uint64
	StartEpoch int64
	EndEpoch int64
}

type DealsDB struct {
	db *sql.DB
}

func Open(dbPath string) (*DealsDB, error) {
	db, err := sql.Open("sqlite3", "file:" + dbPath)
	if err != nil {
		return nil, err
	}
	return &DealsDB{db: db}, nil
}

func (d *DealsDB) Query(ctx context.Context) ([]Deal, error) {
	qry := "SELECT CreatedAt, PieceCid, PieceSize, StartEpoch, EndEpoch FROM Deals"
	rows, err := d.db.QueryContext(ctx, qry)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deals := make([]Deal, 0, 16)
	for rows.Next() {
		var deal Deal
		var pieceCid string
		if err := rows.Scan(&deal.CreatedAt, &pieceCid, &deal.PieceSize, &deal.StartEpoch, &deal.EndEpoch); err != nil {
			return nil, err
		}
		deal.PieceCid, err = cid.Parse(pieceCid)
		if err != nil {
			return nil, err
		}
		deals = append(deals, deal)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return deals, nil
}

func LoadFixtures(ctx context.Context, dbPath string, create string, fixtures string) error {
	db, err := sql.Open("sqlite3", "file:" + dbPath)
	if err != nil {
		return err
	}

	defer db.Close()

	createScript, err := ioutil.ReadFile(create)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, string(createScript))
	if err != nil {
		return err
	}

	fixturesScript, err := ioutil.ReadFile(fixtures)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, string(fixturesScript))
	return err
}
