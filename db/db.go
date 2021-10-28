package db

import (
	"context"
	"database/sql"
	"io/ioutil"
	"os"
	"time"

	"github.com/ipfs/go-cid"
	_ "github.com/mattn/go-sqlite3"
)

type Scannable interface {
	Scan(dest ...interface{}) error
}

type DealLog struct {
	DealID    string
	CreatedAt time.Time
	Text      string
}

type Deal struct {
	ID                 string
	CreatedAt          time.Time
	PieceCid           cid.Cid
	PieceSize          uint64
	StartEpoch         int64
	EndEpoch           int64
	ProviderCollateral uint64
	Client             string
	State              string
}

var dealFields = "ID, CreatedAt, PieceCid, PieceSize, StartEpoch, EndEpoch, ProviderCollateral, Client, State"

type DealsDB struct {
	db *sql.DB
}

func Open(dbPath string) (*DealsDB, error) {
	db, err := sql.Open("sqlite3", "file:"+dbPath)
	if err != nil {
		return nil, err
	}
	return &DealsDB{db: db}, nil
}

func (d *DealsDB) ByID(id string) (*Deal, error) {
	qry := "SELECT " + dealFields + " FROM Deals WHERE id=?"
	row := d.db.QueryRow(qry, id)
	return d.scanRow(row)
}

func (d *DealsDB) List(ctx context.Context) ([]*Deal, error) {
	qry := "SELECT " + dealFields + " FROM Deals"
	rows, err := d.db.QueryContext(ctx, qry)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deals := make([]*Deal, 0, 16)
	for rows.Next() {
		deal, err := d.scanRow(rows)
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

func (d *DealsDB) scanRow(row Scannable) (*Deal, error) {
	var deal Deal
	var pieceCid string
	err := row.Scan(
		&deal.ID,
		&deal.CreatedAt,
		&pieceCid,
		&deal.PieceSize,
		&deal.StartEpoch,
		&deal.EndEpoch,
		&deal.ProviderCollateral,
		&deal.Client,
		&deal.State)
	if err != nil {
		// TODO: return ErrNotFound if not found in DB
		return nil, err
	}
	deal.PieceCid, err = cid.Parse(pieceCid)
	if err != nil {
		return nil, err
	}

	return &deal, err
}

func (d *DealsDB) Logs(ctx context.Context, dealID string) ([]DealLog, error) {
	qry := "SELECT DealID, CreatedAt, LogText FROM DealLogs WHERE DealID=?"
	rows, err := d.db.QueryContext(ctx, qry, dealID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dealLogs := make([]DealLog, 0, 16)
	for rows.Next() {
		var dealLog DealLog
		err := rows.Scan(
			&dealLog.DealID,
			&dealLog.CreatedAt,
			&dealLog.Text)

		if err != nil {
			return nil, err
		}
		dealLogs = append(dealLogs, dealLog)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return dealLogs, nil
}

func LoadFixtures(ctx context.Context, dbPath string, create string, fixtures string) error {
	db, err := sql.Open("sqlite3", "file:"+dbPath)
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

func CreateTmpDB() (string, error) {
	tmpFile, err := os.CreateTemp("", "test.db")
	if err != nil {
		return "", err
	}
	tmpFilePath := tmpFile.Name()

	ctx := context.Background()
	err = LoadFixtures(ctx, tmpFilePath, "db/create.sql", "db/fixtures.sql")

	return tmpFilePath, err
}
