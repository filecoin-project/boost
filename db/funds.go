package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/db/fielddef"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/google/uuid"
	"github.com/mattn/go-sqlite3"
)

type FundsLog struct {
	DealUUID  uuid.UUID
	CreatedAt time.Time
	Amount    big.Int
	Text      string
}

type FundsDB struct {
	db *sql.DB
}

func NewFundsDB(db *sql.DB) *FundsDB {
	return &FundsDB{db: db}
}

func (f *FundsDB) Tag(ctx context.Context, dealUuid uuid.UUID, collateral abi.TokenAmount, pubMsg abi.TokenAmount) error {
	qry := "INSERT INTO FundsTagged (DealUUID, CreatedAt, Collateral, PubMsg) "
	qry += "VALUES (?, ?, ?, ?)"
	values := []interface{}{dealUuid, time.Now(), collateral.String(), pubMsg.String()}
	_, err := f.db.ExecContext(ctx, qry, values...)
	return err
}

func (f *FundsDB) Untag(ctx context.Context, dealUuid uuid.UUID) (clt abi.TokenAmount, pub abi.TokenAmount, e error) {
	qry := "SELECT Collateral, PubMsg FROM FundsTagged WHERE DealUUID = ?"
	row := f.db.QueryRowContext(ctx, qry, dealUuid)

	collat := &fielddef.BigIntFieldDef{F: new(abi.TokenAmount)}
	pubMsg := &fielddef.BigIntFieldDef{F: new(abi.TokenAmount)}
	err := row.Scan(&collat.Marshalled, &pubMsg.Marshalled)
	if err != nil {
		if err == sql.ErrNoRows {
			return abi.NewTokenAmount(0), abi.NewTokenAmount(0), ErrNotFound
		}
		return abi.NewTokenAmount(0), abi.NewTokenAmount(0), fmt.Errorf("getting untagged amount: %w", err)
	}
	err = collat.Unmarshall()
	if err != nil {
		return abi.NewTokenAmount(0), abi.NewTokenAmount(0), fmt.Errorf("unmarshalling untagged Collateral")
	}
	err = pubMsg.Unmarshall()
	if err != nil {
		return abi.NewTokenAmount(0), abi.NewTokenAmount(0), fmt.Errorf("unmarshalling untagged PubMsg")
	}

	_, err = f.db.ExecContext(ctx, "DELETE FROM FundsTagged WHERE DealUUID = ?", dealUuid)
	return *collat.F, *pubMsg.F, err
}

func (f *FundsDB) InsertLog(ctx context.Context, logs ...*FundsLog) error {
	now := time.Now()
	for _, l := range logs {
		if l.CreatedAt.IsZero() {
			l.CreatedAt = now
		}

		qry := "INSERT INTO FundsLogs (DealUUID, CreatedAt, Amount, LogText) "
		qry += "VALUES (?, ?, ?, ?)"
		values := []interface{}{l.DealUUID, l.CreatedAt, l.Amount.String(), l.Text}
		_, err := f.db.ExecContext(ctx, qry, values...)
		if err != nil {
			return fmt.Errorf("inserting funds log: %w", err)
		}
	}

	return nil
}

func (f *FundsDB) Logs(ctx context.Context, cursor *time.Time, offset int, limit int) ([]FundsLog, error) {
	qry := "SELECT DealUUID, CreatedAt, Amount, LogText FROM FundsLogs"
	args := []interface{}{}
	if cursor != nil {
		qry += " WHERE CreatedAt <= ?"
		args = append(args, cursor.Format(sqlite3.SQLiteTimestampFormats[0]))
	}

	qry += " ORDER BY CreatedAt DESC, RowID"

	if limit > 0 {
		qry += " LIMIT ?"
		args = append(args, limit)

		if offset > 0 {
			qry += " OFFSET ?"
			args = append(args, offset)
		}
	}

	rows, err := f.db.QueryContext(ctx, qry, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	fundsLogs := make([]FundsLog, 0, 16)
	for rows.Next() {
		var fundsLog FundsLog
		amt := &fielddef.BigIntFieldDef{F: &fundsLog.Amount}
		err := rows.Scan(
			&fundsLog.DealUUID,
			&fundsLog.CreatedAt,
			&amt.Marshalled,
			&fundsLog.Text)
		if err != nil {
			return nil, fmt.Errorf("getting fund log: %w", err)
		}

		err = amt.Unmarshall()
		if err != nil {
			return nil, fmt.Errorf("unmarshalling fund log Amount: %w", err)
		}

		fundsLogs = append(fundsLogs, fundsLog)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return fundsLogs, nil
}

func (f *FundsDB) LogsCount(ctx context.Context) (int, error) {
	var count int
	row := f.db.QueryRowContext(ctx, "SELECT count(*) FROM FundsLogs")
	err := row.Scan(&count)
	return count, err
}

type TotalTagged struct {
	Collateral abi.TokenAmount
	PubMsg     abi.TokenAmount
}

func (f *FundsDB) TotalTagged(ctx context.Context) (*TotalTagged, error) {
	rows, err := f.db.QueryContext(ctx, "SELECT Collateral, PubMsg FROM FundsTagged")
	if err != nil {
		return nil, fmt.Errorf("getting total tagged: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	tt := &TotalTagged{
		Collateral: abi.NewTokenAmount(0),
		PubMsg:     abi.NewTokenAmount(0),
	}

	for rows.Next() {
		collat := &fielddef.BigIntFieldDef{F: new(abi.TokenAmount)}
		pubMsg := &fielddef.BigIntFieldDef{F: new(abi.TokenAmount)}
		err := rows.Scan(&collat.Marshalled, &pubMsg.Marshalled)
		if err != nil {
			return nil, fmt.Errorf("getting total tagged: %w", err)
		}

		err = collat.Unmarshall()
		if err != nil {
			return nil, fmt.Errorf("unmarshalling untagged Collateral: %w", err)
		}
		if collat.F.Int != nil {
			tt.Collateral = big.Add(tt.Collateral, *collat.F)
		}

		err = pubMsg.Unmarshall()
		if err != nil {
			return nil, fmt.Errorf("unmarshalling untagged PubMsg: %w", err)
		}
		if pubMsg.F.Int != nil {
			tt.PubMsg = big.Add(tt.PubMsg, *pubMsg.F)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getting total tagged: %w", err)
	}

	return tt, nil
}

func (f *FundsDB) CleanupLogs(ctx context.Context, daysOld int) error {

	t := time.Now()
	td := t.AddDate(0, 0, -1*daysOld)

	qry := "DELETE from FundsLogs WHERE DealUUID IN (SELECT DISTINCT DealUUID FROM FundsLogs WHERE CreatedAt < ?)"

	_, err := f.db.ExecContext(ctx, qry, td)
	return err
}
