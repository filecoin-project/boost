package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/google/uuid"
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

func (f *FundsDB) Untag(ctx context.Context, dealUuid uuid.UUID) (abi.TokenAmount, error) {
	qry := "SELECT Collateral, PubMsg FROM FundsTagged WHERE DealUUID = ?"
	row := f.db.QueryRowContext(ctx, qry, dealUuid)

	collat := &bigIntFieldDef{f: new(abi.TokenAmount)}
	pubMsg := &bigIntFieldDef{f: new(abi.TokenAmount)}
	err := row.Scan(&collat.marshalled, &pubMsg.marshalled)
	if err != nil {
		if err == sql.ErrNoRows {
			return abi.NewTokenAmount(0), ErrDealNotFound
		}
		return abi.NewTokenAmount(0), fmt.Errorf("getting untagged amount: %w", err)
	}
	err = collat.unmarshall()
	if err != nil {
		return abi.NewTokenAmount(0), fmt.Errorf("unmarshalling untagged Collateral")
	}
	err = pubMsg.unmarshall()
	if err != nil {
		return abi.NewTokenAmount(0), fmt.Errorf("unmarshalling untagged PubMsg")
	}

	_, err = f.db.ExecContext(ctx, "DELETE FROM FundsTagged WHERE DealUUID = ?", dealUuid)
	return big.Add(*collat.f, *pubMsg.f), err
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

func (f *FundsDB) Logs(ctx context.Context) ([]FundsLog, error) {
	qry := "SELECT DealUUID, CreatedAt, Amount, LogText FROM FundsLogs"
	rows, err := f.db.QueryContext(ctx, qry)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fundsLogs := make([]FundsLog, 0, 16)
	for rows.Next() {
		var fundsLog FundsLog
		amt := &bigIntFieldDef{f: &fundsLog.Amount}
		err := rows.Scan(
			&fundsLog.DealUUID,
			&fundsLog.CreatedAt,
			&amt.marshalled,
			&fundsLog.Text)
		if err != nil {
			return nil, fmt.Errorf("getting fund log: %w", err)
		}

		err = amt.unmarshall()
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

type TotalTagged struct {
	Collateral abi.TokenAmount
	PubMsg     abi.TokenAmount
}

func (f *FundsDB) TotalTagged(ctx context.Context) (*TotalTagged, error) {
	rows, err := f.db.QueryContext(ctx, "SELECT Collateral, PubMsg FROM FundsTagged")
	if err != nil {
		return nil, fmt.Errorf("getting total tagged: %w", err)
	}
	defer rows.Close()

	tt := &TotalTagged{
		Collateral: abi.NewTokenAmount(0),
		PubMsg:     abi.NewTokenAmount(0),
	}

	for rows.Next() {
		collat := &bigIntFieldDef{f: new(abi.TokenAmount)}
		pubMsg := &bigIntFieldDef{f: new(abi.TokenAmount)}
		err := rows.Scan(&collat.marshalled, &pubMsg.marshalled)
		if err != nil {
			return nil, fmt.Errorf("getting total tagged: %w", err)
		}

		err = collat.unmarshall()
		if err != nil {
			return nil, fmt.Errorf("unmarshalling untagged Collateral: %w", err)
		}
		if collat.f.Int != nil {
			tt.Collateral = big.Add(tt.Collateral, *collat.f)
		}

		err = pubMsg.unmarshall()
		if err != nil {
			return nil, fmt.Errorf("unmarshalling untagged PubMsg: %w", err)
		}
		if pubMsg.f.Int != nil {
			tt.PubMsg = big.Add(tt.PubMsg, *pubMsg.f)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("getting total tagged: %w", err)
	}

	return tt, nil
}
