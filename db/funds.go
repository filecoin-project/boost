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

func (f *FundsDB) Tag(ctx context.Context, dealUuid uuid.UUID, collateral abi.TokenAmount, pubMsg abi.TokenAmount) interface{} {
	qry := "INSERT INTO FundsTagged (DealUUID, CreatedAt, Collateral, PubMsg) "
	qry += "VALUES (?, ?, ?, ?)"
	values := []interface{}{dealUuid, time.Now(), collateral.String(), pubMsg.String()}
	_, err := f.db.ExecContext(ctx, qry, values...)
	return err
}

func (f *FundsDB) Untag(ctx context.Context, dealUuid uuid.UUID) (abi.TokenAmount, error) {
	qry := "SELECT SUM(Collateral) + SUM(PubMsg) FROM FundsTagged WHERE DealUUID = ?"
	row := f.db.QueryRowContext(ctx, qry, dealUuid)

	amt := abi.NewTokenAmount(0)
	amtFD := &bigIntFieldDef{f: &amt}

	err := row.Scan(&amtFD.marshalled)
	if err != nil {
		return abi.NewTokenAmount(0), fmt.Errorf("unmarshalling untagged amount: %w", err)
	}

	_, err = f.db.ExecContext(ctx, "DELETE FROM FundsTagged WHERE DealUUID = ?", dealUuid)
	return amt, err
}

func (f *FundsDB) InsertLog(ctx context.Context, logs ...*FundsLog) error {
	now := time.Now()
	for _, l := range logs {
		if l.CreatedAt.IsZero() {
			l.CreatedAt = now
		}

		amtFD := &bigIntFieldDef{f: &l.Amount}
		amt, err := amtFD.marshall()
		if err != nil {
			return fmt.Errorf("marshalling fund log Amount %d: %w", l.Amount, err)
		}

		qry := "INSERT INTO FundsLogs (DealUUID, CreatedAt, Amount, LogText) "
		qry += "VALUES (?, ?, ?, ?)"
		values := []interface{}{l.DealUUID, l.CreatedAt, amt, l.Text}
		_, err = f.db.ExecContext(ctx, qry, values...)
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
		fundsLog.Amount = abi.NewTokenAmount(0)
		amtFD := &bigIntFieldDef{f: &fundsLog.Amount}
		err := rows.Scan(
			&fundsLog.DealUUID,
			&fundsLog.CreatedAt,
			&amtFD.marshalled,
			&fundsLog.Text)

		if err != nil {
			return nil, err
		}

		err = amtFD.unmarshall()
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
	var collatStr sql.NullString
	var pubMsgStr sql.NullString
	row := f.db.QueryRowContext(ctx, "SELECT sum(Collateral), sum(PubMsg) FROM FundsTagged")
	err := row.Scan(&collatStr, &pubMsgStr)
	if err != nil {
		return nil, err
	}

	var collat big.Int
	if !collatStr.Valid {
		collat = big.NewInt(0)
	} else {
		collat, err = big.FromString(collatStr.String)
		if err != nil {
			return nil, fmt.Errorf("parsing Collateral '%s' to big.Int: %w", collatStr.String, err)
		}
	}

	var pubMsg big.Int
	if !pubMsgStr.Valid {
		pubMsg = big.NewInt(0)
	} else {
		pubMsg, err = big.FromString(pubMsgStr.String)
		if err != nil {
			return nil, fmt.Errorf("parsing PubMsg '%s' to big.Int: %w", pubMsgStr.String, err)
		}
	}

	return &TotalTagged{
		Collateral: collat,
		PubMsg:     pubMsg,
	}, nil
}
