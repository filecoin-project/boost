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
	DealUuid  uuid.UUID
	CreatedAt time.Time
	Text      string
}

type FundsDB struct {
	db *sql.DB
}

func NewFundsDB(db *sql.DB) *FundsDB {
	return &FundsDB{db: db}
}

func (f *FundsDB) Tag(ctx context.Context, dealUuid uuid.UUID, collateral abi.TokenAmount, pubMsg abi.TokenAmount) interface{} {
	qry := "INSERT INTO FundsTagged (DealID, CreatedAt, Collateral, PubMsg) "
	qry += "VALUES (?, ?, ?, ?)"
	values := []interface{}{dealUuid, time.Now(), collateral.String(), pubMsg.String()}
	_, err := f.db.ExecContext(ctx, qry, values...)
	return err
}

func (f *FundsDB) Untag(ctx context.Context, dealUuid uuid.UUID) error {
	_, err := f.db.ExecContext(ctx, "DELETE FROM FundsTagged WHERE DealID = ?", dealUuid)
	return err
}

func (f *FundsDB) InsertLog(ctx context.Context, l *FundsLog) error {
	if l.CreatedAt.IsZero() {
		l.CreatedAt = time.Now()
	}

	qry := "INSERT INTO FundsLogs (DealID, CreatedAt, LogText) "
	qry += "VALUES (?, ?, ?)"
	values := []interface{}{l.DealUuid, l.CreatedAt, l.Text}
	_, err := f.db.ExecContext(ctx, qry, values...)
	return err
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
			return nil, fmt.Errorf("parsing Collateral '%s' to big.Int: %w", collatStr, err)
		}
	}

	var pubMsg big.Int
	if !pubMsgStr.Valid {
		pubMsg = big.NewInt(0)
	} else {
		pubMsg, err = big.FromString(pubMsgStr.String)
		if err != nil {
			return nil, fmt.Errorf("parsing PubMsg '%s' to big.Int: %w", pubMsgStr, err)
		}
	}

	return &TotalTagged{
		Collateral: collat,
		PubMsg:     pubMsg,
	}, nil
}
