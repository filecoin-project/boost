package db

import (
	"context"
	"testing"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"

	"github.com/stretchr/testify/require"
)

func TestFundsDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb, err := CreateTmpDB(ctx)
	req.NoError(err)

	db := NewFundsDB(sqldb)
	req.NoError(err)

	tt, err := db.TotalTagged(ctx)
	req.NoError(err)
	req.Equal(int64(0), tt.PubMsg.Int64())
	req.Equal(int64(0), tt.Collateral.Int64())

	dealUUID := uuid.New()
	amt, err := db.Untag(ctx, dealUUID)
	req.True(xerrors.Is(err, ErrNotFound))
	req.Equal(int64(0), amt.Int64())

	err = db.Tag(ctx, dealUUID, abi.NewTokenAmount(1111), abi.NewTokenAmount(2222))
	req.NoError(err)

	tt, err = db.TotalTagged(ctx)
	req.NoError(err)
	req.Equal(int64(1111), tt.Collateral.Int64())
	req.Equal(int64(2222), tt.PubMsg.Int64())

	amt, err = db.Untag(ctx, dealUUID)
	req.NoError(err)
	req.Equal(int64(3333), amt.Int64())

	fl := &FundsLog{
		DealUUID: dealUUID,
		Amount:   abi.NewTokenAmount(1234),
		Text:     "Hello",
	}
	err = db.InsertLog(ctx, fl)
	req.NoError(err)

	logs, err := db.Logs(ctx)
	req.NoError(err)
	req.Len(logs, 1)
	req.Equal(fl.DealUUID, logs[0].DealUUID)
	req.Equal(fl.Amount, logs[0].Amount)
	req.Equal(fl.Text, logs[0].Text)
}
