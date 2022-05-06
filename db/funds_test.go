package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"

	"github.com/stretchr/testify/require"
)

func TestFundsDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	require.NoError(t, CreateAllBoostTables(ctx, sqldb, sqldb))

	db := NewFundsDB(sqldb)
	tt, err := db.TotalTagged(ctx)
	req.NoError(err)
	req.Equal(int64(0), tt.PubMsg.Int64())
	req.Equal(int64(0), tt.Collateral.Int64())

	dealUUID := uuid.New()
	collat, pub, err := db.Untag(ctx, dealUUID)
	req.True(errors.Is(err, ErrNotFound))
	req.Equal(int64(0), collat.Int64())
	req.True(pub.IsZero())

	err = db.Tag(ctx, dealUUID, abi.NewTokenAmount(1111), abi.NewTokenAmount(2222))
	req.NoError(err)

	tt, err = db.TotalTagged(ctx)
	req.NoError(err)
	req.Equal(int64(1111), tt.Collateral.Int64())
	req.Equal(int64(2222), tt.PubMsg.Int64())

	collat, pub, err = db.Untag(ctx, dealUUID)
	req.NoError(err)
	req.Equal(int64(1111), collat.Int64())
	req.Equal(int64(2222), pub.Int64())

	fl := &FundsLog{
		DealUUID: dealUUID,
		Amount:   abi.NewTokenAmount(1234),
		Text:     "Hello",
	}
	err = db.InsertLog(ctx, fl)
	req.NoError(err)

	time.Sleep(time.Millisecond)

	fl2 := &FundsLog{
		DealUUID: uuid.New(),
		Amount:   abi.NewTokenAmount(4567),
		Text:     "Goodbye",
	}
	err = db.InsertLog(ctx, fl2)
	req.NoError(err)

	count, err := db.LogsCount(ctx)
	req.NoError(err)
	req.Equal(count, 2)

	logs, err := db.Logs(ctx, nil, 0, 0)
	req.NoError(err)
	req.Len(logs, 2)

	// Expect most recently created log first
	req.Equal(fl2.DealUUID, logs[0].DealUUID)
	req.Equal(fl2.Amount, logs[0].Amount)
	req.Equal(fl2.Text, logs[0].Text)

	// Then expect older log
	req.Equal(fl.DealUUID, logs[1].DealUUID)
	req.Equal(fl.Amount, logs[1].Amount)
	req.Equal(fl.Text, logs[1].Text)

	newest := logs[0]
	oldest := logs[1]

	// Get all logs from newest to oldest
	logs, err = db.Logs(ctx, &newest.CreatedAt, 0, 0)
	req.NoError(err)
	req.Len(logs, 2)

	// Get all logs starting at oldest
	logs, err = db.Logs(ctx, &oldest.CreatedAt, 0, 0)
	req.NoError(err)
	req.Len(logs, 1)
	req.Equal(oldest.DealUUID, logs[0].DealUUID)

	// Get all logs from newest with limit 1
	logs, err = db.Logs(ctx, &newest.CreatedAt, 0, 1)
	req.NoError(err)
	req.Len(logs, 1)
	req.Equal(newest.DealUUID, logs[0].DealUUID)

	// Get all logs from newest with offset 1, limit 1
	logs, err = db.Logs(ctx, &newest.CreatedAt, 1, 1)
	req.NoError(err)
	req.Len(logs, 1)
	req.Equal(oldest.DealUUID, logs[0].DealUUID)
}
