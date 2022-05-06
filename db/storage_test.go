package db

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"
)

func TestStorageDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	require.NoError(t, CreateAllBoostTables(ctx, sqldb, sqldb))

	db := NewStorageDB(sqldb)

	tt, err := db.TotalTagged(ctx)
	req.NoError(err)
	req.Equal(uint64(0), tt)

	dealUUID := uuid.New()
	amt, err := db.Untag(ctx, dealUUID)
	req.True(errors.Is(err, ErrNotFound))
	req.Equal(uint64(0), amt)

	err = db.Tag(ctx, dealUUID, 1111)
	req.NoError(err)

	total, err := db.TotalTagged(ctx)
	req.NoError(err)
	req.Equal(uint64(1111), total)

	amt, err = db.Untag(ctx, dealUUID)
	req.NoError(err)
	req.Equal(uint64(1111), amt)

	fl := &StorageLog{
		DealUUID:     dealUUID,
		TransferSize: uint64(1234),
		Text:         "Hello",
	}
	err = db.InsertLog(ctx, fl)
	req.NoError(err)

	logs, err := db.Logs(ctx)
	req.NoError(err)
	req.Len(logs, 1)
	req.Equal(fl.DealUUID, logs[0].DealUUID)
	req.Equal(fl.TransferSize, logs[0].TransferSize)
	req.Equal(fl.Text, logs[0].Text)
}
