package db

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"

	"github.com/stretchr/testify/require"
)

func TestStorageDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb, err := CreateTmpDB(ctx)
	req.NoError(err)

	db := NewStorageDB(sqldb)
	req.NoError(err)

	tt, err := db.TotalTagged(ctx)
	req.NoError(err)
	req.Equal(abi.PaddedPieceSize(0), tt)

	dealUUID := uuid.New()
	amt, err := db.Untag(ctx, dealUUID)
	req.NoError(err)
	req.Equal(abi.PaddedPieceSize(0), amt)

	err = db.Tag(ctx, dealUUID, abi.PaddedPieceSize(1111))
	req.NoError(err)

	total, err := db.TotalTagged(ctx)
	req.NoError(err)
	req.Equal(abi.PaddedPieceSize(1111), total)

	amt, err = db.Untag(ctx, dealUUID)
	req.NoError(err)
	req.Equal(abi.PaddedPieceSize(1111), amt)

	fl := &StorageLog{
		DealUUID:  dealUUID,
		PieceSize: abi.PaddedPieceSize(1234),
		Text:      "Hello",
	}
	err = db.InsertLog(ctx, fl)
	req.NoError(err)

	logs, err := db.Logs(ctx)
	req.NoError(err)
	req.Len(logs, 1)
	req.Equal(fl.DealUUID, logs[0].DealUUID)
	req.Equal(fl.PieceSize, logs[0].PieceSize)
	req.Equal(fl.Text, logs[0].Text)
}
