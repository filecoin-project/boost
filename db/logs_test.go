package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogsDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	require.NoError(t, CreateAllBoostTables(ctx, sqldb, sqldb))

	ldb := NewLogsDB(sqldb)

	deals, err := GenerateDeals()
	req.NoError(err)
	deal := deals[0]

	err = ldb.InsertLog(ctx, &DealLog{DealUUID: deal.DealUuid, LogLevel: "INFO", LogParams: "params", LogMsg: "Test", Subsystem: "Sub"})
	req.NoError(err)

	logs, err := ldb.Logs(ctx, deal.DealUuid)
	req.NoError(err)
	req.Len(logs, 1)
	req.Equal("Test", logs[0].LogMsg)
	req.Equal("params", logs[0].LogParams)
	req.Equal("INFO", logs[0].LogLevel)
	req.Equal("Sub", logs[0].Subsystem)
}
