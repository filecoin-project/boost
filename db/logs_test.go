package db

import (
	"context"
	"testing"
	"time"

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

func TestLogsDBCleanup(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	require.NoError(t, CreateAllBoostTables(ctx, sqldb, sqldb))

	ldb := NewLogsDB(sqldb)

	deals, err := GenerateDeals()
	req.NoError(err)
	deal1 := deals[0]
	deal2 := deals[1]

	err = ldb.InsertLog(ctx, &DealLog{DealUUID: deal1.DealUuid, CreatedAt: time.Now(), LogLevel: "INFO", LogParams: "params", LogMsg: "Test", Subsystem: "Sub"})
	req.NoError(err)

	err = ldb.InsertLog(ctx, &DealLog{DealUUID: deal2.DealUuid, CreatedAt: time.Now().AddDate(0, 0, -7), LogLevel: "INFO", LogParams: "params", LogMsg: "Test", Subsystem: "Sub"})
	req.NoError(err)

	logs, err := ldb.Logs(ctx, deal1.DealUuid)
	req.NoError(err)
	req.Len(logs, 1)

	logs, err = ldb.Logs(ctx, deal2.DealUuid)
	req.NoError(err)
	req.Len(logs, 1)

	// Delete logs older than 1 day
	err = ldb.CleanupLogs(ctx, 1)
	req.NoError(err)

	logs, err = ldb.Logs(ctx, deal1.DealUuid)
	req.NoError(err)
	req.Len(logs, 1)

	logs, err = ldb.Logs(ctx, deal2.DealUuid)
	req.NoError(err)
	req.Len(logs, 0)
}
