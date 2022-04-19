package db

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

var ErrNotFound = errors.New("not found")

type Scannable interface {
	Scan(dest ...interface{}) error
}

func SqlDB(dbPath string) (*sql.DB, error) {
	return sql.Open("sqlite3", "file:"+dbPath)
}

//go:embed create_main_db.sql
var createMainDBSQL string

//go:embed create_logs_db.sql
var createLogsDBSQL string

func CreateAllBoostTables(ctx context.Context, mainDB *sql.DB, logsDB *sql.DB) error {
	if _, err := mainDB.ExecContext(ctx, createMainDBSQL); err != nil {
		return fmt.Errorf("failed to create tables in main DB: %w", err)
	}

	if _, err := logsDB.ExecContext(ctx, createLogsDBSQL); err != nil {
		return fmt.Errorf("failed to create tables in logs DB: %w", err)
	}
	return nil
}

func CreateTestTmpDB(t *testing.T) *sql.DB {
	f, err := ioutil.TempFile(t.TempDir(), "*.db")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	d, err := SqlDB(f.Name())
	require.NoError(t, err)
	return d
}
