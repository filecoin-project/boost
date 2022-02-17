package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"path"
	"runtime"
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

func CreateAllBoostTables(ctx context.Context, mainDB *sql.DB, logsDB *sql.DB) error {
	if err := createTables(ctx, mainDB, "/create_main_db.sql"); err != nil {
		return fmt.Errorf("failed to create tables in main DB: %w", err)
	}

	if err := createTables(ctx, logsDB, "/create_logs_db.sql"); err != nil {
		return fmt.Errorf("failed to create tables in logs DB: %w", err)
	}
	return nil
}

func createTables(ctx context.Context, db *sql.DB, file string) error {
	_, filename, _, _ := runtime.Caller(0)
	createPath := path.Join(path.Dir(filename), file)
	createScript, err := ioutil.ReadFile(createPath)
	if err != nil {
		return fmt.Errorf("failed to read create file for db: %w", err)
	}
	_, err = db.ExecContext(ctx, string(createScript))
	if err != nil {
		return fmt.Errorf("failed to create DB: %w", err)
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
