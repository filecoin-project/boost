package db

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

const DealsDBName = "boost.db"
const LogsDBName = "boost.logs.db"

var ErrNotFound = errors.New("not found")

type Scannable interface {
	Scan(dest ...interface{}) error
}

func SqlDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "file:"+dbPath)
	if err == nil {
		// fixes error "database is locked", caused by concurrent access from deal goroutines to a single sqlite3 db connection
		// see: https://github.com/mattn/go-sqlite3#:~:text=Error%3A%20database%20is%20locked
		// see: https://github.com/filecoin-project/boost/pull/657
		db.SetMaxOpenConns(1)
	}

	return db, err
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
	f, err := os.CreateTemp(t.TempDir(), "*.db")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	d, err := SqlDB(f.Name())
	require.NoError(t, err)
	return d
}

func SqlBackup(ctx context.Context, srcDB *sql.DB, dstDir, dbFileName string) error {
	dbPath := path.Join(dstDir, dbFileName+"?cache=shared")
	dstDB, err := SqlDB(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open source sql db for backup: %w", err)
	}

	defer func() {
		_ = dstDB.Close()
	}()

	err = dstDB.Ping()
	if err != nil {
		return fmt.Errorf("failed to open destination sql db for backup: %w", err)
	}

	destConn, err := dstDB.Conn(ctx)
	if err != nil {
		return err
	}

	// Conn must be closed explicitly otherwise, DB conn hangs
	defer func() {
		_ = destConn.Close()
	}()

	srcConn, err := srcDB.Conn(ctx)
	if err != nil {
		return err
	}

	// Conn must be closed explicitly otherwise, DB conn hangs
	defer func() {
		_ = srcConn.Close()
	}()

	return destConn.Raw(func(destConn interface{}) error {
		return srcConn.Raw(func(srcConn interface{}) error {
			destSQLiteConn, ok := destConn.(*sqlite3.SQLiteConn)
			if !ok {
				return fmt.Errorf("can't convert destination connection to SQLiteConn")
			}

			srcSQLiteConn, ok := srcConn.(*sqlite3.SQLiteConn)
			if !ok {
				return fmt.Errorf("can't convert source connection to SQLiteConn")
			}

			b, err := destSQLiteConn.Backup("main", srcSQLiteConn, "main")
			if err != nil {
				return fmt.Errorf("error initializing SQLite backup: %w", err)
			}

			// Allow the initial page count and remaining values to be retrieved
			isDone, err := b.Step(0)
			if err != nil {
				return fmt.Errorf("unable to perform an initial 0-page backup step: %w", err)
			}
			if isDone {
				return fmt.Errorf("backup is unexpectedly done")
			}

			// Check that the page count and remaining values are reasonable.
			initialPageCount := b.PageCount()
			if initialPageCount <= 0 {
				return fmt.Errorf("unexpected initial page count value: %v", initialPageCount)
			}
			initialRemaining := b.Remaining()
			if initialRemaining <= 0 {
				return fmt.Errorf("unexpected initial remaining value: %v", initialRemaining)
			}
			if initialRemaining != initialPageCount {
				return fmt.Errorf("initial remaining value %v differs from the initial page count value %v", initialRemaining, initialPageCount)
			}

			// Copy all the pages
			isDone, err = b.Step(-1)
			if err != nil {
				return fmt.Errorf("failed to perform a backup step: %w", err)
			}
			if !isDone {
				return fmt.Errorf("backup is unexpectedly not done")
			}

			// Check that the page count and remaining values are reasonable.
			finalPageCount := b.PageCount()
			if finalPageCount != initialPageCount {
				return fmt.Errorf("final page count %v differs from the initial page count %v", initialPageCount, finalPageCount)
			}
			finalRemaining := b.Remaining()
			if finalRemaining != 0 {
				return fmt.Errorf("unexpected remaining value: %v", finalRemaining)
			}

			err = b.Finish()
			if err != nil {
				return fmt.Errorf("error finishing backup: %w", err)
			}

			return err
		})
	})
}
