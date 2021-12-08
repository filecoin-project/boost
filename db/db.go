package db

import (
	"context"
	"database/sql"
	"errors"
	"io/ioutil"
	"path"
	"runtime"
)

var ErrNotFound = errors.New("not found")

type Scannable interface {
	Scan(dest ...interface{}) error
}

func SqlDB(dbPath string) (*sql.DB, error) {
	return sql.Open("sqlite3", "file:"+dbPath)
}

func CreateTmpDB(ctx context.Context) (*sql.DB, error) {
	sqldb, err := SqlDB("test.db?cache=shared&mode=memory")
	if err != nil {
		return nil, nil
	}

	return sqldb, CreateTables(ctx, sqldb)
}

func CreateTables(ctx context.Context, db *sql.DB) error {
	_, filename, _, _ := runtime.Caller(0)
	createPath := path.Join(path.Dir(filename), "/create.sql")
	createScript, err := ioutil.ReadFile(createPath)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, string(createScript))
	return err
}
