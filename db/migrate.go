package db

import (
	"database/sql"
	"embed"

	_ "github.com/filecoin-project/boost/db/migrations"
	logging "github.com/ipfs/go-log/v2"
	"github.com/pressly/goose/v3"
)

var log = logging.Logger("db")

//go:embed migrations/*.sql
var embedMigrations embed.FS

func Migrate(db *sql.DB) error {
	goose.SetBaseFS(embedMigrations)

	if err := goose.SetDialect("sqlite3"); err != nil {
		return err
	}

	beforeVer, err := goose.GetDBVersion(db)
	if err != nil {
		return err
	}

	if err := goose.Up(db, "migrations"); err != nil {
		return err
	}

	afterVer, err := goose.GetDBVersion(db)
	if err != nil {
		return err
	}

	if beforeVer != afterVer {
		log.Warnw("boost sqlite3 migrated", "previous", beforeVer, "current", afterVer)
	}

	return nil
}
