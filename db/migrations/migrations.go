package migrations

import (
	"database/sql"
	"embed"

	logging "github.com/ipfs/go-log/v2"
	"github.com/pressly/goose/v3"
)

var log = logging.Logger("migrations")

//go:embed *.sql *.go
var EmbedMigrations embed.FS

func Migrate(sqldb *sql.DB) error {
	goose.SetBaseFS(EmbedMigrations)

	if err := goose.SetDialect("sqlite3"); err != nil {
		return err
	}

	beforeVer, err := goose.GetDBVersion(sqldb)
	if err != nil {
		return err
	}

	if err := goose.Up(sqldb, "."); err != nil {
		return err
	}

	afterVer, err := goose.GetDBVersion(sqldb)
	if err != nil {
		return err
	}

	if beforeVer != afterVer {
		log.Warnw("boost sqlite3 migrated", "previous", beforeVer, "current", afterVer)
	}

	return nil
}
