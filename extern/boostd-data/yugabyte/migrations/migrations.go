package migrations

import (
	"database/sql"
	"embed"
	"github.com/filecoin-project/go-address"
	logging "github.com/ipfs/go-log/v2"
	"github.com/pressly/goose/v3"
)

var log = logging.Logger("migrations")

//go:embed *.go
var EmbedMigrations embed.FS

// Used to pass global parameters to the migration functions
type MigrateParams struct {
	MinerAddress address.Address
}

var migrationParams *MigrateParams

func Migrate(sqldb *sql.DB, params MigrateParams) error {
	migrationParams = &params

	goose.SetBaseFS(EmbedMigrations)

	if err := goose.SetDialect("postgres"); err != nil {
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
		log.Warnw("boost postgres migrated", "previous", beforeVer, "current", afterVer)
	}

	return nil
}
