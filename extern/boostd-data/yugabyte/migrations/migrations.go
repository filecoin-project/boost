package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"github.com/filecoin-project/go-address"
	logging "github.com/ipfs/go-log/v2"
	"github.com/pressly/goose/v3"
	"time"
)

var log = logging.Logger("migrations")

//go:embed *.sql *.go
var EmbedMigrations embed.FS

type DBSettings struct {
	// The cassandra hosts to connect to
	Hosts []string
	// The postgres connect string
	ConnectString string
}

// Used to pass global parameters to the migration functions
type MigrateParams struct {
	Settings            DBSettings
	MinerAddress        address.Address
	PieceTrackerHasRows bool
}

var migrationParams *MigrateParams

func Migrate(ctx context.Context, sqldb *sql.DB, params MigrateParams) error {
	migrationParams = &params

	// Check if there are any rows in the PieceTracker table.
	// This is used in the migration that adds the MinerAddr column: if there
	// are rows in the PieceTracker table we need to set MinerAddr to a default
	// value.
	var count int
	cntCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err := sqldb.QueryRowContext(cntCtx, "SELECT COUNT(*) as cnt FROM PieceTracker").Scan(&count)
	cancel()
	if err != nil {
		return fmt.Errorf("getting number of rows in PieceTracker table: %w", err)
	}
	if count > 0 {
		migrationParams.PieceTrackerHasRows = true
	}

	// Run migrations
	goose.SetBaseFS(EmbedMigrations)

	if err := goose.SetDialect("postgres"); err != nil {
		return err
	}

	beforeVer, err := goose.GetDBVersionContext(ctx, sqldb)
	if err != nil {
		return err
	}

	if err := goose.UpContext(ctx, sqldb, "."); err != nil {
		return err
	}

	afterVer, err := goose.GetDBVersionContext(ctx, sqldb)
	if err != nil {
		return err
	}

	if beforeVer != afterVer {
		log.Warnw("boost postgres migrated", "previous", beforeVer, "current", afterVer)
	}

	return nil
}
