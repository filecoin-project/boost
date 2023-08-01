package yugabyte

import (
	"database/sql"
	"fmt"
	"github.com/filecoin-project/boostd-data/yugabyte/migrations"
	"github.com/filecoin-project/go-address"
)

type Migrator struct {
	connectString string
	minerAddr     address.Address
}

func NewMigrator(connectString string, minerAddr address.Address) *Migrator {
	return &Migrator{
		connectString: connectString,
		minerAddr:     minerAddr,
	}
}

func (m *Migrator) Migrate() error {
	log.Infow("running migrations")

	// Create a connection to be used only for running migrations.
	// Note that the migration library requires a *sql.DB, but there's no way
	// to go from a pgxpool connection to a *sql.DB so we need to open a new
	// connection.
	sqldb, err := sql.Open("postgres", m.connectString)
	if err != nil {
		return fmt.Errorf("opening postgres connection to %s: %w", m.connectString, err)
	}

	err = migrations.Migrate(sqldb, migrations.MigrateParams{MinerAddress: m.minerAddr})
	if err != nil {
		return fmt.Errorf("running postgres migrations: %w", err)
	}
	log.Infow("migrations complete")

	return nil
}
