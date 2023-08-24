package yugabyte

import (
	"database/sql"
	"fmt"

	"github.com/filecoin-project/boostd-data/yugabyte/cqlMigrations"
	"github.com/filecoin-project/boostd-data/yugabyte/migrations"
	"github.com/filecoin-project/go-address"
	_ "github.com/lib/pq"
)

type Migrator struct {
	hosts         []string
	connectString string
	minerAddr     address.Address
}

func NewMigrator(hosts []string, connectString string, minerAddr address.Address) *Migrator {
	return &Migrator{
		hosts:         hosts,
		connectString: connectString,
		minerAddr:     minerAddr,
	}
}

func (m *Migrator) Migrate() error {
	log.Infow("running migrations")
	err := m.migratePostgres()
	if err != nil {
		return err
	}
	return m.migrateCassandra()
}

func (m *Migrator) migratePostgres() error {
	// Create a connection to be used only for running migrations.
	// Note that the migration library requires a *sql.DB, but there's no way
	// to go from a pgxpool connection to a *sql.DB so we need to open a new
	// connection.
	sqldb, err := sql.Open("postgres", m.connectString)
	if err != nil {
		return fmt.Errorf("opening postgres connection to %s: %w", m.connectString, err)
	}

	migrateParams := migrations.MigrateParams{ConnectString: m.connectString, MinerAddress: m.minerAddr}
	err = migrations.Migrate(sqldb, migrateParams)
	if err != nil {
		return fmt.Errorf("running postgres migrations: %w", err)
	}
	log.Infow("postgres migrations complete")
	return nil
}

func (m *Migrator) migrateCassandra() error {
	params := &cqlMigrations.MigrateParams{
		Hosts:        m.hosts,
		MinerAddress: m.minerAddr,
	}
	err := cqlMigrations.Migrate(params)
	if err != nil {
		return fmt.Errorf("running cassandra migrations: %w", err)
	}
	log.Infow("cassandra migrations complete")
	return nil
}
