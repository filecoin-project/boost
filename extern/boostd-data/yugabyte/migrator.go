package yugabyte

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte/cassmigrate"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte/migrations"
	"github.com/filecoin-project/go-address"
	_ "github.com/lib/pq"
	"github.com/yugabyte/gocql"
)

type Migrator struct {
	settings          DBSettings
	minerAddr         address.Address
	CassandraKeyspace string
}

type MigratorOpt func(m *Migrator)

func WithCassandraKeyspaceOpt(ks string) MigratorOpt {
	return func(m *Migrator) {
		m.CassandraKeyspace = ks
	}
}

func NewMigrator(settings DBSettings, minerAddr address.Address, opts ...MigratorOpt) *Migrator {
	m := &Migrator{
		settings:          settings,
		minerAddr:         minerAddr,
		CassandraKeyspace: defaultKeyspace,
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

func (m *Migrator) Migrate(ctx context.Context) error {
	// Create a postgres connection to be used only for running migrations.
	// Note that the migration library requires a *sql.DB, but there's no way
	// to go from a pgxpool connection to a *sql.DB so we need to open a new
	// connection.
	sqldb, err := sql.Open("postgres", m.settings.ConnectString)
	if err != nil {
		return fmt.Errorf("opening postgres connection to %s: %w", m.settings.ConnectString, err)
	}

	// Create a cassandra connection to be used only for running migrations.
	cluster := gocql.NewCluster(m.settings.Hosts...)
	cluster.Keyspace = m.CassandraKeyspace
	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("opening cassandra connection to %s: %w", m.settings.Hosts, err)
	}

	log.Infow("running postgres migrations")
	migrateParams := migrations.MigrateParams{Settings: migrations.DBSettings{
		Hosts:         m.settings.Hosts,
		ConnectString: m.settings.ConnectString,
	}, MinerAddress: m.minerAddr}
	err = migrations.Migrate(ctx, sqldb, migrateParams)
	if err != nil {
		return fmt.Errorf("running postgres migrations: %w", err)
	}
	log.Infow("postgres migrations complete")

	log.Infow("running cassandra migrations")
	err = cassmigrate.Migrate(ctx, session)
	if err != nil {
		return fmt.Errorf("running cassandra migrations: %w", err)
	}
	log.Infow("cassandra migrations complete")

	return nil
}
