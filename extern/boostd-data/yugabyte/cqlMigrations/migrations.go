package cqlMigrations

import (
	"embed"
	"fmt"

	"github.com/filecoin-project/go-address"
	"github.com/gocql/gocql"
	gmigrate "github.com/golang-migrate/migrate/v4"
	mcassandra "github.com/golang-migrate/migrate/v4/database/cassandra"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("cqlmigrations")

//go:embed migrations/*.cql
var EmbedCqlMigrations embed.FS

// MigrateParams is used to pass global parameters to the migration functions
type MigrateParams struct {
	Hosts             []string
	MinerAddress      address.Address
	CassandraKeySpace string
}

// Migrate currently support ony schema migrations and direct CQL queries. This package does not support
// the go migrations yet
func Migrate(params *MigrateParams) error {

	cluster := gocql.NewCluster(params.Hosts...)
	cluster.Keyspace = params.CassandraKeySpace
	cluster.Consistency = gocql.All
	s, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	defer s.Close()

	mcfg := &mcassandra.Config{
		KeyspaceName: params.CassandraKeySpace,
	}

	casDrv, err := mcassandra.WithInstance(s, mcfg)
	if err != nil {
		return err
	}

	f, err := iofs.New(EmbedCqlMigrations, "migrations")
	if err != nil {
		return err
	}

	mig, err := gmigrate.NewWithInstance("iofs", f, params.CassandraKeySpace, casDrv)
	if err != nil {
		return err
	}

	beforeVer, dirty, err := mig.Version()
	if err != nil {
		if err != gmigrate.ErrNilVersion {
			return err
		}
		beforeVer = 0
	}

	if dirty {
		return fmt.Errorf("Before a migration runs, each database sets a dirty flag. Execution stops if a migration fails" +
			" and the dirty state persists,\n which prevents attempts to run more migrations on top of a failed migration. " +
			"You need to manually fix the error\n  and then \"force\" the expected version.")
	}

	err = mig.Up()
	if err != nil {
		return err
	}

	afterVer, dirty, err := mig.Version()
	if err != nil {
		return err
	}

	if dirty {
		return fmt.Errorf("Before a migration runs, each database sets a dirty flag. Execution stops if a migration fails" +
			" and the dirty state persists,\n which prevents attempts to run more migrations on top of a failed migration. " +
			"You need to manually fix the error\n  and then \"force\" the expected version.")
	}

	if beforeVer != afterVer {
		log.Warnw("boost postgres migrated", "previous", beforeVer, "current", afterVer)
	}

	return nil
}

// TODO: Fix minerAddr migration for testnet - dependent on go migrations
