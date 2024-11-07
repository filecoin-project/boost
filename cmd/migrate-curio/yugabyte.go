package main

import (
	"fmt"

	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte"
	"github.com/urfave/cli/v2"
	"github.com/yugabyte/pgx/v4/pgxpool"
)

var cleanupLIDCmd = &cli.Command{
	Name:        "cleanup",
	Description: "Removes the indexes and other metadata from LID",
	Usage:       "migrate-curio cleanup",
	Subcommands: []*cli.Command{
		cleanupLevelDBCmd,
		cleanupYugabyteDBCmd,
	},
}

var cleanupYugabyteDBCmd = &cli.Command{
	Name:        "yugabyte",
	Description: "Removes the indexes and other metadata from Yugabyte based LID store",
	Usage:       "migrate-curio cleanup yugabyte",
	Before:      before,
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:     "hosts",
			Usage:    "yugabyte hosts to connect to over cassandra interface eg '127.0.0.1'",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "username",
			Usage: "yugabyte username to connect to over cassandra interface eg 'cassandra'",
		},
		&cli.StringFlag{
			Name:  "password",
			Usage: "yugabyte password to connect to over cassandra interface eg 'cassandra'",
		},
		&cli.StringFlag{
			Name:     "connect-string",
			Usage:    "postgres connect string eg 'postgresql://postgres:postgres@localhost'",
			Required: true,
		},
		&cli.BoolFlag{
			Name:  "i-know-what-i-am-doing",
			Usage: "confirmation flag",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		if !cctx.Bool("i-know-what-i-am-doing") {
			return fmt.Errorf("please use --i-know-what-i-am-doing flag to confirm. THIS CANNOT BE UNDONE.")
		}

		// Create a yugabyte data service
		settings := yugabyte.DBSettings{
			Hosts:         cctx.StringSlice("hosts"),
			Username:      cctx.String("username"),
			Password:      cctx.String("password"),
			ConnectString: cctx.String("connect-string"),
		}

		settings.CQLTimeout = 60

		cluster := yugabyte.NewCluster(settings)

		session, err := cluster.CreateSession()
		if err != nil {
			return fmt.Errorf("creating yugabyte cluster: %w", err)
		}

		keyspace := "idx"

		// Step 1: Drop all indexes
		var indexName string
		indexesQuery := fmt.Sprintf("SELECT index_name FROM system_schema.indexes WHERE keyspace_name='%s';", keyspace)
		iter := session.Query(indexesQuery).Iter()

		for iter.Scan(&indexName) {
			dropIndexQuery := fmt.Sprintf("DROP INDEX %s.%s;", keyspace, indexName)
			fmt.Println("Executing:", dropIndexQuery)
			if err := session.Query(dropIndexQuery).Exec(); err != nil {
				return fmt.Errorf("failed to drop index %s: %w", indexName, err)
			}
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("failed to iterate over indexes: %w", err)
		}

		// Step 2: Drop the keyspace
		dropKeyspaceQuery := fmt.Sprintf("DROP KEYSPACE %s;", keyspace)
		fmt.Println("Executing:", dropKeyspaceQuery)
		if err := session.Query(dropKeyspaceQuery).Exec(); err != nil {
			return fmt.Errorf("failed to drop keyspace: %w", err)
		}

		fmt.Println("Keyspace dropped successfully.")

		// Create connection pool to postgres interface
		db, err := pgxpool.Connect(cctx.Context, settings.ConnectString)
		if err != nil {
			return err
		}

		_, err = db.Exec(cctx.Context, `DROP TABLE IF EXISTS PieceTracker CASCADE;`)
		if err != nil {
			return err
		}

		_, err = db.Exec(cctx.Context, `DROP TABLE IF EXISTS PieceFlagged CASCADE;`)
		if err != nil {
			return err
		}

		return nil
	},
}
