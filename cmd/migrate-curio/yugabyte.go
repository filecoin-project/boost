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

		query := `DROP KEYSPACE idx`
		log.Debug(query)
		err = session.Query(query).WithContext(cctx.Context).Exec()
		if err != nil {
			return err
		}

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
