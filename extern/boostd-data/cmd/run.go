package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/filecoin-project/boostd-data/shared/cliutil"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/filecoin-project/boostd-data/yugabyte"
	"github.com/filecoin-project/boostd-data/yugabyte/migrations"
	"github.com/filecoin-project/go-address"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var runCmd = &cli.Command{
	Name: "run",
	Subcommands: []*cli.Command{
		leveldbCmd,
		yugabyteCmd,
		yugabyteMigrateCmd,
	},
}

var runFlags = []cli.Flag{
	&cli.StringFlag{
		Name:  "addr",
		Usage: "the address the boostd-data listens on",
		Value: "localhost:8042",
	},
	&cli.BoolFlag{
		Name:  "pprof",
		Usage: "run pprof web server on localhost:6071",
	},
	&cli.BoolFlag{
		Name:  "tracing",
		Usage: "enables tracing of boostd-data calls",
		Value: false,
	},
	&cli.StringFlag{
		Name:  "tracing-endpoint",
		Usage: "the endpoint for the tracing exporter",
		Value: "http://tempo:14268/api/traces",
	},
}

var leveldbCmd = &cli.Command{
	Name:   "leveldb",
	Usage:  "Run boostd-data with a leveldb database",
	Before: before,
	Flags: append([]cli.Flag{
		&cli.StringFlag{
			Name:  "repo",
			Usage: "repo directory where the leveldb database is created",
			Value: "~/.boost",
		}},
		runFlags...,
	),
	Action: func(cctx *cli.Context) error {
		repoDir, err := homedir.Expand(cctx.String("repo"))
		if err != nil {
			return err
		}

		// Create a leveldb data service
		dbsvc, err := svc.NewLevelDB(repoDir)
		if err != nil {
			return err
		}

		return runAction(cctx, "leveldb", dbsvc)
	},
}

var yugabyteCmd = &cli.Command{
	Name:   "yugabyte",
	Usage:  "Run boostd-data with a yugabyte database",
	Before: before,
	Flags: append([]cli.Flag{
		&cli.StringSliceFlag{
			Name:     "hosts",
			Usage:    "yugabyte hosts to connect to over cassandra interface eg '127.0.0.1'",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "connect-string",
			Usage:    "postgres connect string eg 'postgresql://postgres:postgres@localhost'",
			Required: true,
		}},
		runFlags...,
	),
	Action: func(cctx *cli.Context) error {
		// Create a yugabyte data service
		settings := yugabyte.DBSettings{
			Hosts:         cctx.StringSlice("hosts"),
			ConnectString: cctx.String("connect-string"),
		}

		// One of the migrations requires a miner address. But we don't want to
		// add a miner-address parameter to this command just for the one time
		// that the user needs to perform that specific migration. Instead, pass
		// a disabled miner address, and if the migration is needed it will
		// throw ErrMissingMinerAddr and we can inform the user they need to
		// perform the migration.
		migrator := yugabyte.NewMigrator(settings.ConnectString, migrations.DisabledMinerAddr)

		// Create a connection to the yugabyte implementation of LID
		bdsvc := svc.NewYugabyte(settings, migrator)
		err := runAction(cctx, "yugabyte", bdsvc)
		if err != nil && errors.Is(err, migrations.ErrMissingMinerAddr) {
			return fmt.Errorf("The database needs to be migrated. Run `boostd-data run yugabyte-migrate`")
		}
		return err
	},
}

func runAction(cctx *cli.Context, dbType string, store *svc.Service) error {
	ctx := cliutil.ReqContext(cctx)

	if cctx.Bool("pprof") {
		go func() {
			err := http.ListenAndServe("localhost:6071", nil)
			if err != nil {
				log.Error(err)
			}
		}()
	}

	// Instantiate the tracer and exporter
	enableTracing := cctx.Bool("tracing")
	var tracingStopper func(context.Context) error
	var err error
	if enableTracing {
		tracingStopper, err = tracing.New("boostd-data", cctx.String("tracing-endpoint"))
		if err != nil {
			return fmt.Errorf("failed to instantiate tracer: %w", err)
		}
		log.Info("Tracing exporter enabled")
	}

	// Start the server
	addr := cctx.String("addr")
	_, err = store.Start(ctx, addr)
	if err != nil {
		return fmt.Errorf("starting %s store: %w", dbType, err)
	}

	log.Infof("Started boostd-data %s service on address %s",
		dbType, addr)

	// Monitor for shutdown.
	<-ctx.Done()

	log.Info("Graceful shutdown successful")

	// Sync all loggers.
	_ = log.Sync() //nolint:errcheck

	if enableTracing {
		err = tracingStopper(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

var yugabyteMigrateCmd = &cli.Command{
	Name:   "yugabyte-migrate",
	Usage:  "Migrate boostd-data yugabyte database",
	Before: before,
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:     "hosts",
			Usage:    "yugabyte hosts to connect to over cassandra interface eg '127.0.0.1'",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "connect-string",
			Usage:    "postgres connect string eg 'postgresql://postgres:postgres@localhost'",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "miner-address",
			Usage: "default miner address eg f1234",
		},
	},
	Action: func(cctx *cli.Context) error {
		// Create a yugabyte data service
		settings := yugabyte.DBSettings{
			Hosts:         cctx.StringSlice("hosts"),
			ConnectString: cctx.String("connect-string"),
		}

		maddr := migrations.DisabledMinerAddr
		if cctx.IsSet("miner-address") {
			var err error
			maddr, err = address.NewFromString(cctx.String("miner-address"))
			if err != nil {
				return fmt.Errorf("parsing miner address '%s': %w", maddr, err)
			}
		}
		migrator := yugabyte.NewMigrator(settings.ConnectString, maddr)
		err := migrator.Migrate()
		if err != nil && errors.Is(err, migrations.ErrMissingMinerAddr) {
			msg := "You must set the miner-address flag to do the migration. " +
				"Set it to the address of the storage miner eg f1234"
			return fmt.Errorf(msg)
		}
		return err
	},
}
