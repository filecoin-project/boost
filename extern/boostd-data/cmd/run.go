package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/filecoin-project/boostd-data/couchbase"
	"github.com/filecoin-project/boostd-data/shared/cliutil"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
)

var runCmd = &cli.Command{
	Name: "run",
	Subcommands: []*cli.Command{
		leveldbCmd,
		couchbaseCmd,
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

var couchbaseCmd = &cli.Command{
	Name:   "couchbase",
	Usage:  "Run boostd-data with a couchbase database",
	Before: before,
	Flags: append([]cli.Flag{
		&cli.StringFlag{
			Name:     "connect-string",
			Usage:    "couchbase connect string eg 'couchbase://127.0.0.1'",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "username",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "password",
			Required: true,
		}},
		runFlags...,
	),
	Action: func(cctx *cli.Context) error {
		// Create a couchbase data service
		settings := couchbase.DBSettings{
			ConnectString: cctx.String("connect-string"),
			Auth: couchbase.DBSettingsAuth{
				Username: cctx.String("username"),
				Password: cctx.String("password"),
			},
		}

		bdsvc := svc.NewCouchbase(settings)
		return runAction(cctx, "couchbase", bdsvc)
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
	err = store.Start(ctx, addr)
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
