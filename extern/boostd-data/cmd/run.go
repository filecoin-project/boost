package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/build"
	"github.com/filecoin-project/boost/extern/boostd-data/metrics"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/cliutil"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/tracing"
	"github.com/filecoin-project/boost/extern/boostd-data/svc"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte/migrations"
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	DefaultInsertConcurrency = 4
)

var runCmd = &cli.Command{
	Name: "run",
	Subcommands: []*cli.Command{
		leveldbCmd,
		yugabyteCmd,
		yugabyteMigrateCmd,
		yugabyteAddIndexCmd,
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
			Name:    "repo",
			EnvVars: []string{"LID_LEVELDB_PATH"},
			Usage:   "repo directory where the leveldb database is created. Default is ~/.boost",
			Value:   "~/.boost",
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
		&cli.IntFlag{
			Name:  "insert-concurrency",
			Usage: "the number of concurrent tasks that each add index operation is split into",
			Value: DefaultInsertConcurrency,
		},
		&cli.IntFlag{
			Name:     "CQLTimeout",
			Usage:    "client timeout value in seconds for CQL queries",
			Required: false,
			Value:    yugabyte.CqlTimeout,
		}},
		runFlags...,
	),
	Action: func(cctx *cli.Context) error {
		// Create a yugabyte data service
		settings := yugabyte.DBSettings{
			Hosts:             cctx.StringSlice("hosts"),
			Username:          cctx.String("username"),
			Password:          cctx.String("password"),
			ConnectString:     cctx.String("connect-string"),
			CQLTimeout:        cctx.Int("CQLTimeout"),
			InsertConcurrency: cctx.Int("insert-concurrency"),
		}

		// One of the migrations requires a miner address. But we don't want to
		// add a miner-address parameter to this command just for the one time
		// that the user needs to perform that specific migration. Instead, pass
		// a disabled miner address, and if the migration is needed it will
		// throw ErrMissingMinerAddr and we can inform the user they need to
		// perform the migration.
		migrator := yugabyte.NewMigrator(settings, migrations.DisabledMinerAddr)

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
	ctxx := cliutil.ReqContext(cctx)

	if cctx.Bool("pprof") {
		go func() {
			err := http.ListenAndServe("localhost:6071", nil)
			if err != nil {
				log.Error(err)
			}
		}()
	}

	ctx, _ := tag.New(ctxx,
		tag.Insert(metrics.Version, build.BuildVersion),
		tag.Insert(metrics.Commit, build.CurrentCommit),
		tag.Insert(metrics.NodeType, "boostd-data"),
	)
	// Register all metric views
	if err := view.Register(
		metrics.DefaultViews...,
	); err != nil {
		log.Fatalf("Cannot register the view: %v", err)
	}
	// Set the metric to one so, it is published to the exporter
	stats.Record(ctx, metrics.BoostInfo.M(1))

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
		&cli.StringFlag{
			Name:  "miner-address",
			Usage: "default miner address eg f1234",
		},
		&cli.IntFlag{
			Name:  "insert-concurrency",
			Usage: "the number of concurrent tasks that each add index operation is split into",
			Value: DefaultInsertConcurrency,
		},
		&cli.IntFlag{
			Name:     "CQLTimeout",
			Usage:    "client timeout value in seconds for CQL queries",
			Required: false,
			Value:    yugabyte.CqlTimeout,
		},
	},
	Action: func(cctx *cli.Context) error {
		_ = logging.SetLogLevel("migrations", "info")
		ctx := cliutil.ReqContext(cctx)

		// Create a yugabyte data service
		settings := yugabyte.DBSettings{
			Hosts:             cctx.StringSlice("hosts"),
			Username:          cctx.String("username"),
			Password:          cctx.String("password"),
			ConnectString:     cctx.String("connect-string"),
			CQLTimeout:        cctx.Int("CQLTimeout"),
			InsertConcurrency: cctx.Int("insert-concurrency"),
		}

		maddr := migrations.DisabledMinerAddr
		if cctx.IsSet("miner-address") {
			var err error
			maddr, err = address.NewFromString(cctx.String("miner-address"))
			if err != nil {
				return fmt.Errorf("parsing miner address '%s': %w", maddr, err)
			}
		}
		migrator := yugabyte.NewMigrator(settings, maddr)
		err := migrator.Migrate(ctx)
		if err != nil && errors.Is(err, migrations.ErrMissingMinerAddr) {
			msg := "You must set the miner-address flag to do the migration. " +
				"Set it to the address of the storage miner eg f1234"
			return fmt.Errorf("%s", msg)
		}
		return err
	},
}

var yugabyteAddIndexCmd = &cli.Command{
	Name:   "yugabyte-add-index",
	Usage:  "Add index via boostd-data to YugabyteDB (testing tool)",
	Before: before,
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
		&cli.StringFlag{
			Name:     "filename",
			Usage:    "filename must be same as pieceCID",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "insert-concurrency",
			Usage: "the number of concurrent tasks that each add index operation is split into",
			Value: DefaultInsertConcurrency,
		},
		&cli.IntFlag{
			Name:     "CQLTimeout",
			Usage:    "client timeout value in seconds for CQL queries",
			Required: false,
			Value:    yugabyte.CqlTimeout,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)

		// Create a yugabyte data service
		settings := yugabyte.DBSettings{
			Hosts:             cctx.StringSlice("hosts"),
			Username:          cctx.String("username"),
			Password:          cctx.String("password"),
			ConnectString:     cctx.String("connect-string"),
			CQLTimeout:        cctx.Int("CQLTimeout"),
			InsertConcurrency: cctx.Int("insert-concurrency"),
		}

		migrator := yugabyte.NewMigrator(settings, migrations.DisabledMinerAddr)

		bdsvc := svc.NewYugabyte(settings, migrator)
		err := bdsvc.Impl.Start(ctx)
		if err != nil {
			return err
		}

		piececidStr := cctx.String("filename")

		piececid, err := cid.Parse(piececidStr)
		if err != nil {
			return err
		}

		reader, err := os.Open(cctx.String("filename"))
		if err != nil {
			return fmt.Errorf("couldn't open file: %s", err)
		}
		defer func() {
			_ = reader.Close()
		}()

		opts := []carv2.Option{carv2.ZeroLengthSectionAsEOF(true)}
		blockReader, err := carv2.NewBlockReader(reader, opts...)
		if err != nil {
			return fmt.Errorf("getting block reader over piece file %s: %w", piececid, err)
		}

		recs := make([]model.Record, 0)

		now := time.Now()

		blockMetadata, err := blockReader.SkipNext()
		for err == nil {
			recs = append(recs, model.Record{
				Cid: blockMetadata.Cid,
				OffsetSize: model.OffsetSize{
					Offset: blockMetadata.Offset,
					Size:   blockMetadata.Size,
				},
			})

			blockMetadata, err = blockReader.SkipNext()
		}
		if !errors.Is(err, io.EOF) {
			return fmt.Errorf("generating index for piece %s: %w", piececid, err)
		}

		log.Warnw("read index successfully", "piececid", piececid, "recs", len(recs), "took", time.Since(now))

		now = time.Now()

		aip := bdsvc.Impl.AddIndex(ctx, piececid, recs, true)
		for resp := range aip {
			if resp.Err != "" {
				return fmt.Errorf("failed to add index to yugabytedb: %s", resp.Err)
			}
			log.Warnw("adding index to yugabytedb", "progress", resp.Progress)
		}
		log.Warnw("added index to yugabytedb successfully", "took", time.Since(now))

		return nil
	},
}
