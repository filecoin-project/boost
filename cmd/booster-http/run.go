package main

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/cmd/lib/filters"
	"github.com/filecoin-project/boost/cmd/lib/remoteblockstore"
	bdclient "github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/tracing"
	"github.com/filecoin-project/boost/metrics"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/ipfs/go-cid"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	trustlessMessage = "booster-http only supports trustless HTTP. For " +
		"trusted HTTP use https://github.com/ipfs/bifrost-gateway to translate " +
		"trustless responses. Run bifrost-gateway with the environment variable " +
		"PROXY_GATEWAY_URL=http://localhost:7777 to point to booster-http, and " +
		"GRAPH_BACKEND=true to efficiently translate booster-http's trustless " +
		"responses."
)

var runCmd = &cli.Command{
	Name:   "run",
	Usage:  "Start a booster-http process",
	Before: before,
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "pprof",
			Usage: "run pprof web server on localhost:6070",
		},
		&cli.UintFlag{
			Name:  "pprof-port",
			Usage: "the http port to serve pprof on",
			Value: 6070,
		},
		&cli.StringFlag{
			Name:  "base-path",
			Usage: "the base path at which to run the web server",
			Value: "",
		},
		&cli.StringFlag{
			Name:    "address",
			Aliases: []string{"addr"},
			Usage:   "the listen address for the web server",
			Value:   "0.0.0.0",
		},
		&cli.UintFlag{
			Name:  "port",
			Usage: "the port the web server listens on",
			Value: 7777,
		},
		&cli.StringFlag{
			Name:     "api-lid",
			Usage:    "the endpoint for the local index directory API, eg 'http://localhost:8042'",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "add-index-throttle",
			Usage: "the maximum number of add index operations that can run in parallel",
			Value: 4,
		},
		&cli.IntFlag{
			Name:  "add-index-concurrency",
			Usage: "the maximum number of parallel tasks that a single add index operation can be split into",
			Value: config.DefaultAddIndexConcurrency,
		},
		&cli.StringFlag{
			Name:     "api-fullnode",
			Usage:    "the endpoint for the full node API",
			Required: true,
		},
		&cli.StringSliceFlag{
			Name:     "api-storage",
			Usage:    "the endpoint for the storage node API",
			Required: true,
		},
		&cli.BoolFlag{
			Name:  "serve-pieces",
			Usage: "enables serving raw pieces",
			Value: true,
		},
		&cli.BoolFlag{
			Name:   "serve-blocks",
			Usage:  "(removed option)",
			Hidden: true,
			Action: func(ctx *cli.Context, _ bool) error {
				if _, err := fmt.Fprintf(ctx.App.ErrWriter, "--serve-blocks is no longer supported.\n\n%s\n\n", trustlessMessage); err != nil {
					return err
				}
				return errors.New("--serve-blocks is no longer supported, use bifrost-gateway instead")
			},
		},
		&cli.BoolFlag{
			Name:  "serve-cars",
			Usage: "serve CAR files with the Trustless IPFS Gateway API",
			Value: true,
		},
		&cli.BoolFlag{
			Name:   "serve-files",
			Usage:  "(removed option)",
			Hidden: true,
			Action: func(ctx *cli.Context, _ bool) error {
				if _, err := fmt.Fprintf(ctx.App.ErrWriter, "--serve-files is no longer supported.\n\n%s\n\n", trustlessMessage); err != nil {
					return err
				}
				return errors.New("--serve-files is no longer supported, use bifrost-gateway instead")
			},
		},
		&cli.IntFlag{
			Name: "compression-level",
			Usage: "compression level to use for responses, 0-9, 0 is no " +
				"compression, 9 is maximum compression; set to 0 to disable if using " +
				"a reverse proxy with compression",
			Value: gzip.BestSpeed,
		},
		&cli.StringFlag{
			Name:  "log-file",
			Usage: "path to file to append HTTP request and error logs to, defaults to stdout (-)",
			Value: "-",
		},
		&cli.BoolFlag{
			Name:  "tracing",
			Usage: "enables tracing of booster-http calls",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "tracing-endpoint",
			Usage: "the endpoint for the tracing exporter",
			Value: "http://tempo:14268/api/traces",
		},
		&cli.StringFlag{
			Name:  "api-filter-endpoint",
			Usage: "the endpoint to use for fetching a remote retrieval configuration for requests",
		},
		&cli.StringFlag{
			Name:  "api-filter-auth",
			Usage: "value to pass in the authorization header when sending a request to the API filter endpoint (e.g. 'Basic ~base64 encoded user/pass~'",
		},
		&cli.StringSliceFlag{
			Name:  "badbits-denylists",
			Usage: "the endpoints for fetching one or more custom BadBits list instead of the default one at https://badbits.dwebops.pub/denylist.json",
			Value: cli.NewStringSlice("https://badbits.dwebops.pub/denylist.json"),
		},
		&cli.BoolFlag{
			Name:   "api-version-check",
			Usage:  "Check API versions (param is used by tests)",
			Hidden: true,
			Value:  true,
		},
		&cli.BoolFlag{
			Name:  "no-metrics",
			Usage: "stops emitting information about the node as metrics (param is used by tests)",
		},
	},
	Action: func(cctx *cli.Context) error {
		servePieces := cctx.Bool("serve-pieces")
		serveTrustless := cctx.Bool("serve-cars")

		if !servePieces && !serveTrustless {
			return errors.New("one of --serve-pieces, --serve-cars must be enabled")
		}

		if cctx.Bool("pprof") {
			pprofPort := cctx.Int("pprof-port")
			go func() {
				err := http.ListenAndServe(fmt.Sprintf("localhost:%d", pprofPort), nil)
				if err != nil {
					log.Error(err)
				}
			}()
		}

		ctx := lcli.ReqContext(cctx)

		if !cctx.Bool("no-metrics") {
			ctx, _ = tag.New(ctx,
				tag.Insert(metrics.Version, build.BuildVersion),
				tag.Insert(metrics.Commit, build.CurrentCommit),
				tag.Insert(metrics.NodeType, "booster-http"),
				tag.Insert(metrics.StartedAt, time.Now().String()),
			)
			// Register all metric views
			if err := view.Register(
				metrics.DefaultViews...,
			); err != nil {
				log.Fatalf("Cannot register the view: %v", err)
			}
			// Set the metric to one so, it is published to the exporter
			stats.Record(ctx, metrics.BoostInfo.M(1))
		}

		// Connect to the local index directory service
		cl := bdclient.NewStore()
		defer cl.Close(ctx)
		err := cl.Dial(ctx, cctx.String("api-lid"))
		if err != nil {
			return fmt.Errorf("connecting to local index directory service: %w", err)
		}

		// Connect to the full node API
		fnApiInfo := cctx.String("api-fullnode")
		fullnodeApi, ncloser, err := lib.GetFullNodeApi(ctx, fnApiInfo, log)
		if err != nil {
			return fmt.Errorf("getting full node API: %w", err)
		}
		defer ncloser()

		if cctx.Bool("api-version-check") {
			err = lib.CheckFullNodeApiVersion(ctx, fullnodeApi)
			if err != nil {
				return err
			}
		}

		// Instantiate the tracer and exporter
		enableTracing := cctx.Bool("tracing")
		var tracingStopper func(context.Context) error
		if enableTracing {
			tracingStopper, err = tracing.New("booster-http", cctx.String("tracing-endpoint"))
			if err != nil {
				return fmt.Errorf("failed to instantiate tracer: %w", err)
			}
			log.Info("Tracing exporter enabled")
		}

		// Connect to the storage API(s) and create a piece reader
		sa := lib.NewMultiMinerAccessor(cctx.StringSlice("api-storage"), fullnodeApi)
		err = sa.Start(ctx, log)
		if err != nil {
			return err
		}
		defer sa.Close()

		// Create the server API
		pd := piecedirectory.NewPieceDirectory(cl, sa, cctx.Int("add-index-throttle"),
			piecedirectory.WithAddIndexConcurrency(cctx.Int("add-index-concurrency")))

		opts := &HttpServerOptions{
			ServePieces:      servePieces,
			ServeTrustless:   serveTrustless,
			CompressionLevel: cctx.Int("compression-level"),
		}

		if serveTrustless {
			repoDir, err := createRepoDir(cctx.String(FlagRepo.Name))
			if err != nil {
				return err
			}

			// Set up badbits filter
			multiFilter := filters.NewMultiFilter(repoDir, cctx.String("api-filter-endpoint"), cctx.String("api-filter-auth"), cctx.StringSlice("badbits-denylists"))
			err = multiFilter.Start(ctx)
			if err != nil {
				return fmt.Errorf("starting block filter: %w", err)
			}

			httpBlockMetrics := remoteblockstore.BlockMetrics{
				GetRequestCount:             metrics.HttpRblsGetRequestCount,
				GetFailResponseCount:        metrics.HttpRblsGetFailResponseCount,
				GetSuccessResponseCount:     metrics.HttpRblsGetSuccessResponseCount,
				BytesSentCount:              metrics.HttpRblsBytesSentCount,
				HasRequestCount:             metrics.HttpRblsHasRequestCount,
				HasFailResponseCount:        metrics.HttpRblsHasFailResponseCount,
				HasSuccessResponseCount:     metrics.HttpRblsHasSuccessResponseCount,
				GetSizeRequestCount:         metrics.HttpRblsGetSizeRequestCount,
				GetSizeFailResponseCount:    metrics.HttpRblsGetSizeFailResponseCount,
				GetSizeSuccessResponseCount: metrics.HttpRblsGetSizeSuccessResponseCount,
			}
			rbs := remoteblockstore.NewRemoteBlockstore(pd, &httpBlockMetrics)
			filtered := filters.NewFilteredBlockstore(rbs, multiFilter)
			opts.Blockstore = filtered
		}

		switch cctx.String("log-file") {
		case "":
		case "-":
			opts.LogWriter = cctx.App.Writer
		default:
			opts.LogWriter, err = os.OpenFile(cctx.String("log-file"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				return err
			}
		}

		sapi := serverApi{ctx: ctx, piecedirectory: pd, sa: sa}
		server := NewHttpServer(
			cctx.String("base-path"),
			cctx.String("address"),
			cctx.Int("port"),
			sapi,
			opts,
		)

		// Start the local index directory
		pd.Start(ctx)

		// Start the server
		log.Infof("Starting booster-http node on listen address %s and port %d with base path '%s'",
			cctx.String("address"), cctx.Int("port"), cctx.String("base-path"))
		err = server.Start(ctx)
		if err != nil {
			return fmt.Errorf("starting http server: %w", err)
		}

		if servePieces {
			log.Infof("serving raw pieces at " + server.pieceBasePath())
		} else {
			log.Infof("serving raw pieces is disabled")
		}
		if serveTrustless {
			log.Infof("serving IPFS Trustless Gateway CARs at " + server.ipfsBasePath())
		} else {
			log.Infof("serving IPFS Trustless Gateway CARs is disabled")
		}

		// Monitor for shutdown.
		<-ctx.Done()

		log.Info("Shutting down...")

		err = server.Stop()
		if err != nil {
			return err
		}
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
	},
}

func createRepoDir(repoDir string) (string, error) {
	repoDir, err := homedir.Expand(repoDir)
	if err != nil {
		return "", fmt.Errorf("expanding repo file path: %w", err)
	}
	if repoDir == "" {
		return "", fmt.Errorf("%s is a required flag", FlagRepo.Name)
	}
	return repoDir, os.MkdirAll(repoDir, 0o744)
}

type serverApi struct {
	ctx            context.Context
	piecedirectory *piecedirectory.PieceDirectory
	sa             *lib.MultiMinerAccessor
}

var _ HttpServerApi = (*serverApi)(nil)

func (s serverApi) GetPieceDeals(ctx context.Context, pieceCID cid.Cid) ([]model.DealInfo, error) {
	return s.piecedirectory.GetPieceDeals(ctx, pieceCID)
}

func (s serverApi) IsUnsealed(ctx context.Context, minerAddr address.Address, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error) {
	return s.sa.IsUnsealed(ctx, minerAddr, sectorID, offset, length)
}

func (s serverApi) UnsealSectorAt(ctx context.Context, minerAddr address.Address, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error) {
	return s.sa.UnsealSectorAt(ctx, minerAddr, sectorID, offset, length)
}
