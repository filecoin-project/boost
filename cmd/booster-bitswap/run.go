package main

import (
	"fmt"
	"github.com/filecoin-project/boost/cmd/booster-bitswap/blockfilter"
	"github.com/filecoin-project/boost/cmd/booster-bitswap/remoteblockstore"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/metrics"
	"github.com/filecoin-project/boost/piecemeta"
	"github.com/filecoin-project/boost/tracing"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"net/http"
	_ "net/http/pprof"
)

var runCmd = &cli.Command{
	Name:   "run",
	Usage:  "Start a booster-bitswap process",
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
		&cli.UintFlag{
			Name:  "port",
			Usage: "the port to listen for bitswap requests on",
			Value: 8888,
		},
		&cli.UintFlag{
			Name:  "metrics-port",
			Usage: "the http port to serve prometheus metrics on",
			Value: 9696,
		},
		&cli.StringFlag{
			Name:     "api-fullnode",
			Usage:    "the endpoint for the full node API",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "api-storage",
			Usage:    "the endpoint for the storage node API",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "api-piece-directory",
			Usage:    "the endpoint for the piece directory API",
			Required: true,
		},
		&cli.IntFlag{
			Name:  "add-index-throttle",
			Usage: "the maximum number of add index operations that can run in parallel",
			Value: 4,
		},
		&cli.StringFlag{
			Name:  "proxy",
			Usage: "the multiaddr of the libp2p proxy that this node connects through",
		},
		&cli.BoolFlag{
			Name:  "tracing",
			Usage: "enables tracing of booster-bitswap calls",
			Value: false,
		},
		&cli.StringFlag{
			Name:  "tracing-endpoint",
			Usage: "the endpoint for the tracing exporter",
			Value: "http://tempo:14268/api/traces",
		},
	},
	Action: func(cctx *cli.Context) error {
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

		// Instantiate the tracer and exporter
		if cctx.Bool("tracing") {
			tracingStopper, err := tracing.New("booster-bitswap", cctx.String("tracing-endpoint"))
			if err != nil {
				return fmt.Errorf("failed to instantiate tracer: %w", err)
			}
			log.Info("Tracing exporter enabled")

			defer func() {
				_ = tracingStopper(ctx)
			}()
		}

		// Connect to the full node API
		fnApiInfo := cctx.String("api-fullnode")
		fullnodeApi, ncloser, err := lib.GetFullNodeApi(ctx, fnApiInfo, log)
		if err != nil {
			return fmt.Errorf("getting full node API: %w", err)
		}
		defer ncloser()

		// Connect to the storage API and create a sector accessor
		storageApiInfo := cctx.String("api-storage")
		sa, storageCloser, err := lib.CreateSectorAccessor(ctx, storageApiInfo, fullnodeApi, log)
		if err != nil {
			return err
		}
		defer storageCloser()

		// Connect to the piece directory service
		pdClient := piecemeta.NewStore()
		defer pdClient.Close(ctx)
		err = pdClient.Dial(ctx, cctx.String("api-piece-directory"))
		if err != nil {
			return fmt.Errorf("connecting to piece directory service: %w", err)
		}

		// Create the bitswap host
		port := cctx.Int("port")
		repoDir, err := homedir.Expand(cctx.String(FlagRepo.Name))
		if err != nil {
			return fmt.Errorf("creating repo dir %s: %w", cctx.String(FlagRepo.Name), err)
		}
		host, err := setupHost(repoDir, port)
		if err != nil {
			return fmt.Errorf("setting up libp2p host: %w", err)
		}

		// Create the bitswap server
		blockFilter := blockfilter.NewBlockFilter(repoDir)
		err = blockFilter.Start(ctx)
		if err != nil {
			return fmt.Errorf("starting block filter: %w", err)
		}
		pieceMeta := piecemeta.NewPieceMeta(pdClient, sa, cctx.Int("add-index-throttle"))
		remoteStore := remoteblockstore.NewRemoteBlockstore(pieceMeta)
		server := NewBitswapServer(remoteStore, host, blockFilter)

		var proxyAddrInfo *peer.AddrInfo
		if cctx.IsSet("proxy") {
			proxy := cctx.String("proxy")
			proxyAddrInfo, err = peer.AddrInfoFromString(proxy)
			if err != nil {
				return fmt.Errorf("parsing proxy multiaddr %s: %w", proxy, err)
			}
		}

		// Start the bitswap server
		log.Infof("Starting booster-bitswap node on port %d", port)
		err = server.Start(ctx, proxyAddrInfo)
		if err != nil {
			return err
		}

		// Start the metrics web server
		metricsPort := cctx.Int("metrics-port")
		log.Infof("Starting booster-bitswap metrics web server on port %d", metricsPort)
		http.Handle("/metrics", metrics.Exporter("booster_bitswap")) // metrics server
		go func() {
			if err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", metricsPort), nil); err != nil {
				log.Errorf("could not start prometheus metric exporter server: %s", err)
			}
		}()

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

		return nil
	},
}
