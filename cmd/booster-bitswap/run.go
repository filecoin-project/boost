package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"github.com/filecoin-project/boost/api"
	bclient "github.com/filecoin-project/boost/api/client"
	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/boost/cmd/lib/filters"
	"github.com/filecoin-project/boost/cmd/lib/remoteblockstore"
	"github.com/filecoin-project/boost/metrics"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	"github.com/filecoin-project/go-jsonrpc"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
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
			Name:     "api-boost",
			Usage:    "the endpoint for the boost API",
			Required: true,
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
		&cli.StringFlag{
			Name:  "api-filter-endpoint",
			Usage: "the endpoint to use for fetching a remote retrieval configuration for bitswap requests",
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
		&cli.IntFlag{
			Name:  "engine-blockstore-worker-count",
			Usage: "number of threads for blockstore operations. Used to throttle the number of concurrent requests to the block store",
			Value: 128,
		},
		&cli.IntFlag{
			Name:  "engine-task-worker-count",
			Usage: "number of worker threads used for preparing and packaging responses before they are sent out. This number should generally be equal to task-worker-count",
			Value: 128,
		},
		&cli.IntFlag{
			Name:  "max-outstanding-bytes-per-peer",
			Usage: "maximum number of bytes (across all tasks) pending to be processed and sent to any individual peer (default 32MiB)",
			Value: 32 << 20,
		},
		&cli.IntFlag{
			Name:  "target-message-size",
			Usage: "target size of messages for bitswap to batch messages, actual size may vary depending on the queue (default 1MiB)",
			Value: 1 << 20,
		},
		&cli.IntFlag{
			Name:  "task-worker-count",
			Usage: "Number of threads (goroutines) sending outgoing messages. Throttles the number of concurrent send operations",
			Value: 128,
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

		// Connect to the Boost API
		boostAPIInfo := cctx.String("api-boost")
		bapi, bcloser, err := getBoostAPI(ctx, boostAPIInfo)
		if err != nil {
			return fmt.Errorf("getting boost API: %w", err)
		}
		defer bcloser()

		bitswapBlockMetrics := remoteblockstore.BlockMetrics{
			GetRequestCount:             metrics.BitswapRblsGetRequestCount,
			GetFailResponseCount:        metrics.BitswapRblsGetFailResponseCount,
			GetSuccessResponseCount:     metrics.BitswapRblsGetSuccessResponseCount,
			BytesSentCount:              metrics.BitswapRblsBytesSentCount,
			HasRequestCount:             metrics.BitswapRblsHasRequestCount,
			HasFailResponseCount:        metrics.BitswapRblsHasFailResponseCount,
			HasSuccessResponseCount:     metrics.BitswapRblsHasSuccessResponseCount,
			GetSizeRequestCount:         metrics.BitswapRblsGetSizeRequestCount,
			GetSizeFailResponseCount:    metrics.BitswapRblsGetSizeFailResponseCount,
			GetSizeSuccessResponseCount: metrics.BitswapRblsGetSizeSuccessResponseCount,
		}
		remoteStore := remoteblockstore.NewRemoteBlockstore(bapi, bitswapBlockMetrics)
		// Create the server API
		port := cctx.Int("port")
		repoDir, err := homedir.Expand(cctx.String(FlagRepo.Name))
		if err != nil {
			return fmt.Errorf("expanding repo file path: %w", err)
		}
		host, err := setupHost(repoDir, port)
		if err != nil {
			return fmt.Errorf("setting up libp2p host: %w", err)
		}

		// Create the bitswap server
		multiFilter := filters.NewMultiFilter(repoDir, cctx.String("api-filter-endpoint"), cctx.String("api-filter-auth"), cctx.StringSlice("badbits-denylists"))
		err = multiFilter.Start(ctx)
		if err != nil {
			return fmt.Errorf("starting block filter: %w", err)
		}
		server := NewBitswapServer(remoteStore, host, multiFilter)

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
		err = server.Start(ctx, proxyAddrInfo, &BitswapServerOptions{
			EngineBlockstoreWorkerCount: cctx.Int("engine-blockstore-worker-count"),
			EngineTaskWorkerCount:       cctx.Int("engine-task-worker-count"),
			MaxOutstandingBytesPerPeer:  cctx.Int("max-outstanding-bytes-per-peer"),
			TargetMessageSize:           cctx.Int("target-message-size"),
			TaskWorkerCount:             cctx.Int("task-worker-count"),
		})
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

func getBoostAPI(ctx context.Context, ai string) (api.Boost, jsonrpc.ClientCloser, error) {
	ai = strings.TrimPrefix(strings.TrimSpace(ai), "BOOST_API_INFO=")
	info := cliutil.ParseApiInfo(ai)
	addr, err := info.DialArgs("v0")
	if err != nil {
		return nil, nil, fmt.Errorf("could not get DialArgs: %w", err)
	}

	log.Infof("Using boost API at %s", addr)
	api, closer, err := bclient.NewBoostRPCV0(ctx, addr, info.AuthHeader())
	if err != nil {
		return nil, nil, fmt.Errorf("creating full node service API: %w", err)
	}

	return api, closer, nil
}
