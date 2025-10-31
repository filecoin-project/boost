package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/metrics"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/api/v1api"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/gateway"

	lcliutil "github.com/filecoin-project/lotus/cli/util"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"

	"github.com/urfave/cli/v2"
)

var runCmd = &cli.Command{
	Name:   "run",
	Usage:  "Start a boost process",
	Before: before,
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "pprof",
			Usage: "run pprof web server on localhost:6060",
		},
		&cli.BoolFlag{
			Name:  "nosync",
			Usage: "dont wait for the full node to sync with the chain",
		},
		&cli.BoolFlag{
			Name:  "no-metrics",
			Usage: "stops emitting information about the node as metrics (param is used by tests)",
		},
		&cli.BoolFlag{
			Name:     "deprecated",
			Usage:    "Boost is now deprecated. Please migrate to Curio instead. See https://docs.curiostorage.org/ and https://docs.curiostorage.org/curio-market/migrating-from-boost for more information.",
			Required: true,
			Value:    false,
		},
	},
	Action: func(cctx *cli.Context) error {
		if cctx.Bool("pprof") {
			go func() {
				err := http.ListenAndServe("localhost:6060", nil)
				if err != nil {
					log.Error(err)
				}
			}()
		}

		subCh := gateway.NewEthSubHandler()
		fullnodeApi, ncloser, err := lcli.GetFullNodeAPIV1(cctx, lcliutil.FullNodeWithEthSubscriptionHandler(subCh))
		if err != nil {
			return fmt.Errorf("getting full node api: %w", err)
		}
		defer ncloser()

		ctx := lcli.ReqContext(cctx)

		if !cctx.Bool("no-metrics") {
			ctx, _ = tag.New(ctx,
				tag.Insert(metrics.Version, build.BuildVersion),
				tag.Insert(metrics.Commit, build.CurrentCommit),
				tag.Insert(metrics.NodeType, "boostd"),
				tag.Insert(metrics.StartedAt, time.Now().String()),
			)
			// Register all metric views
			if err = view.Register(
				metrics.DefaultViews...,
			); err != nil {
				log.Fatalf("Cannot register the view: %v", err)
			}
			// Set the metric to one so, it is published to the exporter
			stats.Record(ctx, metrics.BoostInfo.M(1))
		}

		log.Debug("Checking full node version")

		v, err := fullnodeApi.Version(ctx)
		if err != nil {
			return err
		}

		log.Debugw("Remote full node version", "version", v)

		if !v.APIVersion.EqMajorMinor(lapi.FullAPIVersion1) {
			return fmt.Errorf("remote API version didn't match (expected %s, remote %s)", lapi.FullAPIVersion1, v.APIVersion)
		}

		log.Debug("Checking full node sync status")

		if !cctx.Bool("nosync") {
			if err := lcli.SyncWait(ctx, &v0api.WrapperV1Full{FullNode: fullnodeApi}, false); err != nil {
				return fmt.Errorf("sync wait: %w", err)
			}
		}

		boostRepoPath := cctx.String(FlagBoostRepo)

		r, err := lotus_repo.NewFS(boostRepoPath)
		if err != nil {
			return err
		}
		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("repo at '%s' is not initialized", cctx.String(FlagBoostRepo))
		}

		shutdownChan := make(chan struct{})

		log.Debug("Instantiating new boost node")

		var boostApi api.Boost
		stop, err := node.New(ctx,
			node.BoostAPI(&boostApi),
			node.Override(new(*gateway.EthSubHandler), subCh),
			node.Override(new(dtypes.ShutdownChan), shutdownChan),
			node.Base(),
			node.Repo(r),
			node.Override(new(v1api.FullNode), fullnodeApi),
		)
		if err != nil {
			return fmt.Errorf("creating node: %w", err)
		}

		log.Debug("Getting API endpoint of boost node")

		endpoint, err := r.APIEndpoint()
		if err != nil {
			return fmt.Errorf("getting API endpoint: %w", err)
		}

		// Get maddr for boost node
		maddr, err := boostApi.NetAddrsListen(ctx)
		if err != nil {
			return fmt.Errorf("getting boost libp2p address: %w", err)
		}

		log.Infow("Boost libp2p node listening", "maddr", maddr)

		// Bootstrap with full node
		remoteAddrs, err := fullnodeApi.NetAddrsListen(ctx)
		if err != nil {
			return fmt.Errorf("getting full node libp2p address: %w", err)
		}

		log.Debugw("Bootstrapping boost libp2p network with full node", "maadr", remoteAddrs)

		if err := boostApi.NetConnect(ctx, remoteAddrs); err != nil {
			return fmt.Errorf("connecting to full node (libp2p): %w", err)
		}

		// Instantiate the boost service JSON RPC handler.
		handler, err := node.BoostHandler(boostApi, true)
		if err != nil {
			return fmt.Errorf("failed to instantiate rpc handler: %w", err)
		}

		log.Infow("Boost JSON RPC server is listening", "endpoint", endpoint)

		// Serve the RPC.
		rpcStopper, err := node.ServeRPC(handler, "boost", endpoint)
		if err != nil {
			return fmt.Errorf("failed to start json-rpc endpoint: %s", err)
		}

		// Monitor for shutdown.
		finishCh := node.MonitorShutdown(shutdownChan,
			node.ShutdownHandler{Component: "rpc server", StopFunc: rpcStopper},
			node.ShutdownHandler{Component: "boost", StopFunc: stop},
		)

		<-finishCh
		return nil
	},
}
