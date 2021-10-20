package main

import (
	"fmt"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/node/repo"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	lcli "github.com/filecoin-project/lotus/cli"
)

var runCmd = &cli.Command{
	Name:   "run",
	Usage:  "Start a boost process",
	Before: before,
	Action: func(cctx *cli.Context) error {
		fullnodeApi, ncloser, err := lcli.GetFullNodeAPIV1(cctx)
		if err != nil {
			return xerrors.Errorf("getting full node api: %w", err)
		}
		defer ncloser()

		ctx := lcli.DaemonContext(cctx)

		log.Debug("Checking full node version")

		v, err := fullnodeApi.Version(ctx)
		if err != nil {
			return err
		}

		if !v.APIVersion.EqMajorMinor(lapi.FullAPIVersion1) {
			return xerrors.Errorf("Remote API version didn't match (expected %s, remote %s)", lapi.FullAPIVersion1, v.APIVersion)
		}

		log.Debug("Checking full node sync status")

		if !cctx.Bool("nosync") {
			if err := lcli.SyncWait(ctx, &v0api.WrapperV1Full{FullNode: fullnodeApi}, false); err != nil {
				return xerrors.Errorf("sync wait: %w", err)
			}
		}

		boostRepoPath := cctx.String(FlagBoostRepo)
		r, err := repo.NewFS(boostRepoPath)
		if err != nil {
			return err
		}

		shutdownChan := make(chan struct{})

		log.Debug("Instantiating new boost node")

		var boostApi api.Boost
		stop, err := node.New(ctx,
			node.Boost(&boostApi),
			node.Override(new(dtypes.ShutdownChan), shutdownChan),
			node.Base(),
			node.Repo(r),
			node.Override(new(v1api.FullNode), fullnodeApi),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		log.Debug("Getting API endpoint of boost node")

		endpoint, err := r.APIEndpoint()
		if err != nil {
			return xerrors.Errorf("getting API endpoint: %w", err)
		}

		log.Debug("Bootstrapping libp2p network with full node")

		// Bootstrap with full node
		remoteAddrs, err := fullnodeApi.NetAddrsListen(ctx)
		if err != nil {
			return xerrors.Errorf("getting full node libp2p address: %w", err)
		}

		if err := boostApi.NetConnect(ctx, remoteAddrs); err != nil {
			return xerrors.Errorf("connecting to full node (libp2p): %w", err)
		}

		log.Debugw("Remote full node version", "version", v)

		// Instantiate the boost service JSON RPC handler.
		handler, err := node.BoostHandler(boostApi, true)
		if err != nil {
			return xerrors.Errorf("failed to instantiate rpc handler: %w", err)
		}

		log.Debugw("Boost JSON RPC server is listening", "endpoint", endpoint)

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
