package itests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/boost/pkg/devnet"
	"golang.org/x/xerrors"
)

func TestDummydeal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go devnet.Run(ctx, done)

	time.Sleep(45 * time.Second)

	err := runBoost()
	if err != nil {
		panic(err)
	}

	<-done
}

func runBoost() error {
	//fullnodeApi, ncloser, err := lcli.GetFullNodeAPIV1(cctx)
	//if err != nil {
	//return xerrors.Errorf("getting full node api: %w", err)
	//}
	//defer ncloser()

	r := repo.NewMemory(nil)

	ctx := context.Background()

	shutdownChan := make(chan struct{})

	var boostApi api.Boost
	stop, err := node.New(ctx,
		node.Boost(&boostApi),
		node.Override(new(dtypes.ShutdownChan), shutdownChan),
		node.Base(),
		node.Repo(r),
		//node.Override(new(v1api.FullNode), fullnodeApi),
	)
	if err != nil {
		return xerrors.Errorf("creating node: %w", err)
	}

	// Bootstrap with full node
	//remoteAddrs, err := fullnodeApi.NetAddrsListen(ctx)
	//if err != nil {
	//return xerrors.Errorf("getting full node libp2p address: %w", err)
	//}

	//log.Debugw("Bootstrapping boost libp2p network with full node", "maadr", remoteAddrs)

	//if err := boostApi.NetConnect(ctx, remoteAddrs); err != nil {
	//return xerrors.Errorf("connecting to full node (libp2p): %w", err)
	//}

	// Instantiate the boost service JSON RPC handler.
	handler, err := node.BoostHandler(boostApi, true)
	if err != nil {
		return xerrors.Errorf("failed to instantiate rpc handler: %w", err)
	}

	//log.Debugw("Boost JSON RPC server is listening", "endpoint", endpoint)

	endpoint, err := r.APIEndpoint()
	if err != nil {
		return xerrors.Errorf("getting API endpoint: %w", err)
	}

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
}
