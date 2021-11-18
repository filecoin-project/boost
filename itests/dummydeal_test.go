package itests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/boost/pkg/devnet"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v1api"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"
)

var log = logging.Logger("boosttest")

func TestDummydeal(t *testing.T) {
	_ = logging.SetLogLevel("boosttest", "DEBUG")
	_ = logging.SetLogLevel("devnet", "DEBUG")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go devnet.Run(ctx, done)

	time.Sleep(45 * time.Second)

	err := runBoost(t)
	require.NoError(t, err)

	cancel()

	<-done
}

func runBoost(t *testing.T) error {
	ctx := context.Background()
	addr := "ws://127.0.0.1:1234/rpc/v1"

	fullnodeApi, closer, err := client.NewFullNodeRPCV1(ctx, addr, nil)
	require.NoError(t, err)
	defer closer()

	r := repo.NewMemory(nil)

	creds, err := devnet.GetMinerEndpoint(ctx)
	require.NoError(t, err)

	lr, err := r.Lock(repo.Boost)
	require.NoError(t, err)

	c, err := lr.Config()
	require.NoError(t, err)

	cfg, ok := c.(*config.Boost)
	if !ok {
		t.Fatalf("invalid config from repo, got: %T", c)
	}
	cfg.SectorIndexApiInfo = creds

	err = lr.SetConfig(func(raw interface{}) {
		rcfg := raw.(*config.Boost)
		*rcfg = *cfg
	})
	require.NoError(t, err)

	err = lr.Close()
	require.NoError(t, err)

	shutdownChan := make(chan struct{})

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

	// Bootstrap with full node
	remoteAddrs, err := fullnodeApi.NetAddrsListen(ctx)
	if err != nil {
		return xerrors.Errorf("getting full node libp2p address: %w", err)
	}

	log.Debugw("Bootstrapping boost libp2p network with full node", "maadr", remoteAddrs)

	if err := boostApi.NetConnect(ctx, remoteAddrs); err != nil {
		return xerrors.Errorf("connecting to full node (libp2p): %w", err)
	}

	// Instantiate the boost service JSON RPC handler.
	handler, err := node.BoostHandler(boostApi, true)
	if err != nil {
		return xerrors.Errorf("failed to instantiate rpc handler: %w", err)
	}

	log.Debug("Getting API endpoint of boost node")

	endpoint, err := r.APIEndpoint()
	if err != nil {
		return xerrors.Errorf("getting API endpoint: %w", err)
	}

	log.Debugw("Boost JSON RPC server is listening", "endpoint", endpoint)

	// Serve the RPC.
	rpcStopper, err := node.ServeRPC(handler, "boost", endpoint)
	if err != nil {
		return fmt.Errorf("failed to start json-rpc endpoint: %s", err)
	}

	log.Debugw("Monitoring for shutdown")

	// Monitor for shutdown.
	finishCh := node.MonitorShutdown(shutdownChan,
		node.ShutdownHandler{Component: "rpc server", StopFunc: rpcStopper},
		node.ShutdownHandler{Component: "boost", StopFunc: stop},
	)

	res, err := boostApi.MarketDummyDeal(ctx)
	require.NoError(t, err)

	log.Debugw("Got response from MarketDummyDeal", "res", spew.Sdump(res))

	go func() { shutdownChan <- struct{}{} }()

	go func() { stop(ctx); <-finishCh }()

	return nil
}
