package itests

import (
	"context"
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
)

var log = logging.Logger("boosttest")

func init() {
	_ = logging.SetLogLevel("boosttest", "DEBUG")
	_ = logging.SetLogLevel("devnet", "DEBUG")
}

func TestDummydeal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go devnet.Run(ctx, done)

	//TODO: detect properly when devnet (daemon+miner) are ready to serve requests
	time.Sleep(45 * time.Second)

	boostApi, stop := runBoost(t)

	res, err := boostApi.MarketDummyDeal(ctx)
	require.NoError(t, err)

	log.Debugw("Got response from MarketDummyDeal", "res", spew.Sdump(res))

	cancel()

	go func() { stop() }()

	<-done
}

func runBoost(t *testing.T) (api.Boost, func()) {
	ctx := context.Background()
	addr := "ws://127.0.0.1:1234/rpc/v1"

	fullnodeApi, closer, err := client.NewFullNodeRPCV1(ctx, addr, nil)
	require.NoError(t, err)

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

	var api api.Boost
	stop, err := node.New(ctx,
		node.Boost(&api),
		node.Override(new(dtypes.ShutdownChan), shutdownChan),
		node.Base(),
		node.Repo(r),
		node.Override(new(v1api.FullNode), fullnodeApi),
	)
	require.NoError(t, err)

	// Bootstrap with full node
	remoteAddrs, err := fullnodeApi.NetAddrsListen(ctx)
	require.NoError(t, err)

	log.Debugw("bootstrapping libp2p network with full node", "maadr", remoteAddrs)

	err = api.NetConnect(ctx, remoteAddrs)
	require.NoError(t, err)

	// Instantiate the boost service JSON RPC handler.
	handler, err := node.BoostHandler(api, true)
	require.NoError(t, err)

	log.Debug("getting API endpoint of boost node")

	endpoint, err := r.APIEndpoint()
	require.NoError(t, err)

	log.Debugw("json rpc server listening", "endpoint", endpoint)

	// Serve the RPC.
	rpcStopper, err := node.ServeRPC(handler, "boost", endpoint)
	require.NoError(t, err)

	log.Debugw("monitoring for shutdown")

	// Monitor for shutdown.
	finishCh := node.MonitorShutdown(shutdownChan,
		node.ShutdownHandler{Component: "rpc server", StopFunc: rpcStopper},
		node.ShutdownHandler{Component: "boost", StopFunc: stop},
	)

	return api, func() {
		shutdownChan <- struct{}{}
		stop(ctx)
		<-finishCh
		closer()
	}
}
