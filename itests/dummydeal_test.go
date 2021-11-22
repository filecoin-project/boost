package itests

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	lapi "github.com/filecoin-project/lotus/api"

	chaintypes "github.com/filecoin-project/lotus/chain/types"

	"github.com/filecoin-project/boost/storagemarket"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/testutil"
	types2 "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/boost/util"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/google/uuid"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/boost/pkg/devnet"
	"github.com/filecoin-project/lotus/api/client"

	cliutil "github.com/filecoin-project/boost/cli/util"
	"github.com/filecoin-project/lotus/api/v1api"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
)

var log = logging.Logger("boosttest")

func init() {
	_ = logging.SetLogLevel("boosttest", "DEBUG")
	_ = logging.SetLogLevel("devnet", "DEBUG")
	_ = logging.SetLogLevel("boost", "DEBUG")
	_ = logging.SetLogLevel("provider", "DEBUG")
	_ = logging.SetLogLevel("http-transfer", "DEBUG")
}

func TestDummydeal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go devnet.Run(ctx, done)

	// Wait for the miner to start up by polling it
	minerReadyCmd := "lotus-miner sectors list"
	for waitAttempts := 0; ; waitAttempts++ {
		// Check every second
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}

		cmd := exec.CommandContext(ctx, "sh", "-c", minerReadyCmd)
		_, err := cmd.CombinedOutput()
		if err != nil {
			// Still not ready
			if waitAttempts%5 == 0 {
				log.Debugw("miner not ready")
			}
			continue
		}

		// Miner is ready
		log.Debugw("miner ready")
		time.Sleep(2 * time.Second)
		break
	}

	// Start up boost
	boostApi, providerAddr, stop := runBoost(t)

	// Create a CAR file
	carRes, err := testutil.CreateRandomCARv1(5, 1600)
	require.NoError(t, err)

	// Start a web server to serve the file
	server, err := runWebServer(carRes.CarFile)
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	dealUuid := uuid.New()

	res, err := makeDummyDeal(ctx, boostApi, providerAddr, dealUuid, carRes, server.URL)
	require.NoError(t, err)

	// Wait for the deal to reach the Published state
	err = waitForPublished(ctx, boostApi, dealUuid)
	require.NoError(t, err)

	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))

	cancel()

	go func() { stop() }()

	<-done
}

func waitForPublished(ctx context.Context, boostApi api.Boost, dealUuid uuid.UUID) error {
	publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for {
		deal, err := boostApi.Deal(ctx, dealUuid)
		if err != nil && !xerrors.Is(err, storagemarket.ErrDealNotFound) {
			return fmt.Errorf("error getting deal: %s", err.Error())
		}

		if err == nil {
			if deal.Err != "" {
				return fmt.Errorf(deal.Err)
			}

			log.Infof("deal state: %s", deal.Checkpoint)
			switch {
			case deal.Checkpoint == dealcheckpoints.Complete:
				return nil
			case deal.Checkpoint == dealcheckpoints.Published:
				return nil
			}
		}

		select {
		case <-publishCtx.Done():
			return fmt.Errorf("timed out waiting for deal publish")
		case <-time.After(time.Second):
		}
	}
}

func runWebServer(path string) (*httptest.Server, error) {
	// start server with data to send
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
			return
		}

		http.ServeContent(w, r, "", time.Now(), file)
	}))

	return svr, nil
}

func makeDummyDeal(ctx context.Context, boostApi api.Boost, provAddr address.Address, dealUuid uuid.UUID, carRes *testutil.CarRes, url string) (*api.ProviderDealRejectionInfo, error) {
	pieceCid, pieceSize, err := util.CommP(ctx, carRes.Blockstore, carRes.Root)
	if err != nil {
		return nil, err
	}

	clientAddr, err := address.NewIDAddress(1234)
	if err != nil {
		return nil, err
	}

	proposal := market.ClientDealProposal{
		Proposal: market.DealProposal{
			PieceCID:             pieceCid,
			PieceSize:            pieceSize.Padded(),
			VerifiedDeal:         false,
			Client:               clientAddr,
			Provider:             provAddr,
			Label:                carRes.Root.String(),
			StartEpoch:           abi.ChainEpoch(rand.Intn(100000)),
			EndEpoch:             800000 + abi.ChainEpoch(rand.Intn(10000)),
			StoragePricePerEpoch: abi.NewTokenAmount(rand.Int63()),
			ProviderCollateral:   abi.NewTokenAmount(0),
			ClientCollateral:     abi.NewTokenAmount(0),
		},
		ClientSignature: crypto.Signature{
			Type: crypto.SigTypeBLS,
			Data: []byte("sig"),
		},
	}

	// Save the path to the CAR file as a transfer parameter
	transferParams := &types2.HttpRequest{URL: url}
	paramsBytes, err := json.Marshal(transferParams)
	if err != nil {
		return nil, err
	}

	peerID, err := boostApi.ID(ctx)
	if err != nil {
		return nil, err
	}

	dealParams := &types.ClientDealParams{
		DealUuid:           dealUuid,
		MinerPeerID:        peerID,
		ClientPeerID:       peerID,
		ClientDealProposal: proposal,
		DealDataRoot:       carRes.Root,
		Transfer: types.Transfer{
			Type:   "http",
			Params: paramsBytes,
			Size:   carRes.CarSize,
		},
	}

	return boostApi.MarketDummyDeal(ctx, dealParams)
}

func runBoost(t *testing.T) (api.Boost, address.Address, func()) {
	ctx := context.Background()
	addr := "ws://127.0.0.1:1234/rpc/v1"

	fullnodeApiString, err := devnet.GetFullnodeEndpoint(ctx)
	require.NoError(t, err)

	apiinfo := cliutil.ParseApiInfo(fullnodeApiString)

	fullnodeApi, closer, err := client.NewFullNodeRPCV1(ctx, addr, apiinfo.AuthHeader())
	require.NoError(t, err)

	defaultWallet, err := fullnodeApi.WalletDefaultAddress(ctx)
	require.NoError(t, err)

	bal, err := fullnodeApi.WalletBalance(ctx, defaultWallet)
	require.NoError(t, err)

	log.Infof("default wallet has %d attoFIL", bal)

	// Create a wallet for publish storage deals with some funds
	psdWalletAddr, err := fullnodeApi.WalletNew(ctx, chaintypes.KTSecp256k1)
	require.NoError(t, err)

	sendFunds(ctx, fullnodeApi, psdWalletAddr, abi.NewTokenAmount(1e18))
	log.Info("Created publish storage deals wallet")

	//// Create a wallet for deal collateral with some funds
	//collatWalletAddr, err := fullnodeApi.WalletNew(ctx, chaintypes.KTSecp256k1)
	//require.NoError(t, err)
	//
	//sendFunds(ctx, fullnodeApi, collatWalletAddr, abi.NewTokenAmount(1e18))
	//log.Info("Created collateral wallet")

	r := repo.NewMemory(nil)

	minerEndpoint, err := devnet.GetMinerEndpoint(ctx)
	require.NoError(t, err)

	minerApiInfo := cliutil.ParseApiInfo(minerEndpoint)
	minerConnAddr := "http://127.0.0.1:2345/rpc/v1" // TODO: parse this from minerEndpoint
	minerApi, closer, err := client.NewStorageMinerRPCV0(ctx, minerConnAddr, minerApiInfo.AuthHeader())
	require.NoError(t, err)

	// TODO: minerAddr is always ""
	// Need to figure out how to set it on the miner
	minerAddr, err := minerApi.ActorAddress(ctx)
	require.NoError(t, err)

	//actorAddr, err := address.NewFromString("t01000")
	//require.NoError(t, err)
	//
	//return fullnodeApi.StateMinerInfo(ctx, actorAddr, chaintypes.EmptyTSK)

	mi, err := fullnodeApi.StateMinerInfo(ctx, minerAddr, chaintypes.EmptyTSK)
	require.NoError(t, err)

	lr, err := r.Lock(repo.Boost)
	require.NoError(t, err)

	c, err := lr.Config()
	require.NoError(t, err)

	cfg, ok := c.(*config.Boost)
	if !ok {
		t.Fatalf("invalid config from repo, got: %T", c)
	}
	cfg.SectorIndexApiInfo = minerEndpoint
	cfg.Wallets.Miner = mi.Owner.String()
	cfg.Wallets.PublishStorageDeals = psdWalletAddr.String()
	cfg.Dealmaking.PublishMsgMaxDealsPerMsg = 1

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

	return api, minerAddr, func() {
		shutdownChan <- struct{}{}
		_ = stop(ctx)
		<-finishCh
		closer()
	}
}

func sendFunds(ctx context.Context, sender lapi.FullNode, recipient address.Address, amount abi.TokenAmount) error {
	senderAddr, err := sender.WalletDefaultAddress(ctx)
	if err != nil {
		return err
	}

	msg := &chaintypes.Message{
		From:  senderAddr,
		To:    recipient,
		Value: amount,
	}

	sm, err := sender.MpoolPushMessage(ctx, msg, nil)
	if err != nil {
		return err
	}

	_, err = sender.StateWaitMsg(ctx, sm.Cid(), 1, 1e10, true)
	return err
}
