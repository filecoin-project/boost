package itests

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/build"
	cliutil "github.com/filecoin-project/boost/cli/util"
	boostclient "github.com/filecoin-project/boost/client"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/pkg/devnet"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/testutil"
	types2 "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	chaintypes "github.com/filecoin-project/lotus/chain/types"
	lotus_config "github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/modules"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	miner2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
)

var log = logging.Logger("boosttest")

func init() {
	build.MessageConfidence = 1
}

func setLogLevel() {
	_ = logging.SetLogLevel("boosttest", "DEBUG")
	_ = logging.SetLogLevel("devnet", "DEBUG")
	_ = logging.SetLogLevel("boost", "DEBUG")
	_ = logging.SetLogLevel("actors", "DEBUG")
	_ = logging.SetLogLevel("provider", "DEBUG")
	_ = logging.SetLogLevel("http-transfer", "DEBUG")
	_ = logging.SetLogLevel("boost-provider", "DEBUG")
	_ = logging.SetLogLevel("storagemanager", "DEBUG")
}

func TestDummydeal(t *testing.T) {
	setLogLevel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempHome := t.TempDir()

	done := make(chan struct{})
	go devnet.Run(ctx, tempHome, done)

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
		cmd.Env = []string{fmt.Sprintf("HOME=%s", tempHome)}
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
		time.Sleep(5 * time.Second) // wait for AddPiece
		break
	}

	f := newTestFramework(ctx, t, tempHome)
	f.start()

	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, 2000000)
	require.NoError(t, err)

	failingFilepath, err := testutil.CreateRandomFile(tempdir, 5, 2000000)
	require.NoError(t, err)

	rootCid, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)

	failingRootCid, failingCarFilepath, err := testutil.CreateDenseCARv2(tempdir, failingFilepath)
	require.NoError(t, err)

	// Start a web server to serve the car files
	server, err := testutil.HttpTestFileServer(t, tempdir)
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	dealUuid := uuid.New()

	res, err := f.makeDummyDeal(dealUuid, carFilepath, rootCid, server.URL+"/"+filepath.Base(carFilepath))
	require.NoError(t, err)
	require.True(t, res.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))

	time.Sleep(2 * time.Second)

	failingDealUuid := uuid.New()
	res2, err2 := f.makeDummyDeal(failingDealUuid, failingCarFilepath, failingRootCid, server.URL+"/"+filepath.Base(failingCarFilepath))
	require.NoError(t, err2)
	require.Equal(t, "cannot accept piece of size 2254421, on top of already allocated 2254421 bytes, because it would exceed max staging area size 4000000", res2.Reason)
	log.Debugw("got response from MarketDummyDeal for failing deal", "res2", spew.Sdump(res2))

	// Wait for the deal to be added to a sector and be cleanedup so space is made
	err = f.waitForDealAddedToSector(dealUuid)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	passingDealUuid := uuid.New()
	res2, err2 = f.makeDummyDeal(passingDealUuid, failingCarFilepath, failingRootCid, server.URL+"/"+filepath.Base(failingCarFilepath))
	require.NoError(t, err2)
	require.True(t, res2.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res2", spew.Sdump(res2))

	// Wait for the deal to be added to a sector
	err = f.waitForDealAddedToSector(passingDealUuid)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	cancel()
	go f.stop()
	<-done
}

type testFramework struct {
	ctx     context.Context
	t       *testing.T
	homedir string
	stop    func()

	client     *boostclient.StorageClient
	boost      api.Boost
	fullNode   lapi.FullNode
	clientAddr address.Address
	minerAddr  address.Address
}

func newTestFramework(ctx context.Context, t *testing.T, homedir string) *testFramework {
	return &testFramework{
		ctx:     ctx,
		t:       t,
		homedir: homedir,
	}
}

func (f *testFramework) start() {
	var err error
	f.client, err = boostclient.NewStorageClient(f.ctx)
	require.NoError(f.t, err)

	addr := "ws://127.0.0.1:1234/rpc/v1"

	// Get a FullNode API
	fullnodeApiString, err := devnet.GetFullnodeEndpoint(f.ctx, f.homedir)
	require.NoError(f.t, err)

	apiinfo := cliutil.ParseApiInfo(fullnodeApiString)

	fullnodeApi, closerFullnode, err := client.NewFullNodeRPCV1(f.ctx, addr, apiinfo.AuthHeader())
	require.NoError(f.t, err)

	f.fullNode = fullnodeApi

	err = fullnodeApi.LogSetLevel(f.ctx, "actors", "DEBUG")
	require.NoError(f.t, err)

	wallets, err := fullnodeApi.WalletList(f.ctx)
	require.NoError(f.t, err)

	// Set the default wallet for the devnet daemon
	err = fullnodeApi.WalletSetDefault(f.ctx, wallets[0])
	require.NoError(f.t, err)

	// Make sure that default wallet has been setup successfully
	defaultWallet, err := fullnodeApi.WalletDefaultAddress(f.ctx)
	require.NoError(f.t, err)

	bal, err := fullnodeApi.WalletBalance(f.ctx, defaultWallet)
	require.NoError(f.t, err)

	log.Infof("default wallet %s has %d attoFIL", defaultWallet, bal)

	// Create a wallet for the client with some funds
	var wg sync.WaitGroup
	wg.Add(1)
	var clientAddr address.Address
	go func() {
		log.Info("Creating client wallet")

		clientAddr, err = fullnodeApi.WalletNew(f.ctx, chaintypes.KTBLS)
		require.NoError(f.t, err)

		amt := abi.NewTokenAmount(1e18)
		_ = sendFunds(f.ctx, fullnodeApi, clientAddr, amt)
		log.Infof("Created client wallet %s with %d attoFil", clientAddr, amt)
		wg.Done()
	}()

	// Create a wallet for publish storage deals with some funds
	wg.Add(1)
	var psdWalletAddr address.Address
	go func() {
		log.Info("Creating publish storage deals wallet")
		psdWalletAddr, err = fullnodeApi.WalletNew(f.ctx, chaintypes.KTBLS)
		require.NoError(f.t, err)

		amt := abi.NewTokenAmount(1e18)
		_ = sendFunds(f.ctx, fullnodeApi, psdWalletAddr, amt)
		log.Infof("Created publish storage deals wallet %s with %d attoFil", psdWalletAddr, amt)
		wg.Done()
	}()
	wg.Wait()

	f.clientAddr = clientAddr

	minerEndpoint, err := devnet.GetMinerEndpoint(f.ctx, f.homedir)
	require.NoError(f.t, err)

	minerApiInfo := cliutil.ParseApiInfo(minerEndpoint)
	minerConnAddr := "ws://127.0.0.1:2345/rpc/v0"
	minerApi, closerMiner, err := client.NewStorageMinerRPCV0(f.ctx, minerConnAddr, minerApiInfo.AuthHeader())
	require.NoError(f.t, err)

	log.Debugw("minerApiInfo.Address", "addr", minerApiInfo.Addr)

	minerAddr, err := minerApi.ActorAddress(f.ctx)
	require.NoError(f.t, err)

	log.Debugw("got miner actor addr", "addr", minerAddr)

	f.minerAddr = minerAddr

	// Set the control address for the storage provider to be the publish
	// storage deals wallet
	f.setControlAddress(psdWalletAddr)

	// Create an in-memory repo
	r := lotus_repo.NewMemory(nil)

	lr, err := r.Lock(node.BoostRepoType{})
	require.NoError(f.t, err)

	ds, err := lr.Datastore(context.Background(), "/metadata")
	require.NoError(f.t, err)

	err = ds.Put(context.Background(), datastore.NewKey("miner-address"), minerAddr.Bytes())
	require.NoError(f.t, err)

	// Set some config values on the repo
	c, err := lr.Config()
	require.NoError(f.t, err)

	cfg, ok := c.(*config.Boost)
	if !ok {
		f.t.Fatalf("invalid config from repo, got: %T", c)
	}
	cfg.SectorIndexApiInfo = minerEndpoint
	cfg.SealerApiInfo = minerEndpoint
	cfg.Wallets.Miner = minerAddr.String()
	cfg.Wallets.PublishStorageDeals = psdWalletAddr.String()
	cfg.Dealmaking.PublishMsgMaxDealsPerMsg = 1
	cfg.Dealmaking.PublishMsgPeriod = config.Duration(time.Second * 1)
	cfg.Dealmaking.MaxStagingDealsBytes = 4000000

	err = lr.SetConfig(func(raw interface{}) {
		rcfg := raw.(*config.Boost)
		*rcfg = *cfg
	})
	require.NoError(f.t, err)

	err = lr.Close()
	require.NoError(f.t, err)

	shutdownChan := make(chan struct{})

	// Create Boost API
	stop, err := node.New(f.ctx,
		node.Boost(&f.boost),
		node.Override(new(dtypes.ShutdownChan), shutdownChan),
		node.Base(),
		node.Repo(r),
		node.Override(new(v1api.FullNode), fullnodeApi),

		node.Override(new(*storage.AddressSelector), modules.AddressSelector(&lotus_config.MinerAddressConfig{
			DealPublishControl: []string{
				psdWalletAddr.String(),
			},
			DisableOwnerFallback:  true,
			DisableWorkerFallback: true,
		})),
	)
	require.NoError(f.t, err)

	// Bootstrap libp2p with full node
	remoteAddrs, err := fullnodeApi.NetAddrsListen(f.ctx)
	require.NoError(f.t, err)

	log.Debugw("bootstrapping libp2p network with full node", "maadr", remoteAddrs)

	err = f.boost.NetConnect(f.ctx, remoteAddrs)
	require.NoError(f.t, err)

	// Instantiate the boost service JSON RPC handler.
	handler, err := node.BoostHandler(f.boost, true)
	require.NoError(f.t, err)

	log.Debug("getting API endpoint of boost node")

	endpoint, err := r.APIEndpoint()
	require.NoError(f.t, err)

	log.Debugw("json rpc server listening", "endpoint", endpoint)

	// Serve the RPC.
	rpcStopper, err := node.ServeRPC(handler, "boost", endpoint)
	require.NoError(f.t, err)

	// Add boost libp2p address to test client peer store so the client knows
	// how to connect to boost
	boostAddrs, err := f.boost.NetAddrsListen(f.ctx)
	require.NoError(f.t, err)
	f.client.PeerStore.AddAddrs(boostAddrs.ID, boostAddrs.Addrs, time.Hour)

	log.Debugw("monitoring for shutdown")

	// Monitor for shutdown.
	finishCh := node.MonitorShutdown(shutdownChan,
		node.ShutdownHandler{Component: "rpc server", StopFunc: rpcStopper},
		node.ShutdownHandler{Component: "boost", StopFunc: stop},
	)

	f.stop = func() {
		shutdownChan <- struct{}{}
		_ = stop(f.ctx)
		<-finishCh
		closerFullnode()
		closerMiner()
	}
}

func (f *testFramework) waitForDealAddedToSector(dealUuid uuid.UUID) error {
	publishCtx, cancel := context.WithTimeout(f.ctx, 300*time.Second)
	defer cancel()

	for {
		deal, err := f.boost.Deal(f.ctx, dealUuid)
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
			case deal.Checkpoint == dealcheckpoints.IndexedAndAnnounced:
				return nil
			}
		}

		select {
		case <-publishCtx.Done():
			return fmt.Errorf("timed out waiting for deal to be added to a sector")
		case <-time.After(time.Second):
		}
	}
}

func (f *testFramework) makeDummyDeal(dealUuid uuid.UUID, carFilepath string, rootCid cid.Cid, url string) (*api.ProviderDealRejectionInfo, error) {
	cidAndSize, err := storagemarket.GenerateCommP(carFilepath)
	if err != nil {
		return nil, err
	}

	proposal := market.DealProposal{
		PieceCID:             cidAndSize.PieceCID,
		PieceSize:            cidAndSize.PieceSize,
		VerifiedDeal:         false,
		Client:               f.clientAddr,
		Provider:             f.minerAddr,
		Label:                rootCid.String(),
		StartEpoch:           abi.ChainEpoch(rand.Intn(100000)),
		EndEpoch:             800000 + abi.ChainEpoch(rand.Intn(10000)),
		StoragePricePerEpoch: abi.NewTokenAmount(1),
		ProviderCollateral:   abi.NewTokenAmount(0),
		ClientCollateral:     abi.NewTokenAmount(0),
	}

	signedProposal, err := f.signProposal(f.clientAddr, &proposal)
	if err != nil {
		return nil, err
	}

	log.Debugf("Client balance requirement for deal: %d attoFil", proposal.ClientBalanceRequirement())
	log.Debugf("Provider balance requirement for deal: %d attoFil", proposal.ProviderBalanceRequirement())

	clientBal, err := f.fullNode.WalletBalance(f.ctx, f.clientAddr)
	if err != nil {
		return nil, err
	}
	log.Debugf("Client balance: %d attoFil", clientBal)

	provBal, err := f.fullNode.WalletBalance(f.ctx, f.minerAddr)
	if err != nil {
		return nil, err
	}
	log.Debugf("Provider balance: %d attoFil", provBal)

	// Add client and provider funds for deal to StorageMarketActor
	var errgp errgroup.Group
	errgp.Go(func() error {
		bal := proposal.ClientBalanceRequirement()
		log.Infof("adding client balance requirement %d to Storage Market Actor", bal)
		if big.Cmp(bal, big.Zero()) > 0 {
			mcid, err := f.fullNode.MarketAddBalance(f.ctx, f.clientAddr, f.clientAddr, bal)
			if err != nil {
				return err
			}
			return f.WaitMsg(mcid)
		}
		return nil
	})
	errgp.Go(func() error {
		bal := proposal.ProviderBalanceRequirement()
		log.Infof("adding provider balance requirement %d to Storage Market Actor", bal)
		if big.Cmp(bal, big.Zero()) > 0 {
			mcid, err := f.fullNode.MarketAddBalance(f.ctx, f.minerAddr, f.minerAddr, bal)
			if err != nil {
				return err
			}
			return f.WaitMsg(mcid)
		}
		return nil
	})
	err = errgp.Wait()
	if err != nil {
		return nil, err
	}

	log.Info("done adding balance requirements")

	// Save the path to the CAR file as a transfer parameter
	transferParams := &types2.HttpRequest{URL: url}
	transferParamsJSON, err := json.Marshal(transferParams)
	if err != nil {
		return nil, err
	}

	peerID, err := f.boost.ID(f.ctx)
	if err != nil {
		return nil, err
	}

	carFileinfo, err := os.Stat(carFilepath)
	if err != nil {
		return nil, err
	}

	dealParams := types.DealParams{
		DealUUID:           dealUuid,
		ClientDealProposal: *signedProposal,
		DealDataRoot:       rootCid,
		Transfer: types.Transfer{
			Type:   "http",
			Params: transferParamsJSON,
			Size:   uint64(carFileinfo.Size()),
		},
	}

	return f.client.StorageDeal(f.ctx, dealParams, peerID)
}

func (f *testFramework) signProposal(addr address.Address, proposal *market.DealProposal) (*market.ClientDealProposal, error) {
	buf, err := cborutil.Dump(proposal)
	if err != nil {
		return nil, err
	}

	sig, err := f.fullNode.WalletSign(f.ctx, addr, buf)
	if err != nil {
		return nil, err
	}

	return &market.ClientDealProposal{
		Proposal:        *proposal,
		ClientSignature: *sig,
	}, nil
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

func (f *testFramework) setControlAddress(psdAddr address.Address) {
	mi, err := f.fullNode.StateMinerInfo(f.ctx, f.minerAddr, chaintypes.EmptyTSK)
	require.NoError(f.t, err)

	cwp := &miner2.ChangeWorkerAddressParams{
		NewWorker:       mi.Worker,
		NewControlAddrs: []address.Address{psdAddr},
	}
	sp, err := actors.SerializeParams(cwp)
	require.NoError(f.t, err)

	smsg, err := f.fullNode.MpoolPushMessage(f.ctx, &chaintypes.Message{
		From:   mi.Owner,
		To:     f.minerAddr,
		Method: miner.Methods.ChangeWorkerAddress,

		Value:  big.Zero(),
		Params: sp,
	}, nil)
	require.NoError(f.t, err)

	err = f.WaitMsg(smsg.Cid())
	require.NoError(f.t, err)
}

func (f *testFramework) WaitMsg(mcid cid.Cid) error {
	_, err := f.fullNode.StateWaitMsg(f.ctx, mcid, 1, 1e10, true)
	return err
}
