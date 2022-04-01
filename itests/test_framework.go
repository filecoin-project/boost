package itests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/filecoin-project/lotus/api/v0api"

	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/api"

	cliutil "github.com/filecoin-project/boost/cli/util"
	boostclient "github.com/filecoin-project/boost/client"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/pkg/devnet"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	types2 "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	lbuild "github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"

	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	chaintypes "github.com/filecoin-project/lotus/chain/types"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	lotus_config "github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/modules"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage"

	lotus_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	miner2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/miner"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("boosttest")

type testFramework struct {
	ctx     context.Context
	homedir string
	stop    func()

	client        *boostclient.StorageClient
	boost         api.Boost
	fullNode      v0api.FullNode
	clientAddr    address.Address
	minerAddr     address.Address
	defaultWallet address.Address
}

func newTestFramework(ctx context.Context, homedir string) *testFramework {
	return &testFramework{
		ctx:     ctx,
		homedir: homedir,
	}
}

func (f *testFramework) start() error {
	addr := "ws://127.0.0.1:1234/rpc/v1"

	// Get a FullNode API
	fullnodeApiString, err := devnet.GetFullnodeEndpoint(f.ctx, f.homedir)
	if err != nil {
		return err
	}

	apiinfo := cliutil.ParseApiInfo(fullnodeApiString)

	fullnodeApi, closerFullnode, err := client.NewFullNodeRPCV0(f.ctx, addr, apiinfo.AuthHeader())
	if err != nil {
		return err
	}

	f.fullNode = fullnodeApi

	err = fullnodeApi.LogSetLevel(f.ctx, "actors", "DEBUG")
	if err != nil {
		return err
	}

	wallets, err := fullnodeApi.WalletList(f.ctx)
	if err != nil {
		return err
	}

	// Set the default wallet for the devnet daemon
	err = fullnodeApi.WalletSetDefault(f.ctx, wallets[0])
	if err != nil {
		return err
	}

	// Make sure that default wallet has been setup successfully
	defaultWallet, err := fullnodeApi.WalletDefaultAddress(f.ctx)
	if err != nil {
		return err
	}

	f.defaultWallet = defaultWallet

	bal, err := fullnodeApi.WalletBalance(f.ctx, defaultWallet)
	if err != nil {
		return err
	}

	log.Infof("default wallet %s has %d attoFIL", defaultWallet, bal)

	// Create a wallet for the client with some funds
	var wg sync.WaitGroup
	wg.Add(1)
	var clientAddr address.Address
	go func() {
		log.Info("Creating client wallet")

		clientAddr, _ = fullnodeApi.WalletNew(f.ctx, chaintypes.KTBLS)

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
		psdWalletAddr, _ = fullnodeApi.WalletNew(f.ctx, chaintypes.KTBLS)

		amt := abi.NewTokenAmount(1e18)
		_ = sendFunds(f.ctx, fullnodeApi, psdWalletAddr, amt)
		log.Infof("Created publish storage deals wallet %s with %d attoFil", psdWalletAddr, amt)
		wg.Done()
	}()
	wg.Wait()

	f.clientAddr = clientAddr

	f.client, err = boostclient.NewStorageClient(f.clientAddr, f.fullNode)
	if err != nil {
		return err
	}

	minerEndpoint, err := devnet.GetMinerEndpoint(f.ctx, f.homedir)
	if err != nil {
		return err
	}

	minerApiInfo := cliutil.ParseApiInfo(minerEndpoint)
	minerConnAddr := "ws://127.0.0.1:2345/rpc/v0"
	minerApi, closerMiner, err := client.NewStorageMinerRPCV0(f.ctx, minerConnAddr, minerApiInfo.AuthHeader())
	if err != nil {
		return err
	}

	log.Debugw("minerApiInfo.Address", "addr", minerApiInfo.Addr)

	minerAddr, err := minerApi.ActorAddress(f.ctx)
	if err != nil {
		return err
	}

	log.Debugw("got miner actor addr", "addr", minerAddr)

	f.minerAddr = minerAddr

	// Set the control address for the storage provider to be the publish
	// storage deals wallet
	_ = f.setControlAddress(psdWalletAddr)

	// Create an in-memory repo
	r := lotus_repo.NewMemory(nil)

	lr, err := r.Lock(node.Boost)
	if err != nil {
		return err
	}

	ds, err := lr.Datastore(context.Background(), "/metadata")
	if err != nil {
		return err
	}

	err = ds.Put(context.Background(), datastore.NewKey("miner-address"), minerAddr.Bytes())
	if err != nil {
		return err
	}

	// Set some config values on the repo
	c, err := lr.Config()
	if err != nil {
		return err
	}

	cfg, ok := c.(*config.Boost)
	if !ok {
		return fmt.Errorf("invalid config from repo, got: %T", c)
	}
	cfg.SectorIndexApiInfo = minerEndpoint
	cfg.SealerApiInfo = minerEndpoint
	cfg.Wallets.Miner = minerAddr.String()
	cfg.Wallets.PublishStorageDeals = psdWalletAddr.String()
	cfg.Dealmaking.PublishMsgMaxDealsPerMsg = 1
	cfg.Dealmaking.PublishMsgPeriod = config.Duration(time.Second * 1)
	cfg.Dealmaking.MaxStagingDealsBytes = 4000000
	cfg.Storage.ParallelFetchLimit = 10

	err = lr.SetConfig(func(raw interface{}) {
		rcfg := raw.(*config.Boost)
		*rcfg = *cfg
	})
	if err != nil {
		return err
	}

	err = lr.Close()
	if err != nil {
		return err
	}

	shutdownChan := make(chan struct{})

	// Create Boost API
	stop, err := node.New(f.ctx,
		node.BoostAPI(&f.boost),
		node.Override(new(dtypes.ShutdownChan), shutdownChan),
		node.Base(),
		node.Repo(r),
		node.Override(new(v0api.FullNode), fullnodeApi),

		node.Override(new(*storage.AddressSelector), modules.AddressSelector(&lotus_config.MinerAddressConfig{
			DealPublishControl: []string{
				psdWalletAddr.String(),
			},
			DisableOwnerFallback:  true,
			DisableWorkerFallback: true,
		})),
	)
	if err != nil {
		return err
	}

	// Bootstrap libp2p with full node
	remoteAddrs, err := fullnodeApi.NetAddrsListen(f.ctx)
	if err != nil {
		return err
	}

	log.Debugw("bootstrapping libp2p network with full node", "maadr", remoteAddrs)

	err = f.boost.NetConnect(f.ctx, remoteAddrs)
	if err != nil {
		return err
	}

	// Instantiate the boost service JSON RPC handler.
	handler, err := node.BoostHandler(f.boost, true)
	if err != nil {
		return err
	}

	log.Debug("getting API endpoint of boost node")

	endpoint, err := r.APIEndpoint()
	if err != nil {
		return err
	}

	log.Debugw("json rpc server listening", "endpoint", endpoint)

	// Serve the RPC.
	rpcStopper, err := node.ServeRPC(handler, "boost", endpoint)
	if err != nil {
		return err
	}

	// Add boost libp2p address to test client peer store so the client knows
	// how to connect to boost
	boostAddrs, err := f.boost.NetAddrsListen(f.ctx)
	if err != nil {
		return err
	}
	f.client.PeerStore.AddAddrs(boostAddrs.ID, boostAddrs.Addrs, time.Hour)

	// Add boost libp2p to chain
	log.Debugw("serialize params")
	params, err := actors.SerializeParams(&miner2.ChangePeerIDParams{NewID: abi.PeerID(boostAddrs.ID)})
	if err != nil {
		return err
	}

	msg := &ltypes.Message{
		To: minerAddr,
		//From:   minerAddr,
		From:   defaultWallet,
		Method: miner.Methods.ChangePeerID,
		Params: params,
		Value:  ltypes.NewInt(0),
	}

	log.Debugw("push message to mpool")
	signed, err := fullnodeApi.MpoolPushMessage(f.ctx, msg, nil)
	if err != nil {
		return err
	}

	log.Debugw("wait for state msg")
	mw, err := fullnodeApi.StateWaitMsg(f.ctx, signed.Cid(), 2)
	if err != nil {
		return err
	}
	if exitcode.Ok != mw.Receipt.ExitCode {
		return errors.New("expected mw.Receipt.ExitCode to be OK")
	}

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

	return nil
}

func (f *testFramework) waitForDealAddedToSector(dealUuid uuid.UUID) error {
	publishCtx, cancel := context.WithTimeout(f.ctx, 300*time.Second)
	defer cancel()
	peerID, err := f.boost.ID(f.ctx)
	if err != nil {
		return err
	}

	for {
		resp, err := f.client.DealStatus(f.ctx, peerID, dealUuid)
		if err != nil && !xerrors.Is(err, storagemarket.ErrDealNotFound) {
			return fmt.Errorf("error getting status: %s", err.Error())
		}

		if err == nil && resp.Error == "" {
			log.Infof("deal state: %s", resp.DealStatus)
			switch {
			case resp.DealStatus.Status == dealcheckpoints.Complete.String():
				return nil
			case resp.DealStatus.Status == dealcheckpoints.IndexedAndAnnounced.String():
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

func (f *testFramework) makeDummyDeal(dealUuid uuid.UUID, carFilepath string, rootCid cid.Cid, url string, isOffline bool) (*api.ProviderDealRejectionInfo, error) {
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
		StartEpoch:           10000 + abi.ChainEpoch(rand.Intn(30000)),
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
		IsOffline:          isOffline,
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

func (f *testFramework) DefaultMarketsV1DealParams() lapi.StartDealParams {
	return lapi.StartDealParams{
		Data:              &lotus_storagemarket.DataRef{TransferType: lotus_storagemarket.TTGraphsync},
		EpochPrice:        ltypes.NewInt(62500000), // minimum asking price
		MinBlocksDuration: uint64(lbuild.MinDealDuration),
		Miner:             f.minerAddr,
		Wallet:            f.defaultWallet,
		DealStartEpoch:    30000 + abi.ChainEpoch(rand.Intn(10000)),
		FastRetrieval:     true,
	}
}

func sendFunds(ctx context.Context, sender v0api.FullNode, recipient address.Address, amount abi.TokenAmount) error {
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

	_, err = sender.StateWaitMsg(ctx, sm.Cid(), 1)
	return err
}

func (f *testFramework) setControlAddress(psdAddr address.Address) error {
	mi, err := f.fullNode.StateMinerInfo(f.ctx, f.minerAddr, chaintypes.EmptyTSK)
	if err != nil {
		return err
	}

	cwp := &miner2.ChangeWorkerAddressParams{
		NewWorker:       mi.Worker,
		NewControlAddrs: []address.Address{psdAddr},
	}
	sp, err := actors.SerializeParams(cwp)
	if err != nil {
		return err
	}

	smsg, err := f.fullNode.MpoolPushMessage(f.ctx, &chaintypes.Message{
		From:   mi.Owner,
		To:     f.minerAddr,
		Method: miner.Methods.ChangeWorkerAddress,

		Value:  big.Zero(),
		Params: sp,
	}, nil)
	if err != nil {
		return err
	}

	err = f.WaitMsg(smsg.Cid())
	if err != nil {
		return err
	}
	return nil
}

func (f *testFramework) WaitMsg(mcid cid.Cid) error {
	_, err := f.fullNode.StateWaitMsg(f.ctx, mcid, 1)
	return err
}

func (f *testFramework) WaitDealSealed(ctx context.Context, deal *cid.Cid) error {
	for {
		di, err := f.fullNode.ClientGetDealInfo(ctx, *deal)
		if err != nil {
			return err
		}

		switch di.State {
		case lotus_storagemarket.StorageDealAwaitingPreCommit, lotus_storagemarket.StorageDealSealing:
		case lotus_storagemarket.StorageDealProposalRejected:
			return errors.New("deal rejected")
		case lotus_storagemarket.StorageDealFailing:
			return errors.New("deal failed")
		case lotus_storagemarket.StorageDealError:
			return fmt.Errorf("deal errored: %s", di.Message)
		case lotus_storagemarket.StorageDealActive:
			return nil
		}

		time.Sleep(2 * time.Second)
	}
}
