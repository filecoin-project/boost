package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	gfm_storagemarket "github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost/api"
	boostclient "github.com/filecoin-project/boost/client"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	types2 "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	lotus_gfm_retrievalmarket "github.com/filecoin-project/go-fil-markets/retrievalmarket"
	lotus_gfm_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	minertypes "github.com/filecoin-project/go-state-types/builtin/v9/miner"
	"github.com/filecoin-project/go-state-types/exitcode"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	lbuild "github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	chaintypes "github.com/filecoin-project/lotus/chain/types"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/gateway"
	"github.com/filecoin-project/lotus/itests/kit"
	lnode "github.com/filecoin-project/lotus/node"
	lotus_config "github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/modules"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/modules/lp2p"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/ctladdr"
	"github.com/filecoin-project/lotus/storage/pipeline/sealiface"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/google/uuid"
	"github.com/ipfs/boxo/files"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	dstest "github.com/ipfs/boxo/ipld/merkledag/test"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var Log = logging.Logger("boosttest")

type TestFramework struct {
	ctx  context.Context
	Stop func()

	HomeDir       string
	Client        *boostclient.StorageClient
	Boost         api.Boost
	FullNode      *kit.TestFullNode
	LotusMiner    *kit.TestMiner
	ClientAddr    address.Address
	MinerAddr     address.Address
	DefaultWallet address.Address
}

func NewTestFramework(ctx context.Context, t *testing.T) *TestFramework {
	fullNode, miner := FullNodeAndMiner(t)

	return &TestFramework{
		ctx:        ctx,
		HomeDir:    t.TempDir(),
		FullNode:   fullNode,
		LotusMiner: miner,
	}
}

func FullNodeAndMiner(t *testing.T) (*kit.TestFullNode, *kit.TestMiner) {
	// Set up a full node and a miner (without markets)
	var fullNode kit.TestFullNode
	var miner kit.TestMiner

	// 8MiB sectors
	secSizeOpt := kit.SectorSize(8 << 20)
	minerOpts := []kit.NodeOpt{
		kit.WithSubsystems(kit.SSealing, kit.SSectorStorage, kit.SMining),
		kit.DisableLibp2p(),
		kit.ThroughRPC(),
		secSizeOpt,
		kit.ConstructorOpts(lnode.Options(
			lnode.Override(new(lotus_dtypes.GetSealingConfigFunc), func() (lotus_dtypes.GetSealingConfigFunc, error) {
				return func() (sealiface.Config, error) {
					cfg := lotus_config.DefaultStorageMiner()
					sc := modules.ToSealingConfig(cfg.Dealmaking, cfg.Sealing)
					sc.MaxWaitDealsSectors = 2
					sc.MaxSealingSectors = 1
					sc.MaxSealingSectorsForDeals = 3
					sc.AlwaysKeepUnsealedCopy = true
					sc.WaitDealsDelay = time.Hour
					sc.BatchPreCommits = false
					sc.AggregateCommits = false

					return sc, nil
				}, nil
			}),
		)),
	}
	fnOpts := []kit.NodeOpt{
		kit.ConstructorOpts(
			lnode.Override(new(lp2p.RawHost), func() (host.Host, error) {
				return libp2p.New(libp2p.DefaultTransports)
			}),
		),
		kit.ThroughRPC(),
		secSizeOpt,
	}

	eOpts := []kit.EnsembleOpt{
		//TODO: at the moment we are not mocking proofs
		//maybe enable this in the future to speed up tests further

		//kit.MockProofs(),
	}

	blockTime := 100 * time.Millisecond
	ens := kit.NewEnsemble(t, eOpts...).FullNode(&fullNode, fnOpts...).Miner(&miner, &fullNode, minerOpts...).Start()
	ens.BeginMining(blockTime)

	return &fullNode, &miner
}

func (f *TestFramework) Start() error {
	lapi.RunningNodeType = lapi.NodeMiner

	fullnodeApi := f.FullNode

	// Make sure that default wallet has been setup successfully
	defaultWallet, err := fullnodeApi.WalletDefaultAddress(f.ctx)
	if err != nil {
		return err
	}

	f.DefaultWallet = defaultWallet

	bal, err := fullnodeApi.WalletBalance(f.ctx, defaultWallet)
	if err != nil {
		return err
	}

	Log.Infof("default wallet %s has %d attoFIL", defaultWallet, bal)

	// Create a wallet for the client with some funds
	var wg sync.WaitGroup
	wg.Add(1)
	var clientAddr address.Address
	go func() {
		Log.Info("Creating client wallet")

		clientAddr, _ = fullnodeApi.WalletNew(f.ctx, chaintypes.KTBLS)

		amt := abi.NewTokenAmount(1e18)
		_ = sendFunds(f.ctx, fullnodeApi, clientAddr, amt)
		Log.Infof("Created client wallet %s with %d attoFil", clientAddr, amt)
		wg.Done()
	}()

	// Create wallets for publish storage deals and deal collateral with
	// some funds
	wg.Add(2)
	var psdWalletAddr address.Address
	var dealCollatAddr address.Address
	go func() {
		Log.Info("Creating publish storage deals wallet")
		psdWalletAddr, _ = fullnodeApi.WalletNew(f.ctx, chaintypes.KTBLS)

		amt := abi.NewTokenAmount(1e18)
		_ = sendFunds(f.ctx, fullnodeApi, psdWalletAddr, amt)
		Log.Infof("Created publish storage deals wallet %s with %d attoFil", psdWalletAddr, amt)
		wg.Done()
	}()
	go func() {
		Log.Info("Creating deal collateral wallet")
		dealCollatAddr, _ = fullnodeApi.WalletNew(f.ctx, chaintypes.KTBLS)

		amt := abi.NewTokenAmount(1e18)
		_ = sendFunds(f.ctx, fullnodeApi, dealCollatAddr, amt)
		Log.Infof("Created deal collateral wallet %s with %d attoFil", dealCollatAddr, amt)
		wg.Done()
	}()
	wg.Wait()

	f.ClientAddr = clientAddr

	f.Client, err = boostclient.NewStorageClient(f.ClientAddr, f.FullNode)
	if err != nil {
		return err
	}

	minerApi := f.LotusMiner

	minerAddr, err := minerApi.ActorAddress(f.ctx)
	if err != nil {
		return err
	}

	Log.Debugw("got miner actor addr", "addr", minerAddr)

	f.MinerAddr = minerAddr

	// Set the control address for the storage provider to be the publish
	// storage deals wallet
	_ = f.setControlAddress(psdWalletAddr)

	// Create an in-memory repo
	r := lotus_repo.NewMemory(nil)

	lr, err := r.Lock(repo.Boost)
	if err != nil {
		return err
	}

	// The in-memory repo implementation assumes that its being used to test
	// a miner, which has storage configuration.
	// Boost doesn't have storage configuration so clear the storage config.
	if err := lr.SetStorage(func(sc *storiface.StorageConfig) {
		sc.StoragePaths = nil
	}); err != nil {
		return fmt.Errorf("set storage config: %w", err)
	}

	// Set up the datastore
	ds, err := lr.Datastore(f.ctx, "/metadata")
	if err != nil {
		return err
	}

	// Set the miner address in the datastore
	err = ds.Put(f.ctx, datastore.NewKey("miner-address"), minerAddr.Bytes())
	if err != nil {
		return err
	}

	// Set some config values on the repo
	c, err := lr.Config()
	if err != nil {
		return err
	}

	token, err := f.LotusMiner.AuthNew(f.ctx, api.AllPermissions)
	if err != nil {
		return err
	}
	apiInfo := fmt.Sprintf("%s:%s", token, f.LotusMiner.ListenAddr)
	Log.Debugf("miner API info: %s", apiInfo)

	cfg, ok := c.(*config.Boost)
	if !ok {
		return fmt.Errorf("invalid config from repo, got: %T", c)
	}
	cfg.SectorIndexApiInfo = apiInfo
	cfg.SealerApiInfo = apiInfo
	cfg.Wallets.Miner = minerAddr.String()
	cfg.Wallets.PublishStorageDeals = psdWalletAddr.String()
	cfg.Wallets.DealCollateral = dealCollatAddr.String()
	cfg.LotusDealmaking.MaxDealsPerPublishMsg = 1
	cfg.LotusDealmaking.PublishMsgPeriod = lotus_config.Duration(0)
	val, err := ltypes.ParseFIL("0.1 FIL")
	if err != nil {
		return err
	}
	cfg.LotusFees.MaxPublishDealsFee = val
	cfg.Dealmaking.MaxStagingDealsBytes = 4000000 // 4 MB
	cfg.Dealmaking.RemoteCommp = true
	// No transfers will start until the first stall check period has elapsed
	cfg.Dealmaking.HttpTransferStallCheckPeriod = config.Duration(100 * time.Millisecond)
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
		node.BoostAPI(&f.Boost),
		node.Override(new(dtypes.ShutdownChan), shutdownChan),
		node.Base(),
		node.Repo(r),
		node.Override(new(v1api.FullNode), fullnodeApi),
		node.Override(new(*gateway.EthSubHandler), fullnodeApi.EthSubRouter),

		node.Override(new(*ctladdr.AddressSelector), modules.AddressSelector(&lotus_config.MinerAddressConfig{
			DealPublishControl: []string{
				psdWalletAddr.String(),
			},
			DisableOwnerFallback:  true,
			DisableWorkerFallback: true,
		})),

		// Reduce publish storage deals message confidence to 1 epoch so we
		// don't wait so long for publish confirmation
		node.Override(new(*storagemarket.ChainDealManager), func(a v1api.FullNode) *storagemarket.ChainDealManager {
			cdmCfg := storagemarket.ChainDealManagerCfg{PublishDealsConfidence: 1}
			return storagemarket.NewChainDealManager(a, cdmCfg)
		}),
	)
	if err != nil {
		return err
	}

	// Instantiate the boost service JSON RPC handler.
	handler, err := node.BoostHandler(f.Boost, true)
	if err != nil {
		return err
	}

	Log.Debug("getting API endpoint of boost node")

	endpoint, err := r.APIEndpoint()
	if err != nil {
		return err
	}

	Log.Debugw("json rpc server listening", "endpoint", endpoint)

	// Serve the RPC.
	rpcStopper, err := node.ServeRPC(handler, "boost", endpoint)
	if err != nil {
		return err
	}

	// Add boost libp2p address to boost client peer store so the client knows
	// how to connect to boost
	boostAddrs, err := f.Boost.NetAddrsListen(f.ctx)
	if err != nil {
		return err
	}
	f.Client.PeerStore.AddAddrs(boostAddrs.ID, boostAddrs.Addrs, time.Hour)

	// Connect full node to boost so that full node can make legacy deals
	// with boost
	err = f.FullNode.NetConnect(f.ctx, boostAddrs)
	if err != nil {
		return fmt.Errorf("unable to connect full node to boost: %w", err)
	}

	// Set boost libp2p address on chain
	Log.Debugw("setting peer id on chain", "peer id", boostAddrs.ID)
	params, err := actors.SerializeParams(&minertypes.ChangePeerIDParams{NewID: abi.PeerID(boostAddrs.ID)})
	if err != nil {
		return err
	}

	minerInfo, err := fullnodeApi.StateMinerInfo(f.ctx, minerAddr, ltypes.EmptyTSK)
	if err != nil {
		return err
	}

	msg := &ltypes.Message{
		To:     minerAddr,
		From:   minerInfo.Owner,
		Method: builtin.MethodsMiner.ChangePeerID,
		Params: params,
		Value:  ltypes.NewInt(0),
	}

	signed, err := fullnodeApi.MpoolPushMessage(f.ctx, msg, nil)
	if err != nil {
		return err
	}

	Log.Debugw("waiting for set peer id message to land on chain")
	mw, err := fullnodeApi.StateWaitMsg(f.ctx, signed.Cid(), 1, api.LookbackNoLimit, true)
	if err != nil {
		return err
	}
	if exitcode.Ok != mw.Receipt.ExitCode {
		return errors.New("expected mw.Receipt.ExitCode to be OK")
	}

	Log.Debugw("test framework setup complete")

	// Monitor for shutdown.
	finishCh := node.MonitorShutdown(shutdownChan,
		node.ShutdownHandler{Component: "rpc server", StopFunc: rpcStopper},
		node.ShutdownHandler{Component: "boost", StopFunc: stop},
	)

	f.Stop = func() {
		shutdownCtx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
		defer cancel()
		shutdownChan <- struct{}{}
		_ = stop(shutdownCtx)
		<-finishCh
	}

	return nil
}

// Add funds escrow in StorageMarketActor for both client and provider
func (f *TestFramework) AddClientProviderBalance(bal abi.TokenAmount) error {
	var errgp errgroup.Group
	errgp.Go(func() error {
		Log.Infof("adding client balance %d to Storage Market Actor", bal)
		mcid, err := f.FullNode.MarketAddBalance(f.ctx, f.ClientAddr, f.ClientAddr, bal)
		if err != nil {
			return fmt.Errorf("adding client balance to Storage Market Actor: %w", err)
		}
		return f.WaitMsg(mcid)
	})
	errgp.Go(func() error {
		mi, err := f.FullNode.StateMinerInfo(f.ctx, f.MinerAddr, chaintypes.EmptyTSK)
		if err != nil {
			return err
		}

		Log.Infof("adding provider balance %d to Storage Market Actor", bal)
		mcid, err := f.FullNode.MarketAddBalance(f.ctx, mi.Owner, f.MinerAddr, bal)
		if err != nil {
			return fmt.Errorf("adding provider balance to Storage Market Actor: %w", err)
		}
		return f.WaitMsg(mcid)
	})
	err := errgp.Wait()
	if err != nil {
		return err
	}

	Log.Info("done adding balance requirements")
	return nil
}

func (f *TestFramework) WaitForDealAddedToSector(dealUuid uuid.UUID) error {
	publishCtx, cancel := context.WithTimeout(f.ctx, 300*time.Second)
	defer cancel()
	peerID, err := f.Boost.ID(f.ctx)
	if err != nil {
		return err
	}

	for {
		resp, err := f.Client.DealStatus(f.ctx, peerID, dealUuid)
		if err != nil && !errors.Is(err, storagemarket.ErrDealNotFound) {
			return fmt.Errorf("error getting status: %s", err.Error())
		}

		if err == nil && resp.Error == "" {
			Log.Infof("deal state: %s", resp.DealStatus.Status)
			switch {
			case resp.DealStatus.Status == dealcheckpoints.Complete.String():
				if resp.DealStatus.Error != "" {
					return fmt.Errorf("Deal Error: %s", resp.DealStatus.Error)
				}
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

type DealResult struct {
	DealParams types.DealParams
	Result     *api.ProviderDealRejectionInfo
}

func (f *TestFramework) MakeDummyDeal(dealUuid uuid.UUID, carFilepath string, rootCid cid.Cid, url string, isOffline bool) (*DealResult, error) {
	cidAndSize, err := storagemarket.GenerateCommP(carFilepath)
	if err != nil {
		return nil, err
	}

	head, err := f.FullNode.ChainHead(f.ctx)
	if err != nil {
		return nil, fmt.Errorf("getting chain head: %w", err)
	}
	startEpoch := head.Height() + abi.ChainEpoch(2000)
	l, err := market.NewLabelFromString(rootCid.String())
	if err != nil {
		return nil, err
	}
	proposal := market.DealProposal{
		PieceCID:             cidAndSize.PieceCID,
		PieceSize:            cidAndSize.Size,
		VerifiedDeal:         false,
		Client:               f.ClientAddr,
		Provider:             f.MinerAddr,
		Label:                l,
		StartEpoch:           startEpoch,
		EndEpoch:             startEpoch + market.DealMinDuration,
		StoragePricePerEpoch: abi.NewTokenAmount(2000000),
		ProviderCollateral:   abi.NewTokenAmount(0),
		ClientCollateral:     abi.NewTokenAmount(0),
	}

	signedProposal, err := f.signProposal(f.ClientAddr, &proposal)
	if err != nil {
		return nil, err
	}

	Log.Debugf("Client balance requirement for deal: %d attoFil", proposal.ClientBalanceRequirement())
	Log.Debugf("Provider balance requirement for deal: %d attoFil", proposal.ProviderBalanceRequirement())

	// Save the path to the CAR file as a transfer parameter
	transferParams := &types2.HttpRequest{URL: url}
	transferParamsJSON, err := json.Marshal(transferParams)
	if err != nil {
		return nil, err
	}

	peerID, err := f.Boost.ID(f.ctx)
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
		RemoveUnsealedCopy: false,
		SkipIPNIAnnounce:   false,
	}

	res, err := f.Client.StorageDeal(f.ctx, dealParams, peerID)
	return &DealResult{
		DealParams: dealParams,
		Result:     res,
	}, err
}

func (f *TestFramework) signProposal(addr address.Address, proposal *market.DealProposal) (*market.ClientDealProposal, error) {
	buf, err := cborutil.Dump(proposal)
	if err != nil {
		return nil, err
	}

	sig, err := f.FullNode.WalletSign(f.ctx, addr, buf)
	if err != nil {
		return nil, err
	}

	return &market.ClientDealProposal{
		Proposal:        *proposal,
		ClientSignature: *sig,
	}, nil
}

func (f *TestFramework) DefaultMarketsV1DealParams() lapi.StartDealParams {
	return lapi.StartDealParams{
		Data:              &lotus_gfm_storagemarket.DataRef{TransferType: gfm_storagemarket.TTGraphsync},
		EpochPrice:        ltypes.NewInt(62500000), // minimum asking price
		MinBlocksDuration: uint64(lbuild.MinDealDuration),
		Miner:             f.MinerAddr,
		Wallet:            f.DefaultWallet,
		DealStartEpoch:    35000,
		FastRetrieval:     true,
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

func (f *TestFramework) setControlAddress(psdAddr address.Address) error {
	mi, err := f.FullNode.StateMinerInfo(f.ctx, f.MinerAddr, chaintypes.EmptyTSK)
	if err != nil {
		return err
	}

	cwp := &minertypes.ChangeWorkerAddressParams{
		NewWorker:       mi.Worker,
		NewControlAddrs: []address.Address{psdAddr},
	}
	sp, err := actors.SerializeParams(cwp)
	if err != nil {
		return err
	}

	smsg, err := f.FullNode.MpoolPushMessage(f.ctx, &chaintypes.Message{
		From:   mi.Owner,
		To:     f.MinerAddr,
		Method: builtin.MethodsMiner.ChangeWorkerAddress,

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

func (f *TestFramework) WaitMsg(mcid cid.Cid) error {
	_, err := f.FullNode.StateWaitMsg(f.ctx, mcid, 1, 1e10, true)
	return err
}

func (f *TestFramework) WaitDealSealed(ctx context.Context, deal *cid.Cid) error {
	for {
		di, err := f.FullNode.ClientGetDealInfo(ctx, *deal)
		if err != nil {
			return err
		}

		switch di.State {
		case gfm_storagemarket.StorageDealAwaitingPreCommit, gfm_storagemarket.StorageDealSealing:
		case gfm_storagemarket.StorageDealProposalRejected:
			return errors.New("deal rejected")
		case gfm_storagemarket.StorageDealFailing:
			return errors.New("deal failed")
		case gfm_storagemarket.StorageDealError:
			return fmt.Errorf("deal errored: %s", di.Message)
		case gfm_storagemarket.StorageDealActive:
			return nil
		}

		time.Sleep(2 * time.Second)
	}
}

func (f *TestFramework) Retrieve(ctx context.Context, t *testing.T, deal *cid.Cid, root cid.Cid, carExport bool) string {
	// perform retrieval.
	info, err := f.FullNode.ClientGetDealInfo(ctx, *deal)
	require.NoError(t, err)

	offers, err := f.FullNode.ClientFindData(ctx, root, &info.PieceCID)
	require.NoError(t, err)
	require.NotEmpty(t, offers, "no offers")

	return f.retrieve(ctx, t, offers[0], carExport)
}

func (f *TestFramework) ExtractFileFromCAR(ctx context.Context, t *testing.T, file *os.File) string {
	bserv := dstest.Bserv()
	ch, err := car.LoadCar(ctx, bserv.Blockstore(), file)
	require.NoError(t, err)

	var b blocks.Block
	if ch.Roots[0].Prefix().MhType == multihash.IDENTITY {
		mh, err := multihash.Decode(ch.Roots[0].Hash())
		require.NoError(t, err)
		b, err = blocks.NewBlockWithCid(mh.Digest, ch.Roots[0])
		require.NoError(t, err)
	} else {
		b, err = bserv.GetBlock(ctx, ch.Roots[0])
		require.NoError(t, err)
	}

	reg := ipld.Registry{}
	reg.Register(cid.DagProtobuf, dag.DecodeProtobufBlock)
	reg.Register(cid.DagCBOR, ipldcbor.DecodeBlock)
	reg.Register(cid.Raw, dag.DecodeRawBlock)

	nd, err := reg.Decode(b)
	require.NoError(t, err)

	dserv := dag.NewDAGService(bserv)
	fil, err := unixfile.NewUnixfsFile(ctx, dserv, nd)
	require.NoError(t, err)

	tmpFile := path.Join(t.TempDir(), fmt.Sprintf("file-in-car-%d", rand.Uint32()))
	err = files.WriteTo(fil, tmpFile)
	require.NoError(t, err)

	return tmpFile
}

func (f *TestFramework) RetrieveDirect(ctx context.Context, t *testing.T, root cid.Cid, pieceCid *cid.Cid, carExport bool) string {
	offer, err := f.FullNode.ClientMinerQueryOffer(ctx, f.MinerAddr, root, pieceCid)
	require.NoError(t, err)

	return f.retrieve(ctx, t, offer, carExport)
}

func (f *TestFramework) retrieve(ctx context.Context, t *testing.T, offer lapi.QueryOffer, carExport bool) string {
	p := path.Join(t.TempDir(), "ret-car-"+t.Name())
	carFile, err := os.Create(p)
	require.NoError(t, err)

	defer carFile.Close() //nolint:errcheck

	caddr, err := f.FullNode.WalletDefaultAddress(ctx)
	require.NoError(t, err)

	updatesCtx, cancel := context.WithCancel(ctx)
	updates, err := f.FullNode.ClientGetRetrievalUpdates(updatesCtx)
	require.NoError(t, err)

	retrievalRes, err := f.FullNode.ClientRetrieve(ctx, offer.Order(caddr))
	require.NoError(t, err)
consumeEvents:
	for {
		var evt lapi.RetrievalInfo
		select {
		case <-updatesCtx.Done():
			t.Fatal("Retrieval Timed Out")
		case evt = <-updates:
			if evt.ID != retrievalRes.DealID {
				continue
			}
		}
		switch evt.Status {
		case lotus_gfm_retrievalmarket.DealStatusCompleted:
			break consumeEvents
		case lotus_gfm_retrievalmarket.DealStatusRejected:
			t.Fatalf("Retrieval Proposal Rejected: %s", evt.Message)
		case
			lotus_gfm_retrievalmarket.DealStatusDealNotFound,
			lotus_gfm_retrievalmarket.DealStatusErrored:
			t.Fatalf("Retrieval Error: %s", evt.Message)
		}
	}
	cancel()

	require.NoError(t, f.FullNode.ClientExport(ctx,
		lapi.ExportRef{
			Root:   offer.Root,
			DealID: retrievalRes.DealID,
		},
		lapi.FileRef{
			Path:  carFile.Name(),
			IsCAR: carExport,
		}))

	ret := carFile.Name()
	if carExport {
		ret = f.ExtractFileFromCAR(ctx, t, carFile)
	}

	return ret
}
