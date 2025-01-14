package framework

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/filecoin-project/boost/api"
	clinode "github.com/filecoin-project/boost/cli/node"
	boostclient "github.com/filecoin-project/boost/client"
	"github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/markets/utils"
	"github.com/filecoin-project/boost/node"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/node/repo"
	rc "github.com/filecoin-project/boost/retrievalmarket/client"
	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/storedask"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	transporttypes "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin"
	verifregtypes13 "github.com/filecoin-project/go-state-types/builtin/v13/verifreg"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	miner9types "github.com/filecoin-project/go-state-types/builtin/v9/miner"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/go-state-types/exitcode"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	chaintypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet/key"
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
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/files"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	dstest "github.com/ipfs/boxo/ipld/merkledag/test"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	levelds "github.com/ipfs/go-ds-leveldb"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	ipldformat "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car"
	carv2 "github.com/ipld/go-car/v2"
	storagecar "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	trustless "github.com/ipld/go-trustless-utils"
	"github.com/ipld/go-trustless-utils/traversal"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/term"
)

var Log = logging.Logger("boosttest")

type TestFrameworkConfig struct {
	Ensemble                  *kit.Ensemble
	MaxStagingBytes           int64
	ProvisionalWalletBalances int64
	StartEpochSealingBuffer   uint64
}

type TestFramework struct {
	ctx    context.Context
	Stop   func()
	config *TestFrameworkConfig

	HomeDir       string
	Client        *boostclient.StorageClient
	Boost         api.Boost
	FullNode      *kit.TestFullNode
	LotusMiner    *kit.TestMiner
	ClientAddr    address.Address
	MinerAddr     address.Address
	DefaultWallet address.Address
}

type FrameworkOpts func(pc *TestFrameworkConfig)

func SetMaxStagingBytes(max int64) FrameworkOpts {
	return func(tmc *TestFrameworkConfig) {
		tmc.MaxStagingBytes = max
	}
}

func WithEnsemble(e *kit.Ensemble) FrameworkOpts {
	return func(tmc *TestFrameworkConfig) {
		tmc.Ensemble = e
	}
}

func WithMaxStagingDealsBytes(e int64) FrameworkOpts {
	return func(tmc *TestFrameworkConfig) {
		tmc.MaxStagingBytes = e
	}
}

func SetProvisionalWalletBalances(balance int64) FrameworkOpts {
	return func(tmc *TestFrameworkConfig) {
		tmc.ProvisionalWalletBalances = balance
	}
}

func WithStartEpochSealingBuffer(e uint64) FrameworkOpts {
	return func(tmc *TestFrameworkConfig) {
		tmc.StartEpochSealingBuffer = e
	}
}

func NewTestFramework(ctx context.Context, t *testing.T, opts ...FrameworkOpts) *TestFramework {
	//bal, err := chaintypes.ParseFIL("1000fil")
	//require.NoError(t, err)
	fmc := &TestFrameworkConfig{
		// default provisional balance
		ProvisionalWalletBalances: 9e18,
	}
	for _, opt := range opts {
		opt(fmc)
	}

	fullNode, miner := FullNodeAndMiner(t, fmc.Ensemble)
	return &TestFramework{
		ctx:        ctx,
		config:     fmc,
		HomeDir:    t.TempDir(),
		FullNode:   fullNode,
		LotusMiner: miner,
	}
}

func FullNodeAndMiner(t *testing.T, ensemble *kit.Ensemble) (*kit.TestFullNode, *kit.TestMiner) {
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
					sc.WaitDealsDelay = time.Second
					//sc.AggregateCommits = false
					sc.CommitBatchWait = 2 * time.Second
					sc.StartEpochSealingBuffer = 30

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

	defaultEnsemble := ensemble == nil
	if defaultEnsemble {
		eOpts := []kit.EnsembleOpt{
			//TODO: at the moment we are not mocking proofs
			//maybe enable this in the future to speed up tests further

			//kit.MockProofs(),
		}
		ensemble = kit.NewEnsemble(t, eOpts...)
	}

	ensemble.FullNode(&fullNode, fnOpts...).Miner(&miner, &fullNode, minerOpts...)
	if defaultEnsemble {
		ensemble.Start()
		blockTime := 100 * time.Millisecond
		ensemble.BeginMining(blockTime)
	}

	return &fullNode, &miner
}

type ConfigOpt func(cfg *config.Boost)

func (f *TestFramework) Start(opts ...ConfigOpt) error {
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

		amt := abi.NewTokenAmount(f.config.ProvisionalWalletBalances)
		_ = SendFunds(f.ctx, fullnodeApi, clientAddr, amt)
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

		amt := abi.NewTokenAmount(f.config.ProvisionalWalletBalances)
		_ = SendFunds(f.ctx, fullnodeApi, psdWalletAddr, amt)
		Log.Infof("Created publish storage deals wallet %s with %d attoFil", psdWalletAddr, amt)
		wg.Done()
	}()
	go func() {
		Log.Info("Creating deal collateral wallet")
		dealCollatAddr, _ = fullnodeApi.WalletNew(f.ctx, chaintypes.KTBLS)

		amt := abi.NewTokenAmount(f.config.ProvisionalWalletBalances)
		_ = SendFunds(f.ctx, fullnodeApi, dealCollatAddr, amt)
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

	apiInfo, err := f.LotusMinerApiInfo()
	if err != nil {
		return err
	}
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
	cfg.Dealpublish.MaxDealsPerPublishMsg = 1
	cfg.Dealpublish.PublishMsgPeriod = config.Duration(0)
	val, err := chaintypes.ParseFIL("0.1 FIL")
	if err != nil {
		return err
	}
	cfg.Dealpublish.MaxPublishDealsFee = val

	cfg.Dealmaking.RemoteCommp = true
	// No transfers will start until the first stall check period has elapsed
	cfg.HttpDownload.HttpTransferStallCheckPeriod = config.Duration(100 * time.Millisecond)
	cfg.Storage.ParallelFetchLimit = 10

	for _, o := range opts {
		o(cfg)
	}

	if f.config.MaxStagingBytes > 0 {
		cfg.Dealmaking.MaxStagingDealsBytes = f.config.MaxStagingBytes
	} else {
		cfg.Dealmaking.MaxStagingDealsBytes = 4000000 // 4 MB
	}

	cfg.Dealmaking.StartEpochSealingBuffer = 50

	if f.config.StartEpochSealingBuffer > 0 {
		cfg.Dealmaking.StartEpochSealingBuffer = f.config.StartEpochSealingBuffer
	}

	cfg.Dealmaking.ExpectedSealDuration = 10

	// Enable LID with leveldb
	cfg.LocalIndexDirectory.Leveldb.Enabled = true
	cfg.IndexProvider.Announce.AnnounceOverHttp = true
	//cfg.IndexProvider.DataTransferPublisher = false
	cfg.IndexProvider.HttpPublisher.Enabled = true
	//cfg.IndexProvider.HttpPublisher.Port = 3333
	cfg.IndexProvider.HttpPublisher.PublicHostname = "127.0.0.1"
	cfg.IndexProvider.HttpPublisher.WithLibp2p = true

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
	params, err := actors.SerializeParams(&miner9types.ChangePeerIDParams{NewID: abi.PeerID(boostAddrs.ID)})
	if err != nil {
		return err
	}

	minerInfo, err := fullnodeApi.StateMinerInfo(f.ctx, minerAddr, chaintypes.EmptyTSK)
	if err != nil {
		return err
	}

	msg := &chaintypes.Message{
		To:     minerAddr,
		From:   minerInfo.Owner,
		Method: builtin.MethodsMiner.ChangePeerID,
		Params: params,
		Value:  chaintypes.NewInt(0),
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

func (f *TestFramework) LotusMinerApiInfo() (string, error) {
	token, err := f.LotusMiner.AuthNew(f.ctx, api.AllPermissions)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%s", token, f.LotusMiner.ListenAddr), nil
}

func (f *TestFramework) LotusFullNodeApiInfo() (string, error) {
	token, err := f.FullNode.AuthNew(f.ctx, api.AllPermissions)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", token, f.FullNode.ListenAddr), nil
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
	cidAndSize, err := storagemarket.GenerateCommPLocally(carFilepath)
	if err != nil {
		return nil, err
	}

	head, err := f.FullNode.ChainHead(f.ctx)
	if err != nil {
		return nil, fmt.Errorf("getting chain head: %w", err)
	}
	startEpoch := head.Height() + abi.ChainEpoch(1000)
	l, err := market.NewLabelFromString(rootCid.String())
	if err != nil {
		return nil, err
	}
	price := big.Div(big.Mul(storedask.DefaultPrice, abi.NewTokenAmount(int64(cidAndSize.Size))), abi.NewTokenAmount(1<<30))
	proposal := market.DealProposal{
		PieceCID:             cidAndSize.PieceCID,
		PieceSize:            cidAndSize.Size,
		VerifiedDeal:         false,
		Client:               f.ClientAddr,
		Provider:             f.MinerAddr,
		Label:                l,
		StartEpoch:           startEpoch,
		EndEpoch:             startEpoch + market.DealMinDuration,
		StoragePricePerEpoch: price,
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
	transferParams := &transporttypes.HttpRequest{URL: url}
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

func SendFunds(ctx context.Context, sender lapi.FullNode, recipient address.Address, amount abi.TokenAmount) error {
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

	cwp := &miner9types.ChangeWorkerAddressParams{
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

	reg := ipldformat.Registry{}
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

func (f *TestFramework) Retrieve(ctx context.Context, t *testing.T, request trustless.Request, extractCar bool) string {
	tempdir := t.TempDir()

	var out string
	retPath := path.Join(tempdir, "retrievals")
	require.NoError(t, os.Mkdir(retPath, 0755))

	clientPath := path.Join(tempdir, "client")
	require.NoError(t, os.Mkdir(clientPath, 0755))

	clientNode, err := clinode.Setup(clientPath)
	require.NoError(t, err)

	addr, err := clientNode.Wallet.GetDefault()
	require.NoError(t, err)

	bstoreDatastore, err := flatfs.CreateOrOpen(path.Join(tempdir, "blockstore"), flatfs.NextToLast(3), false)
	bstore := blockstore.NewBlockstore(bstoreDatastore, blockstore.NoPrefix())
	require.NoError(t, err)

	ds, err := levelds.NewDatastore(path.Join(clientPath, "dstore"), nil)
	require.NoError(t, err)

	// Create the retrieval client
	fc, err := rc.NewClient(clientNode.Host, f.FullNode, clientNode.Wallet, addr, bstore, ds, clientPath)
	require.NoError(t, err)

	baddrs, err := f.Boost.NetAddrsListen(ctx)
	require.NoError(t, err)

	// Query the remote to find out the retrieval parameters
	query, err := RetrievalQuery(ctx, t, clientNode, &baddrs, request.Root)
	require.NoError(t, err)

	// Create a matching proposal for the query
	proposal, err := rc.RetrievalProposalForAsk(query, request.Root, request.Selector())
	require.NoError(t, err)

	// Let's see the selector we're working with
	encoded, err := ipld.Encode(request.Selector(), dagjson.Encode)
	require.NoError(t, err)
	t.Logf("Retrieving with selector: %s", string(encoded))

	// Retrieve the data
	_, err = fc.RetrieveContentWithProgressCallback(
		ctx,
		f.MinerAddr,
		proposal,
		func(bytesReceived_ uint64) {
			printProgress(bytesReceived_, t)
		},
	)
	require.NoError(t, err)

	// Validate the data

	dservOffline := dag.NewDAGService(blockservice.New(bstore, offline.Exchange(bstore)))
	lsys := utils.CreateLinkSystem(dservOffline)

	if !extractCar {
		// If the caller wants a CAR, we create it and then when we run our check traversal over the DAG
		// each load will trigger a write to the CAR
		file, err := os.CreateTemp(retPath, "*"+request.Root.String()+".car")
		require.NoError(t, err)
		out = file.Name()
		storage, err := storagecar.NewWritable(file, []cid.Cid{request.Root}, carv2.WriteAsCarV1(true))
		require.NoError(t, err)
		sro := lsys.StorageReadOpener
		lsys.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
			r, err := sro(lc, l)
			if err != nil {
				return nil, err
			}
			buf, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}
			if err := storage.Put(lc.Ctx, l.(cidlink.Link).Cid.KeyString(), buf); err != nil {
				return nil, err
			}
			return bytes.NewReader(buf), nil
		}
	}

	// Check that we got what we expected by executing the same selector over our
	// retrieved DAG
	_, err = traversal.Config{
		Root:     request.Root,
		Selector: request.Selector(),
	}.Traverse(ctx, lsys, nil)
	require.NoError(t, err)

	if extractCar {
		// Caller doesn't want the raw blocks, so extract the file as UnixFS and
		// assume that we've fetched the right blocks to be able to do this.
		dnode, err := dservOffline.Get(ctx, request.Root)
		require.NoError(t, err)
		ufsFile, err := unixfile.NewUnixfsFile(ctx, dservOffline, dnode)
		require.NoError(t, err)
		file, err := os.CreateTemp(retPath, "*"+request.Root.String())
		require.NoError(t, err)
		err = file.Close()
		require.NoError(t, err)
		err = os.Remove(file.Name())
		require.NoError(t, err)
		err = files.WriteTo(ufsFile, file.Name())
		require.NoError(t, err)
		out = file.Name()
	}

	return out
}

type RetrievalInfo struct {
	PayloadCID   cid.Cid
	ID           legacyretrievaltypes.DealID
	PieceCID     *cid.Cid
	PricePerByte abi.TokenAmount
	UnsealPrice  abi.TokenAmount

	Status        legacyretrievaltypes.DealStatus
	Message       string // more information about deal state, particularly errors
	Provider      peer.ID
	BytesReceived uint64
	BytesPaidFor  uint64
	TotalPaid     abi.TokenAmount

	TransferChannelID *datatransfer.ChannelID
	DataTransfer      *DataTransferChannel

	// optional event if part of ClientGetRetrievalUpdates
	Event *legacyretrievaltypes.ClientEvent
}

type RestrievalRes struct {
	DealID legacyretrievaltypes.DealID
}

type DataTransferChannel struct {
	TransferID  datatransfer.TransferID
	Status      datatransfer.Status
	BaseCID     cid.Cid
	IsInitiator bool
	IsSender    bool
	Voucher     string
	Message     string
	OtherPeer   peer.ID
	Transferred uint64
	Stages      *datatransfer.ChannelStages
}

func printProgress(bytesReceived uint64, t *testing.T) {
	str := fmt.Sprintf("%v (%v)", bytesReceived, humanize.IBytes(bytesReceived))

	termWidth, _, err := term.GetSize(int(os.Stdin.Fd()))
	strLen := len(str)
	if err == nil {

		if strLen < termWidth {
			// If the string is shorter than the terminal width, pad right side
			// with spaces to remove old text
			str = strings.Join([]string{str, strings.Repeat(" ", termWidth-strLen)}, "")
		} else if strLen > termWidth {
			// If the string doesn't fit in the terminal, cut it down to a size
			// that fits
			str = str[:termWidth]
		}
	}

	t.Logf("%s\r", str)
}

func RetrievalQuery(ctx context.Context, t *testing.T, client *clinode.Node, peerAddr *peer.AddrInfo, pcid cid.Cid) (*legacyretrievaltypes.QueryResponse, error) {
	client.Host.Peerstore().AddAddrs(peerAddr.ID, peerAddr.Addrs, peerstore.TempAddrTTL)
	s, err := client.Host.NewStream(ctx, peerAddr.ID, rc.RetrievalQueryProtocol)
	require.NoError(t, err)

	client.Host.ConnManager().Protect(s.Conn().RemotePeer(), "RetrievalQuery")
	defer func() {
		client.Host.ConnManager().Unprotect(s.Conn().RemotePeer(), "RetrievalQuery")
		s.Close()
	}()

	// We have connected

	q := &legacyretrievaltypes.Query{
		PayloadCID: pcid,
	}

	var resp legacyretrievaltypes.QueryResponse
	dline, ok := ctx.Deadline()
	if ok {
		_ = s.SetDeadline(dline)
		defer func() { _ = s.SetDeadline(time.Time{}) }()
	}

	err = cborutil.WriteCborRPC(s, q)
	require.NoError(t, err)

	err = cborutil.ReadCborRPC(s, &resp)
	require.NoError(t, err)

	return &resp, nil
}

type DatacapParams struct {
	RootKey     *key.Key
	VerifierKey *key.Key
	Opts        []kit.EnsembleOpt
}

func BuildDatacapParams() (*DatacapParams, error) {
	rootKey, err := key.GenerateKey(chaintypes.KTSecp256k1)
	if err != nil {
		return nil, err
	}

	verifierKey, err := key.GenerateKey(chaintypes.KTSecp256k1)
	if err != nil {
		return nil, err
	}

	bal, err := chaintypes.ParseFIL("100fil")
	if err != nil {
		return nil, err
	}

	eOpts := []kit.EnsembleOpt{
		kit.RootVerifier(rootKey, abi.NewTokenAmount(bal.Int64())),
		// assign some balance to the verifier so they can send an AddClient message.
		kit.Account(verifierKey, abi.NewTokenAmount(bal.Int64())),
	}

	return &DatacapParams{
		RootKey:     rootKey,
		VerifierKey: verifierKey,
		Opts:        eOpts,
	}, nil
}

func (f *TestFramework) AddClientDataCap(t *testing.T, ctx context.Context, rootKey *key.Key, verifierKey *key.Key) error {
	// get VRH
	vrh, err := f.FullNode.StateVerifiedRegistryRootKey(ctx, chaintypes.TipSetKey{})
	fmt.Println(vrh.String())
	require.NoError(t, err)

	// import the root key to Lotus to send message
	rootAddr, err := f.FullNode.WalletImport(ctx, &rootKey.KeyInfo)
	require.NoError(t, err)

	// import the verifiers' keys to Lotus to send message
	verifier1Addr, err := f.FullNode.WalletImport(ctx, &verifierKey.KeyInfo)
	require.NoError(t, err)

	// make the verifier1Addr a datacap verifiers

	mkVerifier(ctx, t, f.FullNode.FullNode.(*lapi.FullNodeStruct), rootAddr, verifier1Addr)

	// assign datacap to a client
	initialDatacap := big.NewInt(10000000)

	params, err := actors.SerializeParams(&verifregtypes13.AddVerifiedClientParams{Address: f.ClientAddr, Allowance: initialDatacap})
	require.NoError(t, err)

	msg := &chaintypes.Message{
		From:   verifier1Addr,
		To:     verifreg.Address,
		Method: verifreg.Methods.AddVerifiedClient,
		Params: params,
		Value:  big.Zero(),
	}

	sm, err := f.FullNode.MpoolPushMessage(ctx, msg, nil)
	require.NoError(t, err)

	res, err := f.FullNode.StateWaitMsg(ctx, sm.Cid(), 1, lapi.LookbackNoLimit, true)
	require.NoError(t, err)
	require.EqualValues(t, 0, res.Receipt.ExitCode)

	// check datacap balance
	dcap, err := f.FullNode.StateVerifiedClientStatus(ctx, f.ClientAddr, chaintypes.EmptyTSK)
	if err != nil {
		return err
	}
	if !dcap.Equals(initialDatacap) {
		return fmt.Errorf("verified client datacap is %s but expected %d", dcap, initialDatacap)
	}

	return nil
}

func mkVerifier(ctx context.Context, t *testing.T, api *lapi.FullNodeStruct, rootAddr address.Address, addr address.Address) {
	allowance := big.NewInt(100000000000)
	params, aerr := actors.SerializeParams(&verifreg9types.AddVerifierParams{Address: addr, Allowance: allowance})
	require.NoError(t, aerr)

	msg := &chaintypes.Message{
		From:   rootAddr,
		To:     verifreg.Address,
		Method: verifreg.Methods.AddVerifier,
		Params: params,
		Value:  big.Zero(),
	}

	sm, err := api.MpoolPushMessage(ctx, msg, nil)
	require.NoError(t, err, "AddVerifier failed")

	res, err := api.StateWaitMsg(ctx, sm.Cid(), 1, lapi.LookbackNoLimit, true)
	require.NoError(t, err)
	require.EqualValues(t, 0, res.Receipt.ExitCode)

	verifierAllowance, err := api.StateVerifierStatus(ctx, addr, chaintypes.EmptyTSK)
	require.NoError(t, err)
	require.Equal(t, allowance, *verifierAllowance)
}
