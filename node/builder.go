package node

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	rmnet "github.com/filecoin-project/boost-gfm/retrievalmarket/network"
	gfm_storagemarket "github.com/filecoin-project/boost-gfm/storagemarket"
	storageimpl "github.com/filecoin-project/boost-gfm/storagemarket/impl"
	"github.com/filecoin-project/boost-gfm/storagemarket/impl/storedask"
	"github.com/filecoin-project/boost-gfm/stores"
	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/boost/markets/idxprov"
	"github.com/filecoin-project/boost/markets/retrievaladapter"
	storageadapter "github.com/filecoin-project/boost/markets/storageadapter"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/impl"
	"github.com/filecoin-project/boost/node/impl/backupmgr"
	"github.com/filecoin-project/boost/node/impl/common"
	"github.com/filecoin-project/boost/node/modules"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/boost/protocolproxy"
	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	"github.com/filecoin-project/boost/retrievalmarket/rtvllog"
	"github.com/filecoin-project/boost/retrievalmarket/server"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/dealfilter"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/go-address"
	lotus_gfm_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	lotus_api "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	lotus_journal "github.com/filecoin-project/lotus/journal"
	"github.com/filecoin-project/lotus/journal/alerting"
	_ "github.com/filecoin-project/lotus/lib/sigs/bls"
	_ "github.com/filecoin-project/lotus/lib/sigs/secp"
	mdagstore "github.com/filecoin-project/lotus/markets/dagstore"
	lotus_dealfilter "github.com/filecoin-project/lotus/markets/dealfilter"
	lotus_config "github.com/filecoin-project/lotus/node/config"
	lotus_common "github.com/filecoin-project/lotus/node/impl/common"
	lotus_net "github.com/filecoin-project/lotus/node/impl/net"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	lotus_helpers "github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/filecoin-project/lotus/node/modules/lp2p"
	lotus_lp2p "github.com/filecoin-project/lotus/node/modules/lp2p"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/ctladdr"
	"github.com/filecoin-project/lotus/storage/paths"
	"github.com/filecoin-project/lotus/storage/sealer"
	"github.com/filecoin-project/lotus/storage/sectorblocks"
	"github.com/filecoin-project/lotus/system"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-metrics-interface"
	provider "github.com/ipni/index-provider"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	record "github.com/libp2p/go-libp2p-record"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	"github.com/libp2p/go-libp2p/p2p/net/conngater"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/fx"
)

//nolint:deadcode,varcheck
var log = logging.Logger("builder")

// special is a type used to give keys to modules which
//
//	can't really be identified by the returned type
type special struct{ id int }

//nolint:golint
var (
	DefaultTransportsKey = special{0}  // Libp2p option
	DiscoveryHandlerKey  = special{2}  // Private type
	AddrsFactoryKey      = special{3}  // Libp2p option
	SmuxTransportKey     = special{4}  // Libp2p option
	RelayKey             = special{5}  // Libp2p option
	SecurityKey          = special{6}  // Libp2p option
	BaseRoutingKey       = special{7}  // fx groups + multiret
	NatPortMapKey        = special{8}  // Libp2p option
	ConnectionManagerKey = special{9}  // Libp2p option
	AutoNATSvcKey        = special{10} // Libp2p option
	BandwidthReporterKey = special{11} // Libp2p option
	ConnGaterKey         = special{12} // libp2p option
	DAGStoreKey          = special{13} // constructor returns multiple values
	ResourceManagerKey   = special{14} // Libp2p option
	UserAgentKey         = special{15} // Libp2p option
)

type invoke int

// Invokes are called in the order they are defined.
//
//nolint:golint
const (
	// InitJournal at position 0 initializes the journal global var as soon as
	// the system starts, so that it's available for all other components.
	InitJournalKey = invoke(iota)

	// System processes.
	InitMemoryWatchdog

	// health checks
	CheckFDLimit

	// libp2p
	PstoreAddSelfKeysKey
	StartListeningKey
	BootstrapKey

	// filecoin
	SetGenesisKey

	RunHelloKey
	RunChainExchangeKey
	RunChainGraphsync
	RunPeerMgrKey

	HandleIncomingBlocksKey
	HandleIncomingMessagesKey
	HandleMigrateClientFundsKey
	HandlePaymentChannelManagerKey

	// miner
	GetParamsKey
	HandleMigrateProviderFundsKey
	HandleDealsKey
	HandleCreateRetrievalTablesKey
	HandleSetShardSelector
	HandleSetRetrievalAskGetter
	HandleRetrievalEventsKey
	HandleRetrievalKey
	HandleRetrievalTransportsKey
	HandleProtocolProxyKey
	RunSectorServiceKey

	// boost should be started after legacy markets (HandleDealsKey)
	HandleBoostDealsKey
	HandleContractDealsKey
	HandleProposalLogCleanerKey
	HandleOnlineBackupMgrKey

	// daemon
	ExtractApiKey
	HeadMetricsKey
	SettlePaymentChannelsKey
	RunPeerTaggerKey
	SetupFallbackBlockstoresKey
	HandleSetLinkSystem

	SetApiEndpointKey

	_nInvokes // keep this last
)

type Settings struct {
	// modules is a map of constructors for DI
	//
	// In most cases the index will be a reflect. Type of element returned by
	// the constructor, but for some 'constructors' it's hard to specify what's
	// the return type should be (or the constructor returns fx group)
	modules map[interface{}]fx.Option

	// invokes are separate from modules as they can't be referenced by return
	// type, and must be applied in correct order
	invokes []fx.Option

	nodeType lotus_repo.RepoType

	Base   bool // Base option applied
	Config bool // Config option applied
	Lite   bool // Start node in "lite" mode
}

// Basic lotus-app services
func defaults() []Option {
	return []Option{
		// global system journal
		Override(new(lotus_journal.DisabledEvents), lotus_journal.EnvDisabledEvents),
		Override(new(lotus_journal.Journal), lotus_modules.OpenFilesystemJournal),
		Override(new(*alerting.Alerting), alerting.NewAlertingSystem),
		Override(new(lotus_dtypes.NodeStartTime), FromVal(lotus_dtypes.NodeStartTime(time.Now()))),

		Override(CheckFDLimit, lotus_modules.CheckFdLimit(build.DefaultFDLimit)),

		Override(new(system.MemoryConstraints), modules.MemoryConstraints),

		Override(new(lotus_helpers.MetricsCtx), func() context.Context {
			return metrics.CtxScope(context.Background(), "boost")
		}),

		Override(new(lotus_dtypes.ShutdownChan), make(chan struct{})),
	}
}

var LibP2P = Options(
	// Host config
	Override(new(lotus_dtypes.Bootstrapper), lotus_dtypes.Bootstrapper(false)),

	// Host dependencies
	Override(new(peerstore.Peerstore), func() (peerstore.Peerstore, error) { return pstoremem.NewPeerstore() }),
	Override(PstoreAddSelfKeysKey, lotus_lp2p.PstoreAddSelfKeys),
	Override(StartListeningKey, lotus_lp2p.StartListening([]string{"/ip4/127.0.0.1/tcp/1899"})),

	// Host settings
	Override(DefaultTransportsKey, lotus_lp2p.DefaultTransports),
	Override(AddrsFactoryKey, lotus_lp2p.AddrsFactory(nil, nil)),
	Override(SmuxTransportKey, lotus_lp2p.SmuxTransport()),
	Override(RelayKey, lotus_lp2p.NoRelay()),
	Override(SecurityKey, lotus_lp2p.Security(true, false)),
	Override(DefaultTransportsKey, lp2p.DefaultTransports),
	Override(UserAgentKey, modules.UserAgent),

	// Host
	Override(new(lotus_lp2p.RawHost), lotus_lp2p.Host),
	Override(new(host.Host), lotus_lp2p.RoutedHost),
	Override(new(lotus_lp2p.BaseIpfsRouting), lotus_lp2p.DHTRouting(dht.ModeAuto)),

	Override(DiscoveryHandlerKey, lotus_lp2p.DiscoveryHandler),

	// Routing
	Override(new(record.Validator), modules.RecordValidator),
	Override(BaseRoutingKey, lotus_lp2p.BaseRouting),
	Override(new(routing.Routing), lotus_lp2p.Routing),

	// Services
	Override(BandwidthReporterKey, lotus_lp2p.BandwidthCounter),
	Override(AutoNATSvcKey, lotus_lp2p.AutoNATService),

	// Services (pubsub)
	Override(new(*lotus_dtypes.ScoreKeeper), lotus_lp2p.ScoreKeeper),
	Override(new(*pubsub.PubSub), lotus_lp2p.GossipSub),
	Override(new(*lotus_config.Pubsub), func(bs lotus_dtypes.Bootstrapper) *lotus_config.Pubsub {
		return &lotus_config.Pubsub{
			Bootstrapper: bool(bs),
		}
	}),

	// Services (connection management)
	Override(ConnectionManagerKey, lotus_lp2p.ConnectionManager(50, 200, 20*time.Second, nil)),
	Override(new(*conngater.BasicConnectionGater), lotus_lp2p.ConnGater),
	Override(ConnGaterKey, lotus_lp2p.ConnGaterOption),
)

func Base() Option {
	return Options(
		func(s *Settings) error { s.Base = true; return nil }, // mark Base as applied
		ApplyIf(func(s *Settings) bool { return s.Config },
			Error(errors.New("the Base() option must be set before Config option")),
		),
		LibP2P,
		BoostNode,
	)
}

// Config sets up constructors based on the provided Config
func ConfigCommon(cfg *config.Common) Option {
	return Options(
		func(s *Settings) error { s.Config = true; return nil },
		Override(new(dtypes.APIEndpoint), func() (dtypes.APIEndpoint, error) {
			return multiaddr.NewMultiaddr(cfg.API.ListenAddress)
		}),
		Override(SetApiEndpointKey, func(lr lotus_repo.LockedRepo, e dtypes.APIEndpoint) error {
			return lr.SetAPIEndpoint(e)
		}),
		Override(new(paths.URLs), func(e dtypes.APIEndpoint) (paths.URLs, error) {
			ip := cfg.API.RemoteListenAddress

			var urls paths.URLs
			urls = append(urls, "http://"+ip+"/remote") // TODO: This makes no assumptions, and probably could...
			return urls, nil
		}),
		ApplyIf(func(s *Settings) bool { return s.Base }), // apply only if Base has already been applied
		Override(new(api.Net), From(new(lotus_net.NetAPI))),
		Override(new(api.Common), From(new(common.CommonAPI))),

		Override(new(lotus_api.Net), From(new(lotus_net.NetAPI))),
		Override(new(lotus_api.Common), From(new(lotus_common.CommonAPI))),

		Override(new(lotus_dtypes.MetadataDS), lotus_modules.Datastore(cfg.Backup.DisableMetadataLog)),
		Override(StartListeningKey, lotus_lp2p.StartListening(cfg.Libp2p.ListenAddresses)),
		Override(ConnectionManagerKey, lotus_lp2p.ConnectionManager(
			cfg.Libp2p.ConnMgrLow,
			cfg.Libp2p.ConnMgrHigh,
			time.Duration(cfg.Libp2p.ConnMgrGrace),
			cfg.Libp2p.ProtectedPeers)),
		ApplyIf(func(s *Settings) bool { return len(cfg.Libp2p.BootstrapPeers) > 0 },
			Override(new(lotus_dtypes.BootstrapPeers), modules.ConfigBootstrap(cfg.Libp2p.BootstrapPeers)),
		),

		Override(new(network.ResourceManager), modules.ResourceManager(cfg.Libp2p.ConnMgrHigh)),
		Override(ResourceManagerKey, lp2p.ResourceManagerOption),
		Override(new(*pubsub.PubSub), lp2p.GossipSub),
		Override(new(*lotus_config.Pubsub), &cfg.Pubsub),

		ApplyIf(func(s *Settings) bool { return len(cfg.Libp2p.BootstrapPeers) > 0 },
			Override(new(lotus_dtypes.BootstrapPeers), modules.ConfigBootstrap(cfg.Libp2p.BootstrapPeers)),
		),

		Override(AddrsFactoryKey, lotus_lp2p.AddrsFactory(
			cfg.Libp2p.AnnounceAddresses,
			cfg.Libp2p.NoAnnounceAddresses)),
		If(!cfg.Libp2p.DisableNatPortMap, Override(NatPortMapKey, lotus_lp2p.NatPortMap)),
	)
}

func Repo(r lotus_repo.Repo) Option {
	return func(settings *Settings) error {
		lr, err := r.Lock(settings.nodeType)
		if err != nil {
			return err
		}
		// If it's not a mem-repo
		if _, ok := r.(*lotus_repo.MemRepo); !ok {
			// Migrate config file
			err = config.ConfigMigrate(lr.Path())
			if err != nil {
				return fmt.Errorf("migrating config: %w", err)
			}
		}
		c, err := lr.Config()
		if err != nil {
			return err
		}
		cfg, ok := c.(*config.Boost)
		if !ok {
			return fmt.Errorf("invalid config type from repo, expected *config.Boost but got %T", c)
		}

		return Options(
			Override(new(lotus_repo.LockedRepo), lotus_modules.LockedRepo(lr)), // module handles closing

			Override(new(ci.PrivKey), lotus_lp2p.PrivKey),
			Override(new(ci.PubKey), ci.PrivKey.GetPublic),
			Override(new(peer.ID), peer.IDFromPublicKey),

			Override(new(types.KeyStore), modules.KeyStore),

			Override(new(*lotus_dtypes.APIAlg), lotus_modules.APISecret),

			ConfigBoost(cfg),
		)(settings)
	}
}

type StopFunc func(context.Context) error

// New builds and starts new Filecoin node
func New(ctx context.Context, opts ...Option) (StopFunc, error) {
	settings := Settings{
		modules: map[interface{}]fx.Option{},
		invokes: make([]fx.Option, _nInvokes),
	}

	// apply module options in the right order
	if err := Options(Options(defaults()...), Options(opts...))(&settings); err != nil {
		return nil, fmt.Errorf("applying node options failed: %w", err)
	}

	// gather constructors for fx.Options
	ctors := make([]fx.Option, 0, len(settings.modules))
	for _, opt := range settings.modules {
		ctors = append(ctors, opt)
	}

	// fill holes in invokes for use in fx.Options
	for i, opt := range settings.invokes {
		if opt == nil {
			settings.invokes[i] = fx.Options()
		}
	}

	app := fx.New(
		fx.Options(ctors...),
		fx.Options(settings.invokes...),

		fx.NopLogger,
	)

	// TODO: we probably should have a 'firewall' for Closing signal
	//  on this context, and implement closing logic through lifecycles
	//  correctly
	if err := app.Start(ctx); err != nil {
		// comment fx.NopLogger few lines above for easier debugging
		return nil, fmt.Errorf("starting node: %w", err)
	}

	return app.Stop, nil
}

var BoostNode = Options(
	Override(new(sealer.StorageAuth), lotus_modules.StorageAuth),

	// Actor config
	Override(new(lotus_dtypes.MinerAddress), lotus_modules.MinerAddress),
	Override(new(lotus_dtypes.MinerID), lotus_modules.MinerID),

	Override(new(lotus_dtypes.NetworkName), lotus_modules.StorageNetworkName),
	Override(new(*sql.DB), modules.NewBoostDB),
	Override(new(*modules.LogSqlDB), modules.NewLogsSqlDB),
	Override(new(*modules.RetrievalSqlDB), modules.NewRetrievalSqlDB),
	Override(HandleCreateRetrievalTablesKey, modules.CreateRetrievalTables),
	Override(new(*db.DealsDB), modules.NewDealsDB),
	Override(new(*db.LogsDB), modules.NewLogsDB),
	Override(new(*db.ProposalLogsDB), modules.NewProposalLogsDB),
	Override(new(*db.FundsDB), modules.NewFundsDB),
	Override(new(*db.SectorStateDB), modules.NewSectorStateDB),
	Override(new(*rtvllog.RetrievalLogDB), modules.NewRetrievalLogDB),
)

func ConfigBoost(cfg *config.Boost) Option {
	pricingConfig := cfg.Dealmaking.RetrievalPricing
	if pricingConfig.Strategy == config.RetrievalPricingExternalMode {
		if pricingConfig.External == nil {
			return Error(errors.New("retrieval pricing policy has been to set to external but external policy config is nil"))
		}

		if pricingConfig.External.Path == "" {
			return Error(errors.New("retrieval pricing policy has been to set to external but external script path is empty"))
		}
	} else if pricingConfig.Strategy != config.RetrievalPricingDefaultMode {
		return Error(errors.New("retrieval pricing policy must be either default or external"))
	}

	collatWalletStr := cfg.Wallets.DealCollateral
	if collatWalletStr == "" && cfg.Wallets.PledgeCollateral != "" { // nolint:staticcheck
		collatWalletStr = cfg.Wallets.PledgeCollateral // nolint:staticcheck
	}
	walletDealCollat, err := address.NewFromString(collatWalletStr)
	if err != nil {
		return Error(fmt.Errorf("failed to parse deal collateral wallet: '%s'; err: %w", collatWalletStr, err))
	}
	walletPSD, err := address.NewFromString(cfg.Wallets.PublishStorageDeals)
	if err != nil {
		return Error(fmt.Errorf("failed to parse cfg.Wallets.PublishStorageDeals: %s; err: %w", cfg.Wallets.PublishStorageDeals, err))
	}
	walletMiner, err := address.NewFromString(cfg.Wallets.Miner)
	if err != nil {
		return Error(fmt.Errorf("failed to parse cfg.Wallets.Miner: %s; err: %w", cfg.Wallets.Miner, err))
	}
	if len(cfg.DAGStore.RootDir) > 0 {
		return Error(fmt.Errorf("Detected custom DAG store path %s. The DAG store must be at $BOOST_PATH/dagstore", cfg.DAGStore.RootDir))
	}

	legacyFees := cfg.LotusFees.Legacy()

	return Options(
		ConfigCommon(&cfg.Common),

		Override(CheckFDLimit, lotus_modules.CheckFdLimit(build.BoostFDLimit)), // recommend at least 100k FD limit to miners

		Override(new(lotus_dtypes.DrandSchedule), lotus_modules.BuiltinDrandConfig),
		Override(new(lotus_dtypes.BootstrapPeers), lotus_modules.BuiltinBootstrap),
		Override(new(lotus_dtypes.DrandBootstrap), lotus_modules.DrandBootstrap),

		Override(new(paths.LocalStorage), From(new(lotus_repo.LockedRepo))),
		Override(new(*paths.Local), lotus_modules.LocalStorage),
		Override(new(lotus_config.SealerConfig), cfg.StorageManager()),
		Override(new(*paths.Remote), lotus_modules.RemoteStorage),

		Override(new(*fundmanager.FundManager), fundmanager.New(fundmanager.Config{
			Enabled:      cfg.Dealmaking.FundsTaggingEnabled,
			StorageMiner: walletMiner,
			CollatWallet: walletDealCollat,
			PubMsgWallet: walletPSD,
			PubMsgBalMin: abi.TokenAmount(cfg.LotusFees.MaxPublishDealsFee),
		})),

		Override(new(*storagemanager.StorageManager), storagemanager.New(storagemanager.Config{
			MaxStagingDealsBytes:          uint64(cfg.Dealmaking.MaxStagingDealsBytes),
			MaxStagingDealsPercentPerHost: uint64(cfg.Dealmaking.MaxStagingDealsPercentPerHost),
		})),

		// Sector API
		Override(new(sectorblocks.SectorBuilder), From(new(lotus_modules.MinerStorageService))),

		Override(new(*sectorblocks.SectorBlocks), sectorblocks.NewSectorBlocks),

		// Sealing Pipeline State API
		Override(new(sealingpipeline.API), From(new(lotus_modules.MinerStorageService))),

		Override(new(*indexprovider.Wrapper), indexprovider.NewWrapper(cfg)),

		Override(new(*storagemarket.ChainDealManager), modules.NewChainDealManager),
		Override(new(smtypes.CommpCalculator), From(new(lotus_modules.MinerStorageService))),

		Override(new(*storagemarket.Provider), modules.NewStorageMarketProvider(walletMiner, cfg)),

		// GraphQL server
		Override(new(gql.BlockGetter), From(new(dtypes.IndexBackedBlockstore))),
		Override(new(*gql.Server), modules.NewGraphqlServer(cfg)),

		// Tracing
		Override(new(*tracing.Tracing), modules.NewTracing(cfg)),

		// Address selector
		Override(new(*ctladdr.AddressSelector), lotus_modules.AddressSelector(&lotus_config.MinerAddressConfig{
			DealPublishControl: []string{cfg.Wallets.PublishStorageDeals},
		})),

		// Lotus Markets
		Override(new(dtypes.ProviderTransferNetwork), modules.NewProviderTransferNetwork),
		Override(new(*modules.ProxyAskGetter), modules.NewAskGetter),
		Override(new(server.AskGetter), From(new(*modules.ProxyAskGetter))),
		Override(new(*modules.LinkSystemProv), modules.NewLinkSystemProvider),
		Override(new(server.LinkSystemProvider), From(new(*modules.LinkSystemProv))),
		Override(new(*server.GraphsyncUnpaidRetrieval), modules.RetrievalGraphsync(cfg.LotusDealmaking.SimultaneousTransfersForStorage, cfg.LotusDealmaking.SimultaneousTransfersForStoragePerClient, cfg.LotusDealmaking.SimultaneousTransfersForRetrieval)),
		Override(new(dtypes.StagingGraphsync), From(new(*server.GraphsyncUnpaidRetrieval))),
		Override(new(dtypes.ProviderPieceStore), modules.NewProviderPieceStore),

		// Lotus Markets (retrieval deps)
		Override(new(sealer.PieceProvider), sealer.NewPieceProvider),

		Override(new(dtypes.RetrievalPricingFunc), modules.RetrievalPricingFunc(config.DealmakingConfig{
			RetrievalPricing: &lotus_config.RetrievalPricing{
				Strategy: config.RetrievalPricingDefaultMode,
				Default:  &lotus_config.RetrievalPricingDefault{},
			},
		})),

		// DAG Store
		Override(new(lotus_dtypes.ProviderPieceStore), modules.NewLotusGFMProviderPieceStore),
		Override(new(mdagstore.MinerAPI), lotus_modules.NewMinerAPI(cfg.DAGStore)),
		Override(DAGStoreKey, lotus_modules.DAGStore(cfg.DAGStore)),
		Override(new(dagstore.Interface), From(new(*dagstore.DAGStore))),
		Override(new(stores.DAGStoreWrapper), modules.NewBoostGFMDAGStoreWrapper),
		Override(new(*modules.ShardSelector), modules.NewShardSelector),
		Override(new(dtypes.IndexBackedBlockstore), modules.NewIndexBackedBlockstore(cfg)),
		Override(HandleSetShardSelector, modules.SetShardSelectorFunc),

		// Lotus Markets (retrieval)
		Override(new(mdagstore.SectorAccessor), modules.NewSectorAccessor(cfg)),
		Override(new(retrievalmarket.SectorAccessor), From(new(mdagstore.SectorAccessor))),
		Override(new(retrievalmarket.RetrievalProviderNode), retrievaladapter.NewRetrievalProviderNode),
		Override(new(rmnet.RetrievalMarketNetwork), modules.RetrievalNetwork),
		Override(new(retrievalmarket.RetrievalProvider), modules.RetrievalProvider),
		Override(HandleSetRetrievalAskGetter, modules.SetAskGetter),
		Override(HandleRetrievalEventsKey, modules.HandleRetrievalGraphsyncUpdates(time.Duration(cfg.Dealmaking.RetrievalLogDuration), time.Duration(cfg.Dealmaking.StalledRetrievalTimeout))),
		Override(HandleRetrievalKey, modules.HandleRetrieval),
		Override(new(*lp2pimpl.TransportsListener), modules.NewTransportsListener(cfg)),
		Override(new(*protocolproxy.ProtocolProxy), modules.NewProtocolProxy(cfg)),
		Override(HandleRetrievalTransportsKey, modules.HandleRetrievalTransports),
		Override(HandleProtocolProxyKey, modules.HandleProtocolProxy),
		Override(new(idxprov.MeshCreator), idxprov.NewMeshCreator),
		Override(new(provider.Interface), modules.IndexProvider(cfg.IndexProvider)),

		// Lotus Markets (storage)
		Override(new(dtypes.ProviderTransport), modules.NewProviderTransport),
		Override(new(dtypes.ProviderDataTransfer), modules.NewProviderDataTransfer),
		Override(new(*storedask.StoredAsk), modules.NewStorageAsk),

		Override(new(gfm_storagemarket.StorageProviderNode), storageadapter.NewProviderNodeAdapter(&legacyFees, &cfg.LotusDealmaking)),
		Override(new(gfm_storagemarket.StorageProvider), modules.NewLegacyStorageProvider(cfg)),
		Override(HandleDealsKey, modules.HandleLegacyDeals),
		Override(HandleBoostDealsKey, modules.HandleBoostLibp2pDeals),
		Override(HandleContractDealsKey, modules.HandleContractDeals(&cfg.ContractDeals)),
		Override(HandleProposalLogCleanerKey, modules.HandleProposalLogCleaner(time.Duration(cfg.Dealmaking.DealProposalLogDuration))),
		Override(HandleSetLinkSystem, modules.SetLinkSystem),

		// Boost storage deal filter
		Override(new(dtypes.StorageDealFilter), modules.BasicDealFilter(cfg.Dealmaking, nil)),
		If(cfg.Dealmaking.Filter != "",
			Override(new(dtypes.StorageDealFilter), modules.BasicDealFilter(cfg.Dealmaking, dtypes.StorageDealFilter(dealfilter.CliStorageDealFilter(cfg.Dealmaking.Filter)))),
		),

		// Lotus markets storage deal filter
		Override(new(lotus_dtypes.StorageDealFilter), lotus_modules.BasicDealFilter(cfg.LotusDealmaking, nil)),
		If(cfg.LotusDealmaking.Filter != "",
			Override(new(lotus_dtypes.StorageDealFilter), lotus_modules.BasicDealFilter(cfg.LotusDealmaking, lotus_dealfilter.CliStorageDealFilter(cfg.LotusDealmaking.Filter))),
		),
		Override(new(storageimpl.DealDeciderFunc), modules.DealDeciderFn),

		// Boost retrieval deal filter
		Override(new(dtypes.RetrievalDealFilter), modules.RetrievalDealFilter(nil)),
		If(cfg.Dealmaking.RetrievalFilter != "",
			Override(new(dtypes.RetrievalDealFilter), modules.RetrievalDealFilter(dtypes.RetrievalDealFilter(dealfilter.CliRetrievalDealFilter(cfg.Dealmaking.RetrievalFilter)))),
		),

		// Lotus markets retrieval deal filter
		Override(new(lotus_gfm_storagemarket.StorageProviderNode), modules.LotusGFMStorageProviderNode),
		Override(new(lotus_dtypes.RetrievalDealFilter), lotus_modules.RetrievalDealFilter(nil)),
		If(cfg.LotusDealmaking.RetrievalFilter != "",
			Override(new(lotus_dtypes.RetrievalDealFilter), lotus_modules.RetrievalDealFilter(lotus_dealfilter.CliRetrievalDealFilter(cfg.LotusDealmaking.RetrievalFilter))),
		),

		Override(new(*storageadapter.DealPublisher), storageadapter.NewDealPublisher(&legacyFees, storageadapter.PublishMsgConfig{
			Period:                  time.Duration(cfg.LotusDealmaking.PublishMsgPeriod),
			MaxDealsPerMsg:          cfg.LotusDealmaking.MaxDealsPerPublishMsg,
			StartEpochSealingBuffer: cfg.LotusDealmaking.StartEpochSealingBuffer,
		})),

		Override(new(sealer.Unsealer), From(new(lotus_modules.MinerStorageService))),
		Override(new(paths.SectorIndex), From(new(lotus_modules.MinerSealingService))),

		Override(new(lotus_modules.MinerStorageService), lotus_modules.ConnectStorageService(cfg.SectorIndexApiInfo)),
		Override(new(lotus_modules.MinerSealingService), lotus_modules.ConnectSealingService(cfg.SealerApiInfo)),

		Override(new(sealer.StorageAuth), lotus_modules.StorageAuthWithURL(cfg.SectorIndexApiInfo)),
		Override(new(*backupmgr.BackupMgr), modules.NewOnlineBackupMgr(cfg)),

		// Dynamic Lotus configs
		Override(new(lotus_dtypes.ConsiderOnlineStorageDealsConfigFunc), lotus_modules.NewConsiderOnlineStorageDealsConfigFunc),
		Override(new(lotus_dtypes.SetConsiderOnlineStorageDealsConfigFunc), lotus_modules.NewSetConsideringOnlineStorageDealsFunc),
		Override(new(lotus_dtypes.ConsiderOnlineRetrievalDealsConfigFunc), lotus_modules.NewConsiderOnlineRetrievalDealsConfigFunc),
		Override(new(lotus_dtypes.SetConsiderOnlineRetrievalDealsConfigFunc), lotus_modules.NewSetConsiderOnlineRetrievalDealsConfigFunc),
		Override(new(lotus_dtypes.StorageDealPieceCidBlocklistConfigFunc), lotus_modules.NewStorageDealPieceCidBlocklistConfigFunc),
		Override(new(lotus_dtypes.SetStorageDealPieceCidBlocklistConfigFunc), lotus_modules.NewSetStorageDealPieceCidBlocklistConfigFunc),
		Override(new(lotus_dtypes.ConsiderOfflineStorageDealsConfigFunc), lotus_modules.NewConsiderOfflineStorageDealsConfigFunc),
		Override(new(lotus_dtypes.SetConsiderOfflineStorageDealsConfigFunc), lotus_modules.NewSetConsideringOfflineStorageDealsFunc),
		Override(new(lotus_dtypes.ConsiderOfflineRetrievalDealsConfigFunc), lotus_modules.NewConsiderOfflineRetrievalDealsConfigFunc),
		Override(new(lotus_dtypes.SetConsiderOfflineRetrievalDealsConfigFunc), lotus_modules.NewSetConsiderOfflineRetrievalDealsConfigFunc),
		Override(new(lotus_dtypes.ConsiderVerifiedStorageDealsConfigFunc), lotus_modules.NewConsiderVerifiedStorageDealsConfigFunc),
		Override(new(lotus_dtypes.SetConsiderVerifiedStorageDealsConfigFunc), lotus_modules.NewSetConsideringVerifiedStorageDealsFunc),
		Override(new(lotus_dtypes.ConsiderUnverifiedStorageDealsConfigFunc), lotus_modules.NewConsiderUnverifiedStorageDealsConfigFunc),
		Override(new(lotus_dtypes.SetConsiderUnverifiedStorageDealsConfigFunc), lotus_modules.NewSetConsideringUnverifiedStorageDealsFunc),
		Override(new(lotus_dtypes.SetExpectedSealDurationFunc), lotus_modules.NewSetExpectedSealDurationFunc),
		Override(new(lotus_dtypes.GetExpectedSealDurationFunc), lotus_modules.NewGetExpectedSealDurationFunc),
		Override(new(lotus_dtypes.SetMaxDealStartDelayFunc), lotus_modules.NewSetMaxDealStartDelayFunc),
		Override(new(lotus_dtypes.GetMaxDealStartDelayFunc), lotus_modules.NewGetMaxDealStartDelayFunc),

		// Dynamic Boost configs
		Override(new(dtypes.ConsiderOnlineStorageDealsConfigFunc), modules.NewConsiderOnlineStorageDealsConfigFunc),
		Override(new(dtypes.SetConsiderOnlineStorageDealsConfigFunc), modules.NewSetConsideringOnlineStorageDealsFunc),
		Override(new(dtypes.ConsiderOnlineRetrievalDealsConfigFunc), modules.NewConsiderOnlineRetrievalDealsConfigFunc),
		Override(new(dtypes.SetConsiderOnlineRetrievalDealsConfigFunc), modules.NewSetConsiderOnlineRetrievalDealsConfigFunc),
		Override(new(dtypes.StorageDealPieceCidBlocklistConfigFunc), modules.NewStorageDealPieceCidBlocklistConfigFunc),
		Override(new(dtypes.SetStorageDealPieceCidBlocklistConfigFunc), modules.NewSetStorageDealPieceCidBlocklistConfigFunc),
		Override(new(dtypes.ConsiderOfflineStorageDealsConfigFunc), modules.NewConsiderOfflineStorageDealsConfigFunc),
		Override(new(dtypes.SetConsiderOfflineStorageDealsConfigFunc), modules.NewSetConsideringOfflineStorageDealsFunc),
		Override(new(dtypes.ConsiderOfflineRetrievalDealsConfigFunc), modules.NewConsiderOfflineRetrievalDealsConfigFunc),
		Override(new(dtypes.SetConsiderOfflineRetrievalDealsConfigFunc), modules.NewSetConsiderOfflineRetrievalDealsConfigFunc),
		Override(new(dtypes.ConsiderVerifiedStorageDealsConfigFunc), modules.NewConsiderVerifiedStorageDealsConfigFunc),
		Override(new(dtypes.SetConsiderVerifiedStorageDealsConfigFunc), modules.NewSetConsideringVerifiedStorageDealsFunc),
		Override(new(dtypes.ConsiderUnverifiedStorageDealsConfigFunc), modules.NewConsiderUnverifiedStorageDealsConfigFunc),
		Override(new(dtypes.SetConsiderUnverifiedStorageDealsConfigFunc), modules.NewSetConsideringUnverifiedStorageDealsFunc),
		Override(new(dtypes.SetExpectedSealDurationFunc), modules.NewSetExpectedSealDurationFunc),
		Override(new(dtypes.GetExpectedSealDurationFunc), modules.NewGetExpectedSealDurationFunc),
		Override(new(dtypes.SetMaxDealStartDelayFunc), modules.NewSetMaxDealStartDelayFunc),
		Override(new(dtypes.GetMaxDealStartDelayFunc), modules.NewGetMaxDealStartDelayFunc),
	)
}

func BoostAPI(out *api.Boost) Option {
	return Options(
		ApplyIf(func(s *Settings) bool { return s.Config },
			Error(errors.New("the StorageMiner option must be set before Config option")),
		),

		func(s *Settings) error {
			s.nodeType = repo.Boost
			return nil
		},

		func(s *Settings) error {
			resAPI := &impl.BoostAPI{}
			s.invokes[ExtractApiKey] = fx.Populate(resAPI)
			*out = resAPI
			return nil
		},
	)
}
