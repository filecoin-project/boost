package node

import (
	"context"
	"errors"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storage/sectorblocks"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/journal"
	"github.com/filecoin-project/boost/journal/alerting"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/impl"
	"github.com/filecoin-project/boost/node/impl/common"
	"github.com/filecoin-project/boost/node/impl/net"
	"github.com/filecoin-project/boost/node/modules"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/node/modules/helpers"
	"github.com/filecoin-project/boost/node/modules/lp2p"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/lotus/chain/types"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	_ "github.com/filecoin-project/lotus/lib/sigs/bls"
	_ "github.com/filecoin-project/lotus/lib/sigs/secp"
	"github.com/filecoin-project/lotus/system"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-metrics-interface"
	ci "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/routing"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/p2p/net/conngater"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

//nolint:deadcode,varcheck
var log = logging.Logger("builder")

// special is a type used to give keys to modules which
//  can't really be identified by the returned type
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
)

type invoke int

// Invokes are called in the order they are defined.
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
	HandleRetrievalKey
	RunSectorServiceKey

	// daemon
	ExtractApiKey
	HeadMetricsKey
	SettlePaymentChannelsKey
	RunPeerTaggerKey
	SetupFallbackBlockstoresKey

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

	nodeType repo.RepoType

	Base   bool // Base option applied
	Config bool // Config option applied
	Lite   bool // Start node in "lite" mode
}

// Basic lotus-app services
func defaults() []Option {
	return []Option{
		// global system journal.
		//TODO (anteva): FIXME: not sure why these are not working

		Override(new(journal.DisabledEvents), journal.EnvDisabledEvents),
		Override(new(journal.Journal), modules.OpenFilesystemJournal),
		Override(new(*alerting.Alerting), alerting.NewAlertingSystem),

		Override(CheckFDLimit, modules.CheckFdLimit(build.DefaultFDLimit)),

		Override(new(system.MemoryConstraints), modules.MemoryConstraints),
		//Override(InitMemoryWatchdog, modules.MemoryWatchdog),

		Override(new(helpers.MetricsCtx), func() context.Context {
			return metrics.CtxScope(context.Background(), "boost")
		}),

		Override(new(dtypes.ShutdownChan), make(chan struct{})),
	}
}

var LibP2P = Options(
	// Host config
	Override(new(dtypes.Bootstrapper), dtypes.Bootstrapper(false)),

	// Host dependencies
	Override(new(peerstore.Peerstore), pstoremem.NewPeerstore),
	Override(PstoreAddSelfKeysKey, lp2p.PstoreAddSelfKeys),
	Override(StartListeningKey, lp2p.StartListening([]string{"/ip4/127.0.0.1/tcp/1899"})),

	// Host settings
	Override(DefaultTransportsKey, lp2p.DefaultTransports),
	Override(AddrsFactoryKey, lp2p.AddrsFactory(nil, nil)),
	Override(SmuxTransportKey, lp2p.SmuxTransport(true)),
	Override(RelayKey, lp2p.NoRelay()),
	Override(SecurityKey, lp2p.Security(true, false)),

	// Host
	Override(new(lp2p.RawHost), lp2p.Host),
	Override(new(host.Host), lp2p.RoutedHost),
	Override(new(lp2p.BaseIpfsRouting), lp2p.DHTRouting(dht.ModeAuto)),

	Override(DiscoveryHandlerKey, lp2p.DiscoveryHandler),

	// Routing
	Override(new(record.Validator), modules.RecordValidator),
	Override(BaseRoutingKey, lp2p.BaseRouting),
	Override(new(routing.Routing), lp2p.Routing),

	// Services
	Override(BandwidthReporterKey, lp2p.BandwidthCounter),
	Override(AutoNATSvcKey, lp2p.AutoNATService),

	// Services (connection management)
	Override(ConnectionManagerKey, lp2p.ConnectionManager(50, 200, 20*time.Second, nil)),
	Override(new(*conngater.BasicConnectionGater), lp2p.ConnGater),
	Override(ConnGaterKey, lp2p.ConnGaterOption),
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
		Override(SetApiEndpointKey, func(lr repo.LockedRepo, e dtypes.APIEndpoint) error {
			return lr.SetAPIEndpoint(e)
		}),
		Override(new(stores.URLs), func(e dtypes.APIEndpoint) (stores.URLs, error) {
			ip := cfg.API.RemoteListenAddress

			var urls stores.URLs
			urls = append(urls, "http://"+ip+"/remote") // TODO: This makes no assumptions, and probably could...
			return urls, nil
		}),
		ApplyIf(func(s *Settings) bool { return s.Base }), // apply only if Base has already been applied
		Override(new(api.Net), From(new(net.NetAPI))),
		Override(new(api.Common), From(new(common.CommonAPI))),
		Override(new(dtypes.MetadataDS), modules.Datastore(cfg.Backup.DisableMetadataLog)),
		Override(StartListeningKey, lp2p.StartListening(cfg.Libp2p.ListenAddresses)),
		Override(ConnectionManagerKey, lp2p.ConnectionManager(
			cfg.Libp2p.ConnMgrLow,
			cfg.Libp2p.ConnMgrHigh,
			time.Duration(cfg.Libp2p.ConnMgrGrace),
			cfg.Libp2p.ProtectedPeers)),
		ApplyIf(func(s *Settings) bool { return len(cfg.Libp2p.BootstrapPeers) > 0 },
			Override(new(dtypes.BootstrapPeers), modules.ConfigBootstrap(cfg.Libp2p.BootstrapPeers)),
		),
		Override(AddrsFactoryKey, lp2p.AddrsFactory(
			cfg.Libp2p.AnnounceAddresses,
			cfg.Libp2p.NoAnnounceAddresses)),
		If(!cfg.Libp2p.DisableNatPortMap, Override(NatPortMapKey, lp2p.NatPortMap)),
	)
}

func Repo(r repo.Repo) Option {
	return func(settings *Settings) error {
		lr, err := r.Lock(settings.nodeType)
		if err != nil {
			return err
		}
		c, err := lr.Config()
		if err != nil {
			return err
		}
		return Options(
			Override(new(repo.LockedRepo), modules.LockedRepo(lr)), // module handles closing

			Override(new(ci.PrivKey), lp2p.PrivKey),
			Override(new(ci.PubKey), ci.PrivKey.GetPublic),
			Override(new(peer.ID), peer.IDFromPublicKey),

			Override(new(types.KeyStore), modules.KeyStore),

			Override(new(*dtypes.APIAlg), modules.APISecret),

			ConfigBoost(c),
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
		return nil, xerrors.Errorf("applying node options failed: %w", err)
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
		return nil, xerrors.Errorf("starting node: %w", err)
	}

	return app.Stop, nil
}

var BoostNode = Options(
	Override(new(sectorstorage.StorageAuth), modules.StorageAuth),

	// Actor config
	Override(new(dtypes.MinerAddress), modules.MinerAddress),
	Override(new(dtypes.MinerID), modules.MinerID),

	Override(new(dtypes.NetworkName), modules.StorageNetworkName),
	Override(new(*db.DealsDB), modules.NewStorageMarketDB),
	Override(new(*gql.Server), modules.NewGraphqlServer),
)

func ConfigBoost(c interface{}) Option {
	cfg, ok := c.(*config.Boost)
	if !ok {
		return Error(xerrors.Errorf("invalid config from repo, got: %T", c))
	}

	pricingConfig := cfg.Dealmaking.RetrievalPricing
	if pricingConfig.Strategy == config.RetrievalPricingExternalMode {
		if pricingConfig.External == nil {
			return Error(xerrors.New("retrieval pricing policy has been to set to external but external policy config is nil"))
		}

		if pricingConfig.External.Path == "" {
			return Error(xerrors.New("retrieval pricing policy has been to set to external but external script path is empty"))
		}
	} else if pricingConfig.Strategy != config.RetrievalPricingDefaultMode {
		return Error(xerrors.New("retrieval pricing policy must be either default or external"))
	}

	return Options(
		ConfigCommon(&cfg.Common),

		Override(CheckFDLimit, modules.CheckFdLimit(build.BoostFDLimit)), // recommend at least 100k FD limit to miners

		Override(new(stores.LocalStorage), From(new(repo.LockedRepo))),
		Override(new(*stores.Local), modules.LocalStorage),
		Override(new(*stores.Remote), modules.RemoteStorage),

		Override(new(modules.MinerStorageService), modules.ConnectStorageService(cfg.SectorIndexApiInfo)),

		Override(new(*storagemarket.DealPublisher), storagemarket.NewDealPublisher(storagemarket.PublishMsgConfig{
			Wallet:                  cfg.Wallets.PublishStorageDeals,
			Period:                  time.Duration(cfg.Dealmaking.PublishMsgPeriod),
			MaxDealsPerMsg:          cfg.Dealmaking.PublishMsgMaxDealsPerMsg,
			StartEpochSealingBuffer: cfg.Dealmaking.StartEpochSealingBuffer,
			MaxPublishDealsFee:      cfg.Dealmaking.PublishMsgMaxFee,
		})),

		// Sector API
		Override(new(modules.MinerStorageService), modules.ConnectStorageService(cfg.SectorIndexApiInfo)),
		Override(new(sectorblocks.SectorBuilder), From(new(modules.MinerStorageService))),
		Override(new(*sectorblocks.SectorBlocks), sectorblocks.NewSectorBlocks),

		Override(new(*storagemarket.Provider), modules.NewStorageMarketProvider(cfg.Wallets.Miner)),
	)
}

func Boost(out *api.Boost) Option {
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
