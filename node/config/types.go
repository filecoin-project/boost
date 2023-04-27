package config

import (
	"github.com/filecoin-project/lotus/chain/types"
	lotus_config "github.com/filecoin-project/lotus/node/config"
	"github.com/ipfs/go-cid"
)

// // NOTE: ONLY PUT STRUCT DEFINITIONS IN THIS FILE
// //
// // After making edits here, run 'make cfgdoc-gen' (or 'make gen')

// Common is common config between full node and miner
type Common struct {
	API    lotus_config.API
	Backup lotus_config.Backup
	Libp2p lotus_config.Libp2p
	Pubsub lotus_config.Pubsub
}

type Backup struct {
	// When set to true disables metadata log (.lotus/kvlog). This can save disk
	// space by reducing metadata redundancy.
	//
	// Note that in case of metadata corruption it might be much harder to recover
	// your node if metadata log is disabled
	DisableMetadataLog bool
}

// Boost is a boost service config
type Boost struct {
	// The version of the config file (used for migrations)
	ConfigVersion int

	Common

	Storage StorageConfig
	// The connect string for the sealing RPC API (lotus miner)
	SealerApiInfo string
	// The connect string for the sector index RPC API (lotus miner)
	SectorIndexApiInfo string
	Dealmaking         DealmakingConfig
	Wallets            WalletsConfig
	Graphql            GraphqlConfig
	Tracing            TracingConfig
	ContractDeals      ContractDealsConfig

	// Lotus configs
	LotusDealmaking lotus_config.DealmakingConfig
	LotusFees       FeeConfig
	DAGStore        lotus_config.DAGStoreConfig
	IndexProvider   lotus_config.IndexProviderConfig
}

func (b *Boost) GetDealmakingConfig() lotus_config.DealmakingConfig {
	return b.LotusDealmaking
}

func (b *Boost) SetDealmakingConfig(other lotus_config.DealmakingConfig) {
	b.LotusDealmaking = other
}

type WalletsConfig struct {
	// The "owner" address of the miner
	Miner string
	// The wallet used to send PublishStorageDeals messages.
	// Must be a control or worker address of the miner.
	PublishStorageDeals string
	// The wallet used as the source for storage deal collateral
	DealCollateral string
	// Deprecated: Renamed to DealCollateral
	PledgeCollateral string
}

type GraphqlConfig struct {
	// The ip address the GraphQL server will bind to. Default: 0.0.0.0
	ListenAddress string
	// The port that the graphql server listens on
	Port uint64
}

type TracingConfig struct {
	Enabled     bool
	ServiceName string
	Endpoint    string
}

type LotusDealmakingConfig struct {
	// A list of Data CIDs to reject when making deals
	PieceCidBlocklist []cid.Cid
	// Maximum expected amount of time getting the deal into a sealed sector will take
	// This includes the time the deal will need to get transferred and published
	// before being assigned to a sector
	ExpectedSealDuration Duration
	// Maximum amount of time proposed deal StartEpoch can be in future
	MaxDealStartDelay Duration
	// When a deal is ready to publish, the amount of time to wait for more
	// deals to be ready to publish before publishing them all as a batch
	PublishMsgPeriod Duration
	// The maximum number of deals to include in a single PublishStorageDeals
	// message
	MaxDealsPerPublishMsg uint64
	// The maximum collateral that the provider will put up against a deal,
	// as a multiplier of the minimum collateral bound
	MaxProviderCollateralMultiplier uint64
	// The maximum allowed disk usage size in bytes of staging deals not yet
	// passed to the sealing node by the markets service. 0 is unlimited.
	MaxStagingDealsBytes int64
	// The maximum number of parallel online data transfers for storage deals
	SimultaneousTransfersForStorage uint64
	// The maximum number of simultaneous data transfers from any single client
	// for storage deals.
	// Unset by default (0), and values higher than SimultaneousTransfersForStorage
	// will have no effect; i.e. the total number of simultaneous data transfers
	// across all storage clients is bound by SimultaneousTransfersForStorage
	// regardless of this number.
	SimultaneousTransfersForStoragePerClient uint64
	// The maximum number of parallel online data transfers for retrieval deals
	SimultaneousTransfersForRetrieval uint64
	// Minimum start epoch buffer to give time for sealing of sector with deal.
	StartEpochSealingBuffer uint64

	// A command used for fine-grained evaluation of storage deals
	// see https://boost.filecoin.io/configuration/deal-filters for more details
	Filter string
	// A command used for fine-grained evaluation of retrieval deals
	// see https://boost.filecoin.io/configuration/deal-filters for more details
	RetrievalFilter string

	RetrievalPricing *lotus_config.RetrievalPricing
}

type DealmakingConfig struct {
	// When enabled, the miner can accept online deals
	ConsiderOnlineStorageDeals bool
	// When enabled, the miner can accept offline deals
	ConsiderOfflineStorageDeals bool
	// When enabled, the miner can accept retrieval deals
	ConsiderOnlineRetrievalDeals bool
	// When enabled, the miner can accept offline retrieval deals
	ConsiderOfflineRetrievalDeals bool
	// When enabled, the miner can accept verified deals
	ConsiderVerifiedStorageDeals bool
	// When enabled, the miner can accept unverified deals
	ConsiderUnverifiedStorageDeals bool
	// A list of Data CIDs to reject when making deals
	PieceCidBlocklist []cid.Cid
	// Maximum expected amount of time getting the deal into a sealed sector will take
	// This includes the time the deal will need to get transferred and published
	// before being assigned to a sector
	ExpectedSealDuration Duration
	// Maximum amount of time proposed deal StartEpoch can be in future
	MaxDealStartDelay Duration
	// The maximum collateral that the provider will put up against a deal,
	// as a multiplier of the minimum collateral bound
	MaxProviderCollateralMultiplier uint64
	// The maximum allowed disk usage size in bytes of downloaded deal data
	// that has not yet been passed to the sealing node by boost.
	// When the client makes a new deal proposal to download data from a host,
	// boost checks this config value against the sum of:
	// - the amount of data downloaded in the staging area
	// - the amount of data that is queued for download
	// - the amount of data in the proposed deal
	// If the total amount would exceed the limit, boost rejects the deal.
	// Set this value to 0 to indicate there is no limit.
	MaxStagingDealsBytes int64
	// The percentage of MaxStagingDealsBytes that is allocated to each host.
	// When the client makes a new deal proposal to download data from a host,
	// boost checks this config value against the sum of:
	// - the amount of data downloaded from the host in the staging area
	// - the amount of data that is queued for download from the host
	// - the amount of data in the proposed deal
	// If the total amount would exceed the limit, boost rejects the deal.
	// Set this value to 0 to indicate there is no limit per host.
	MaxStagingDealsPercentPerHost uint64
	// Minimum start epoch buffer to give time for sealing of sector with deal.
	StartEpochSealingBuffer uint64
	// The amount of time to keep deal proposal logs for before cleaning them up.
	DealProposalLogDuration Duration
	// The amount of time to keep retrieval deal logs for before cleaning them up.
	// Note RetrievalLogDuration should exceed the StalledRetrievalTimeout as the
	// logs db is leveraged for pruning stalled retrievals.
	RetrievalLogDuration Duration
	// The amount of time stalled retrieval deals will remain open before being canceled.
	StalledRetrievalTimeout Duration

	// A command used for fine-grained evaluation of storage deals
	// see https://boost.filecoin.io/configuration/deal-filters for more details
	Filter string
	// A command used for fine-grained evaluation of retrieval deals
	// see https://boost.filecoin.io/configuration/deal-filters for more details
	RetrievalFilter string

	RetrievalPricing *lotus_config.RetrievalPricing

	// The maximum number of shards cached by the Dagstore for retrieval
	// Lower this limit if boostd memory is too high during retrievals
	BlockstoreCacheMaxShards int
	// How long a blockstore shard should be cached before expiring without use
	BlockstoreCacheExpiry Duration

	// How long to cache calls to check whether a sector is unsealed
	IsUnsealedCacheExpiry Duration

	// The maximum amount of time a transfer can take before it fails
	MaxTransferDuration Duration

	// Whether to do commp on the Boost node (local) or on the Sealer (remote)
	// Please note that this only works for v1.2.0 deals and not legacy deals
	RemoteCommp bool
	// The maximum number of commp processes to run in parallel on the local
	// boost process
	MaxConcurrentLocalCommp uint64

	// The public multi-address for retrieving deals with booster-http.
	// Note: Must be in multiaddr format, eg /dns/foo.com/tcp/443/https
	HTTPRetrievalMultiaddr string

	// The maximum number of concurrent storage deal HTTP downloads.
	// Note that this is a soft maximum; if some downloads stall,
	// more downloads are allowed to start.
	HttpTransferMaxConcurrentDownloads uint64
	// The period between checking if downloads have stalled.
	HttpTransferStallCheckPeriod Duration
	// The time that can elapse before a download is considered stalled (and
	// another concurrent download is allowed to start).
	HttpTransferStallTimeout Duration

	// The libp2p peer id used by booster-bitswap.
	// Run 'booster-bitswap init' to get the peer id.
	// When BitswapPeerID is not empty boostd will:
	// - listen on bitswap protocols on boostd's own peer id and proxy
	//   requests to booster-bitswap
	// - advertise boostd's peer id in bitswap records to the content indexer
	//   (bitswap clients connect to boostd, which proxies the requests to
	//   booster-bitswap)
	// - list bitswap as an available transport on the retrieval transport protocol
	BitswapPeerID string

	// Public multiaddresses for booster-bitswap.
	// If empty
	// - booster-bitswap is assumed to be running privately
	// - boostd acts as a proxy: it listens on bitswap protocols on boostd's own
	//   peer id and forwards them to booster-bitswap
	// If public addresses are set
	// - boostd announces the booster-bitswap peer id to the indexer as an
	//   extended provider
	// - clients make connections directly to the booster-bitswap process
	//   (boostd does not act as a proxy)
	BitswapPublicAddresses []string

	// If operating in public mode, in order to announce booster-bitswap as an extended provider, this value must point to a
	// a file containing the booster-bitswap peer id's private key. Can be left blank when operating with protocol proxy.
	BitswapPrivKeyFile string

	// The deal logs older than DealLogDurationDays are deleted from the logsDB
	// to keep the size of logsDB in check. Set the value as "0" to disable log cleanup
	DealLogDurationDays int

	// The sealing pipeline status is cached by Boost if deal filters are enabled to avoid constant call to
	// lotus-miner API. SealingPipelineCacheTimeout defines cache timeout value in seconds. Default is 30 seconds.
	// Any value less than 0 will result in use of default
	SealingPipelineCacheTimeout Duration

	// Whether to enable tagging of funds. If enabled, each time a deal is
	// accepted boost will tag funds for that deal so that they cannot be used
	// for any other deal.
	FundsTaggingEnabled bool
}

type ContractDealsConfig struct {
	// Whether to enable chain monitoring in order to accept contract deals
	Enabled bool

	// Allowlist for contracts that this SP should accept deals from
	AllowlistContracts []string

	// From address for eth_ state call
	From string
}

type FeeConfig struct {
	// The maximum fee to pay when sending the PublishStorageDeals message
	MaxPublishDealsFee types.FIL
	// The maximum fee to pay when sending the AddBalance message (used by legacy markets)
	MaxMarketBalanceAddFee types.FIL
}

func (c *FeeConfig) Legacy() lotus_config.MinerFeeConfig {
	return lotus_config.MinerFeeConfig{
		MaxPublishDealsFee:     c.MaxPublishDealsFee,
		MaxMarketBalanceAddFee: c.MaxMarketBalanceAddFee,
	}
}

type StorageConfig struct {
	// The maximum number of concurrent fetch operations to the storage subsystem
	ParallelFetchLimit int
	// How frequently Boost should refresh the state of sectors with Lotus. (default: 1hour)
	// When run, Boost will trigger a storage redeclare on the miner in addition to a storage list.
	// This ensures that index metadata for sectors reflects their status (removed, unsealed, etc).
	StorageListRefreshDuration Duration
	// Whether or not Boost should have lotus redeclare its storage list (default: true).
	// Disable this if you wish to manually handle the refresh. If manually managing the redeclare
	// and it is not triggered, retrieval quality for users will be impacted.
	RedeclareOnStorageListRefresh bool
}
