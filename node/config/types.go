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

	Dealmaking          DealmakingConfig
	Dealpublish         DealPublishConfig
	Wallets             WalletsConfig
	Graphql             GraphqlConfig
	Monitoring          MonitoringConfig
	Tracing             TracingConfig
	LocalIndexDirectory LocalIndexDirectoryConfig
	ContractDeals       ContractDealsConfig
	HttpDownload        HttpDownloadConfig
	Retrievals          RetrievalConfig
	IndexProvider       IndexProviderConfig
}

type WalletsConfig struct {
	// The miner ID
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
	// The ip address the GraphQL server will bind to. Default: 127.0.0.1
	ListenAddress string
	// The port that the graphql server listens on
	Port uint64
}

type TracingConfig struct {
	Enabled     bool
	ServiceName string
	Endpoint    string
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
	// Maximum amount of time proposed deal StartEpoch can be in the future.
	// This is applicable only for online deals as offline deals can take long duration
	// to import the data
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

	// A command used for fine-grained evaluation of storage deals
	// see https://boost.filecoin.io/configuration/deal-filters for more details
	Filter string

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

type IndexProviderConfig struct {
	// Enable set whether to enable indexing announcement to the network and expose endpoints that
	// allow indexer nodes to process announcements. Enabled by default.
	Enable bool

	// EntriesCacheCapacity sets the maximum capacity to use for caching the indexing advertisement
	// entries. Defaults to 1024 if not specified. The cache is evicted using LRU policy. The
	// maximum storage used by the cache is a factor of EntriesCacheCapacity, EntriesChunkSize and
	// the length of multihashes being advertised. For example, advertising 128-bit long multihashes
	// with the default EntriesCacheCapacity, and EntriesChunkSize means the cache size can grow to
	// 256MiB when full.
	EntriesCacheCapacity int

	// EntriesChunkSize sets the maximum number of multihashes to include in a single entries chunk.
	// Defaults to 16384 if not specified. Note that chunks are chained together for indexing
	// advertisements that include more multihashes than the configured EntriesChunkSize.
	EntriesChunkSize int

	// TopicName sets the topic name on which the changes to the advertised content are announced.
	// If not explicitly specified, the topic name is automatically inferred from the network name
	// in following format: '/indexer/ingest/<network-name>'
	// Defaults to empty, which implies the topic name is inferred from network name.
	TopicName string

	// PurgeCacheOnStart sets whether to clear any cached entries chunks when the provider engine
	// starts. By default, the cache is rehydrated from previously cached entries stored in
	// datastore if any is present.
	PurgeCacheOnStart bool

	// The network indexer host that the web UI should link to for published announcements
	WebHost string

	Announce IndexProviderAnnounceConfig

	HttpPublisher IndexProviderHttpPublisherConfig

	// Set this to true to use the legacy data-transfer/graphsync publisher.
	// This should only be used as a temporary fall-back if publishing ipnisync
	// over libp2p or HTTP is not working, and publishing over
	// data-transfer/graphsync was previously working.
	DataTransferPublisher bool
}

type IndexProviderAnnounceConfig struct {
	// Make a direct announcement to a list of indexing nodes over http.
	// Note that announcements are already made over pubsub regardless
	// of this setting.
	AnnounceOverHttp bool

	// The list of URLs of indexing nodes to announce to.
	DirectAnnounceURLs []string
}

type IndexProviderHttpPublisherConfig struct {
	// If enabled, requests are served over HTTP instead of libp2p.
	Enabled bool
	// Set the public hostname / IP for the index provider listener.
	// eg "82.129.73.111"
	// This is usually the same as the for the boost node.
	PublicHostname string
	// Set the port on which to listen for index provider requests over HTTP.
	// Note that this port must be open on the firewall.
	Port int
	// Set this to true to publish HTTP over libp2p in addition to plain HTTP,
	// Otherwise, the publisher will publish content advertisements using only
	// plain HTTP if Enabled is true.
	WithLibp2p bool
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

type MonitoringConfig struct {
	// The number of epochs after which alert is generated for a local pending
	// message in lotus mpool
	MpoolAlertEpochs int64
}

type LocalIndexDirectoryYugabyteConfig struct {
	Enabled bool
	// The yugabyte postgres connect string eg "postgresql://postgres:postgres@localhost"
	ConnectString string
	// The yugabyte cassandra hosts eg ["127.0.0.1"]
	Hosts []string
	// The yugabyte cassandra username eg "cassandra"
	Username string
	// The yugabyte cassandra password eg "cassandra"
	Password string
}

type LocalIndexDirectoryConfig struct {
	Yugabyte LocalIndexDirectoryYugabyteConfig
	Leveldb  LocalIndexDirectoryLeveldbConfig
	// The maximum number of add index operations allowed to execute in parallel.
	// The add index operation is executed when a new deal is created - it fetches
	// the piece from the sealing subsystem, creates an index of where each block
	// is in the piece, and adds the index to the local index directory.
	ParallelAddIndexLimit int
	// AddIndexConcurrency sets the number of concurrent tasks that each add index operation is split into.
	// This setting is usefull to better utilise bandwidth between boostd and boost-data. The default value is 8.
	AddIndexConcurrency int
	// The port that the embedded local index directory data service runs on.
	// Set this value to zero to disable the embedded local index directory data service
	// (in that case the local index directory data service must be running externally)
	EmbeddedServicePort uint64
	// The connect string for the local index directory data service RPC API eg "ws://localhost:8042"
	// Set this value to "" if the local index directory data service is embedded.
	ServiceApiInfo string
	// The RPC timeout when making requests to the boostd-data service
	ServiceRPCTimeout Duration
	// PieceDoctor runs a continuous background process to check each piece in LID for retrievability
	EnablePieceDoctor bool
	// Interval at which LID clean up job should rerun. The cleanup entails removing indices and metadata
	// for the expired/slashed deals. Disabled if set to '0s'. Please DO NOT set a value lower than 6 hours
	// as this task consumes considerable resources and time
	LidCleanupInterval Duration
}

type LocalIndexDirectoryLeveldbConfig struct {
	Enabled bool
}

type HttpDownloadConfig struct {
	// The maximum number of concurrent storage deal HTTP downloads.
	// Note that this is a soft maximum; if some downloads stall,
	// more downloads are allowed to start.
	HttpTransferMaxConcurrentDownloads uint64
	// The period between checking if downloads have stalled.
	HttpTransferStallCheckPeriod Duration
	// The time that can elapse before a download is considered stalled (and
	// another concurrent download is allowed to start).
	HttpTransferStallTimeout Duration
	// NChunks is a number of chunks to split HTTP downloads into. Each chunk is downloaded in the goroutine of its own
	// which improves the overall download speed. NChunks is always equal to 1 for libp2p transport because libp2p server
	// doesn't support range requests yet. NChunks must be greater than 0 and less than 16, with the default of 5.
	NChunks int
	// AllowPrivateIPs defines whether boost should allow HTTP downloads from private IPs as per https://en.wikipedia.org/wiki/Private_network.
	// The default is false.
	AllowPrivateIPs bool
}

type RetrievalConfig struct {
	Graphsync GraphsyncRetrievalConfig
	Bitswap   BitswapRetrievalConfig
	HTTP      HTTPRetrievalConfig
}

type BitswapRetrievalConfig struct {
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
}

type HTTPRetrievalConfig struct {
	// The public multi-address for retrieving deals with booster-http.
	// Note: Must be in multiaddr format, eg /dns/foo.com/tcp/443/https
	HTTPRetrievalMultiaddr string
}

type GraphsyncRetrievalConfig struct {
	// The maximum number of parallel online data transfers for retrieval deals
	SimultaneousTransfersForRetrieval uint64
	// The amount of time to keep retrieval deal logs for before cleaning them up.
	// Note RetrievalLogDuration should exceed the StalledRetrievalTimeout as the
	// logs db is leveraged for pruning stalled retrievals.
	RetrievalLogDuration Duration
	// The amount of time stalled retrieval deals will remain open before being canceled.
	StalledRetrievalTimeout Duration
	// The connect strings for the RPC APIs of each miner that boost can read
	// sector data from when serving graphsync retrievals.
	// If this parameter is not set, boost will serve data from the endpoint
	// configured in SectorIndexApiInfo.
	GraphsyncStorageAccessApiInfo []string
	// A command used for fine-grained evaluation of retrieval deals
	// see https://boost.filecoin.io/configuration/deal-filters for more details
	RetrievalFilter string
}

type DealPublishConfig struct {
	// When set to true, the user is responsible for publishing deals manually.
	// The values of MaxDealsPerPublishMsg and PublishMsgPeriod will be
	// ignored, and deals will remain in the pending state until manually published.
	ManualDealPublish bool

	// When a deal is ready to publish, the amount of time to wait for more
	// deals to be ready to publish before publishing them all as a batch
	PublishMsgPeriod Duration
	// The maximum number of deals to include in a single PublishStorageDeals
	// message
	MaxDealsPerPublishMsg uint64
	// The maximum collateral that the provider will put up against a deal,
	// as a multiplier of the minimum collateral bound
	// The maximum fee to pay when sending the PublishStorageDeals message
	MaxPublishDealsFee types.FIL
}
