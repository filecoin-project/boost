package config

import (
	"encoding"
	"os"
	"strconv"
	"time"

	"github.com/filecoin-project/lotus/chain/types"
	lotus_config "github.com/filecoin-project/lotus/node/config"
	"github.com/ipfs/go-cid"
)

const (
	// RetrievalPricingDefault configures the node to use the default retrieval pricing policy.
	RetrievalPricingDefaultMode = "default"
	// RetrievalPricingExternal configures the node to use the external retrieval pricing script
	// configured by the user.
	RetrievalPricingExternalMode = "external"
)

// MaxTraversalLinks configures the maximum number of links to traverse in a DAG while calculating
// CommP and traversing a DAG with graphsync; invokes a budget on DAG depth and density.
var MaxTraversalLinks uint64 = 32 * (1 << 20)

func init() {
	if envMaxTraversal, err := strconv.ParseUint(os.Getenv("LOTUS_MAX_TRAVERSAL_LINKS"), 10, 64); err == nil {
		MaxTraversalLinks = envMaxTraversal
	}
}

func defCommon() Common {
	return Common{
		API: lotus_config.API{
			ListenAddress: "/ip4/127.0.0.1/tcp/1288/http",
			Timeout:       lotus_config.Duration(30 * time.Second),
		},
		Libp2p: lotus_config.Libp2p{
			ListenAddresses: []string{
				"/ip4/0.0.0.0/tcp/0",
				"/ip6/::/tcp/0",
			},
			AnnounceAddresses:   []string{},
			NoAnnounceAddresses: []string{},

			ConnMgrLow:   150,
			ConnMgrHigh:  180,
			ConnMgrGrace: lotus_config.Duration(20 * time.Second),
		},
	}

}

var DefaultDefaultMaxFee = types.MustParseFIL("0.07")
var DefaultSimultaneousTransfers = uint64(20)

func DefaultBoost() *Boost {
	cfg := &Boost{
		Common: defCommon(),

		Storage: StorageConfig{
			ParallelFetchLimit:            10,
			StorageListRefreshDuration:    Duration(time.Hour * 1),
			RedeclareOnStorageListRefresh: true,
		},

		Graphql: GraphqlConfig{
			ListenAddress: "127.0.0.1",
			Port:          8080,
		},

		Monitoring: MonitoringConfig{
			MpoolAlertEpochs: 30,
		},

		Tracing: TracingConfig{
			Enabled:     false,
			Endpoint:    "",
			ServiceName: "boostd",
		},

		LocalIndexDirectory: LocalIndexDirectoryConfig{
			Yugabyte: LocalIndexDirectoryYugabyteConfig{
				Enabled: false,
			},
			Couchbase: LocalIndexDirectoryCouchbaseConfig{
				ConnectString:           "",
				Username:                "",
				Password:                "",
				PieceMetadataBucket:     LocalIndexDirectoryCouchbaseBucketConfig{},
				MultihashToPiecesBucket: LocalIndexDirectoryCouchbaseBucketConfig{},
				PieceOffsetsBucket:      LocalIndexDirectoryCouchbaseBucketConfig{},
			},
			ParallelAddIndexLimit: 4,
			EmbeddedServicePort:   8042,
			ServiceApiInfo:        "",
			ServiceRPCTimeout:     Duration(15 * time.Minute),
		},

		ContractDeals: ContractDealsConfig{
			Enabled:            false,
			AllowlistContracts: []string{},
			From:               "0x0000000000000000000000000000000000000000",
		},

		Dealmaking: DealmakingConfig{
			ConsiderOnlineStorageDeals:     true,
			ConsiderOfflineStorageDeals:    true,
			ConsiderOnlineRetrievalDeals:   true,
			ConsiderOfflineRetrievalDeals:  true,
			ConsiderVerifiedStorageDeals:   true,
			ConsiderUnverifiedStorageDeals: true,
			PieceCidBlocklist:              []cid.Cid{},
			// TODO: It'd be nice to set this based on sector size
			MaxDealStartDelay:               Duration(time.Hour * 24 * 14),
			ExpectedSealDuration:            Duration(time.Hour * 24),
			MaxProviderCollateralMultiplier: 2,

			StartEpochSealingBuffer: 480, // 480 epochs buffer == 4 hours from adding deal to sector to sector being sealed

			DealProposalLogDuration: Duration(time.Hour * 24),
			RetrievalLogDuration:    Duration(time.Hour * 24),
			StalledRetrievalTimeout: Duration(time.Second * 30),

			RetrievalPricing: &lotus_config.RetrievalPricing{
				Strategy: RetrievalPricingDefaultMode,
				Default: &lotus_config.RetrievalPricingDefault{
					VerifiedDealsFreeTransfer: true,
				},
				External: &lotus_config.RetrievalPricingExternal{
					Path: "",
				},
			},

			// This should no longer be needed once LID is live
			BlockstoreCacheMaxShards: 20, // Match default simultaneous retrievals
			BlockstoreCacheExpiry:    Duration(30 * time.Second),

			IsUnsealedCacheExpiry: Duration(5 * time.Minute),

			MaxTransferDuration: Duration(24 * 3600 * time.Second),

			RemoteCommp:             false,
			MaxConcurrentLocalCommp: 1,

			HttpTransferMaxConcurrentDownloads: 20,
			HttpTransferStallTimeout:           Duration(5 * time.Minute),
			HttpTransferStallCheckPeriod:       Duration(30 * time.Second),
			DealLogDurationDays:                30,
			SealingPipelineCacheTimeout:        Duration(30 * time.Second),
			FundsTaggingEnabled:                true,
			EnableLegacyStorageDeals:           false,
		},

		LotusDealmaking: lotus_config.DealmakingConfig{
			ConsiderOnlineStorageDeals:     true,
			ConsiderOfflineStorageDeals:    true,
			ConsiderOnlineRetrievalDeals:   true,
			ConsiderOfflineRetrievalDeals:  true,
			ConsiderVerifiedStorageDeals:   true,
			ConsiderUnverifiedStorageDeals: true,
			PieceCidBlocklist:              []cid.Cid{},
			// TODO: It'd be nice to set this based on sector size
			MaxDealStartDelay:               lotus_config.Duration(time.Hour * 24 * 14),
			ExpectedSealDuration:            lotus_config.Duration(time.Hour * 24),
			PublishMsgPeriod:                lotus_config.Duration(time.Hour),
			MaxDealsPerPublishMsg:           8,
			MaxProviderCollateralMultiplier: 2,

			SimultaneousTransfersForStorage:          DefaultSimultaneousTransfers,
			SimultaneousTransfersForStoragePerClient: 0,
			SimultaneousTransfersForRetrieval:        DefaultSimultaneousTransfers,

			StartEpochSealingBuffer: 480, // 480 epochs buffer == 4 hours from adding deal to sector to sector being sealed

			RetrievalPricing: &lotus_config.RetrievalPricing{
				Strategy: RetrievalPricingDefaultMode,
				Default: &lotus_config.RetrievalPricingDefault{
					VerifiedDealsFreeTransfer: true,
				},
				External: &lotus_config.RetrievalPricingExternal{
					Path: "",
				},
			},
		},

		LotusFees: FeeConfig{
			MaxPublishDealsFee:     types.MustParseFIL("0.05"),
			MaxMarketBalanceAddFee: types.MustParseFIL("0.007"),
		},

		DAGStore: lotus_config.DAGStoreConfig{
			MaxConcurrentIndex:         5,
			MaxConcurrencyStorageCalls: 100,
			GCInterval:                 lotus_config.Duration(1 * time.Minute),
		},
		IndexProvider: IndexProviderConfig{
			Enable:               true,
			EntriesCacheCapacity: 1024,
			EntriesChunkSize:     16384,
			// The default empty TopicName means it is inferred from network name, in the following
			// format: "/indexer/ingest/<network-name>"
			TopicName:         "",
			PurgeCacheOnStart: false,

			Announce: IndexProviderAnnounceConfig{
				AnnounceOverHttp:   false,
				DirectAnnounceURLs: []string{"https://cid.contact/ingest/announce"},
			},

			HttpPublisher: IndexProviderHttpPublisherConfig{
				Enabled:        false,
				PublicHostname: "",
				Port:           3104,
			},
		},
	}
	return cfg
}

var _ encoding.TextMarshaler = (*Duration)(nil)
var _ encoding.TextUnmarshaler = (*Duration)(nil)

// Duration is a wrapper type for time.Duration
// for decoding and encoding from/to TOML
type Duration time.Duration

// UnmarshalText implements interface for TOML decoding
func (dur *Duration) UnmarshalText(text []byte) error {
	d, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*dur = Duration(d)
	return err
}

func (dur Duration) MarshalText() ([]byte, error) {
	d := time.Duration(dur)
	return []byte(d.String()), nil
}
