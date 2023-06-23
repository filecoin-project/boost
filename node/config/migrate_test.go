package config

import (
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const mockV0Config = `
SealerApiInfo = "api-endpoint"

[API]
  ListenAddress = "/ip4/127.0.0.1/tcp/1234/http"
`

const mockV2Config = `
ConfigVersion = 2
MyNewKey = "Hello"
`

const testConfig = `
ConfigVersion = 4
SealerApiInfo = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBbGxvdyI6WyJyZWFkIiwid3JpdGUiLCJzaWduIiwiYWRtaW4iXX0.0LyVxqOde8UjLTcHPEo3VEBILtPQqCDNEHcoCbTRQ_Y:/ip4/127.0.0.1/tcp/2345/http"
SectorIndexApiInfo = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBbGxvdyI6WyJyZWFkIiwid3JpdGUiLCJzaWduIiwiYWRtaW4iXX0.0LyVxqOde8UjLTcHPEo3VEBILtPQqCDNEHcoCbTRQ_Y:/ip4/127.0.0.1/tcp/2345/http"

[API]
  ListenAddress = "/ip4/127.0.0.1/tcp/1288/http"
  RemoteListenAddress = ""
  Timeout = "30s"

[Backup]
  DisableMetadataLog = false

[Libp2p]
  ListenAddresses = ["/ip4/0.0.0.1/tcp/50000", "/ip6/::/tcp/0"]
  AnnounceAddresses = []
  NoAnnounceAddresses = []
  DisableNatPortMap = false
  ConnMgrLow = 150
  ConnMgrHigh = 180
  ConnMgrGrace = "20s"

[Pubsub]
  Bootstrapper = false
  RemoteTracer = ""
  JsonTracer = ""
  ElasticSearchTracer = ""
  ElasticSearchIndex = ""
  TracerSourceAuth = ""

[Storage]
  ParallelFetchLimit = 10
  StorageListRefreshDuration = "1h0m0s"
  RedeclareOnStorageListRefresh = true

[Dealmaking]
  ConsiderOnlineStorageDeals = true
  ConsiderOfflineStorageDeals = true
  ConsiderOnlineRetrievalDeals = true
  ConsiderOfflineRetrievalDeals = true
  ConsiderVerifiedStorageDeals = true
  ConsiderUnverifiedStorageDeals = true
  PieceCidBlocklist = []
  ExpectedSealDuration = "24h0m0s"
  MaxDealStartDelay = "336h0m0s"
  MaxProviderCollateralMultiplier = 2
  MaxStagingDealsBytes = 2000000000
  MaxStagingDealsPercentPerHost = 0
  StartEpochSealingBuffer = 480
  DealProposalLogDuration = "24h0m0s"
  RetrievalLogDuration = "24h0m0s"
  StalledRetrievalTimeout = "30s"
  Filter = ""
  RetrievalFilter = ""
  BlockstoreCacheMaxShards = 20
  BlockstoreCacheExpiry = "30s"
  IsUnsealedCacheExpiry = "5m0s"
  MaxTransferDuration = "24h0m0s"
  RemoteCommp = false
  MaxConcurrentLocalCommp = 1
  HTTPRetrievalMultiaddr = ""
  HttpTransferMaxConcurrentDownloads = 20
  HttpTransferStallCheckPeriod = "30s"
  HttpTransferStallTimeout = "5m0s"
  BitswapPeerID = ""
  BitswapPrivKeyFile = ""
  DealLogDurationDays = 30
  SealingPipelineCacheTimeout = "30s"
  FundsTaggingEnabled = true
  [Dealmaking.RetrievalPricing]
    Strategy = "default"
    [Dealmaking.RetrievalPricing.Default]
      VerifiedDealsFreeTransfer = false
    [Dealmaking.RetrievalPricing.External]
      Path = ""

[Wallets]
  Miner = "t01000"
  PublishStorageDeals = "t3rjh3byfhhrlmdl6ofou544rmwuazp4bbqzuleahtn66ejnh73bsaonwyg54qrokvnbv5bmg37dfd5vu2bx4q"
  DealCollateral = "t3qa5i54vprhl7gmdfgriubvjc5kdffx7cvbqvumopgvsy2osre5dcjdjhz5coruxp4co3chj3gbtxin7vtoia"
  PledgeCollateral = ""

[Graphql]
  ListenAddress = "0.0.0.0"
  Port = 8080

[Tracing]
  Enabled = false
  ServiceName = "boostd"
  Endpoint = ""

[ContractDeals]
  Enabled = false
  AllowlistContracts = []
  From = "0x0000000000000000000000000000000000000000"

[LotusDealmaking]
  ConsiderOnlineStorageDeals = true
  ConsiderOfflineStorageDeals = true
  ConsiderOnlineRetrievalDeals = true
  ConsiderOfflineRetrievalDeals = true
  ConsiderVerifiedStorageDeals = true
  ConsiderUnverifiedStorageDeals = true
  PieceCidBlocklist = []
  ExpectedSealDuration = "24h0m0s"
  MaxDealStartDelay = "336h0m0s"
  PublishMsgPeriod = "1h0m0s"
  MaxDealsPerPublishMsg = 8
  MaxProviderCollateralMultiplier = 2
  MaxStagingDealsBytes = 0
  SimultaneousTransfersForStorage = 20
  SimultaneousTransfersForStoragePerClient = 0
  SimultaneousTransfersForRetrieval = 20
  StartEpochSealingBuffer = 480
  Filter = ""
  RetrievalFilter = ""
  [LotusDealmaking.RetrievalPricing]
    Strategy = "default"
    [LotusDealmaking.RetrievalPricing.Default]
      VerifiedDealsFreeTransfer = true
    [LotusDealmaking.RetrievalPricing.External]
      Path = ""

[LotusFees]
  MaxPublishDealsFee = "0.05 FIL"
  MaxMarketBalanceAddFee = "0.007 FIL"

[DAGStore]
  RootDir = ""
  MaxConcurrentIndex = 5
  MaxConcurrentReadyFetches = 0
  MaxConcurrentUnseals = 0
  MaxConcurrencyStorageCalls = 100
  GCInterval = "1m0s"

[IndexProvider]
  Enable = true
  EntriesCacheCapacity = 1024
  EntriesChunkSize = 16384
  TopicName = ""
  PurgeCacheOnStart = false
  [IndexProvider.Announce]
    AnnounceOverHttp = false
    DirectAnnounceURLs = ["https://cid.contact/ingest/announce", "http://localhost:3000"]
  [IndexProvider.HttpPublisher]
    Enabled = false
    PublicHostname = ""
    Port = 3104
`

func TestMigrate(t *testing.T) {
	// Add a new mock migration so as to be able to test migrating up and down
	mockv1Tov2 := func(string) (string, error) {
		return mockV2Config, nil
	}
	migrations = []migrateUpFn{
		v0Tov1,
		mockv1Tov2,
	}

	repoDir := t.TempDir()
	err := os.WriteFile(path.Join(repoDir, "config.toml"), []byte(mockV0Config), 0644)
	require.NoError(t, err)

	// Migrate up to v1
	err = configMigrate(repoDir, 1)
	require.NoError(t, err)

	// The existing config file should have been copied to config/config.toml.0
	v0File := path.Join(repoDir, "config", "config.toml.0")
	bz, err := os.ReadFile(v0File)
	require.NoError(t, err)
	require.Equal(t, mockV0Config, string(bz))

	// The new config file should have been written to config/config.toml.1
	v1File := path.Join(repoDir, "config", "config.toml.1")
	bz, err = os.ReadFile(v1File)
	v1FileContents := string(bz)
	require.NoError(t, err)

	// There should be a symlink from config.toml to config/config.toml.1
	symLink := path.Join(repoDir, "config.toml")
	bz, err = os.ReadFile(symLink)
	require.NoError(t, err)
	require.Equal(t, v1FileContents, string(bz))

	// The config file should have the new version
	require.True(t, strings.Contains(v1FileContents, `ConfigVersion = 1`))

	// The v1 config file should retain key / values that were changed from the defaults
	// in the original config file
	require.True(t, strings.Contains(v1FileContents, `SealerApiInfo = "api-endpoint"`))
	require.True(t, strings.Contains(v1FileContents, `ListenAddress = "/ip4/127.0.0.1/tcp/1234/http"`))

	// The config file should have comments:
	// # The connect string for the sealing RPC API (lotus miner)
	// #
	// # type: string
	// # env var: LOTUS__SEALERAPIINFO
	// SealerApiInfo = "api-endpoint"
	require.True(t, strings.Contains(v1FileContents, `The connect string for the sealing RPC API`))

	// Migrate to v1 should have no effect (because config file is already at v1)
	err = configMigrate(repoDir, 1)
	require.NoError(t, err)

	// Migrate up to v2 should apply v2 migration function
	err = configMigrate(repoDir, 2)
	require.NoError(t, err)

	bz, err = os.ReadFile(symLink)
	require.NoError(t, err)
	require.Equal(t, mockV2Config, string(bz))

	// Migrate down to v1 should restore v1 config
	err = configMigrate(repoDir, 1)
	require.NoError(t, err)

	bz, err = os.ReadFile(symLink)
	require.NoError(t, err)
	require.Equal(t, v1FileContents, string(bz))

	// Migrate down to v0 should have no effect (because the v0 and v1 config files are compatible)
	err = configMigrate(repoDir, 0)
	require.NoError(t, err)

	bz, err = os.ReadFile(symLink)
	require.NoError(t, err)
	require.Equal(t, v1FileContents, string(bz))
}

func TestConfigDiff(t *testing.T) {
	repoDir := t.TempDir()
	err := os.WriteFile(path.Join(repoDir, "config.toml"), []byte(testConfig), 0644)
	require.NoError(t, err)

	cgf, err := FromFile(path.Join(repoDir, "config.toml"), DefaultBoost())
	require.NoError(t, err)

	s, err := ConfigUpdate(cgf, DefaultBoost(), false, true)
	require.NoError(t, err)

	require.False(t, strings.Contains(string(s), `The connect string for the sealing RPC API`))

	s, err = ConfigUpdate(cgf, DefaultBoost(), true, true)
	require.NoError(t, err)

	require.True(t, strings.Contains(string(s), `The connect string for the sealing RPC API`))
}
