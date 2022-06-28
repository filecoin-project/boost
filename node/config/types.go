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
	// The port that the graphql server listens on
	Port uint64
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
	// see https://docs.filecoin.io/mine/lotus/miner-configuration/#using-filters-for-fine-grained-storage-and-retrieval-deal-acceptance for more details
	Filter string
	// A command used for fine-grained evaluation of retrieval deals
	// see https://docs.filecoin.io/mine/lotus/miner-configuration/#using-filters-for-fine-grained-storage-and-retrieval-deal-acceptance for more details
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
	// When a deal is ready to publish, the amount of time to wait for more
	// deals to be ready to publish before publishing them all as a batch
	PublishMsgPeriod Duration
	// The maximum number of deals to include in a single PublishStorageDeals
	// message
	PublishMsgMaxDealsPerMsg uint64
	// The maximum network fees to pay when sending the PublishStorageDeals message
	PublishMsgMaxFee types.FIL
	// The maximum collateral that the provider will put up against a deal,
	// as a multiplier of the minimum collateral bound
	MaxProviderCollateralMultiplier uint64
	// The maximum allowed disk usage size in bytes of staging deals not yet
	// passed to the sealing node by the markets service. 0 is unlimited.
	MaxStagingDealsBytes int64
	// The maximum number of parallel online data transfers for storage deals
	SimultaneousTransfersForStorage uint64
	// The maximum number of parallel online data transfers for retrieval deals
	SimultaneousTransfersForRetrieval uint64
	// Minimum start epoch buffer to give time for sealing of sector with deal.
	StartEpochSealingBuffer uint64
	// The amount of time to keep deal proposal logs for before cleaning them up.
	DealProposalLogDuration Duration

	// A command used for fine-grained evaluation of storage deals
	// see https://docs.filecoin.io/mine/lotus/miner-configuration/#using-filters-for-fine-grained-storage-and-retrieval-deal-acceptance for more details
	Filter string
	// A command used for fine-grained evaluation of retrieval deals
	// see https://docs.filecoin.io/mine/lotus/miner-configuration/#using-filters-for-fine-grained-storage-and-retrieval-deal-acceptance for more details
	RetrievalFilter string

	RetrievalPricing *lotus_config.RetrievalPricing

	// The maximum amount of time a transfer can take before it fails
	MaxTransferDuration Duration
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
}
