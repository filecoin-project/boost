package api

import (
	"context"

	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

//                       MODIFYING THE API INTERFACE
//
// When adding / changing methods in this file:
// * Do the change here
// * Adjust implementation in `node/impl/`
// * Run `make gen` - this will:
//  * Generate proxy structs
//  * Generate mocks
//  * Generate markdown docs
//  * Generate openrpc blobs

type Boost interface {
	Common
	Net

	// MethodGroup: Boost
	BoostIndexerAnnounceAllDeals(ctx context.Context) error                                                                        //perm:admin
	BoostOfflineDealWithData(dealUuid uuid.UUID, filePath string) (*ProviderDealRejectionInfo, error)                              //perm:admin
	BoostDeal(ctx context.Context, dealUuid uuid.UUID) (*smtypes.ProviderDealState, error)                                         //perm:admin
	BoostDummyDeal(context.Context, smtypes.DealParams) (*ProviderDealRejectionInfo, error)                                        //perm:admin
	BoostDagstoreInitializeShard(ctx context.Context, key string) error                                                            //perm:admin
	BoostDagstoreInitializeAll(ctx context.Context, params DagstoreInitializeAllParams) (<-chan DagstoreInitializeAllEvent, error) //perm:admin
	BoostDagstoreGC(ctx context.Context) ([]DagstoreShardResult, error)                                                            //perm:admin

	BoostDagstoreListShards(ctx context.Context) ([]DagstoreShardInfo, error) //perm:read

	// MethodGroup: LegacyMarket
	MarketListRetrievalDeals(ctx context.Context) ([]retrievalmarket.ProviderDealState, error)                                                                                           //perm:read
	MarketSetRetrievalAsk(ctx context.Context, rask *retrievalmarket.Ask) error                                                                                                          //perm:admin
	MarketGetRetrievalAsk(ctx context.Context) (*retrievalmarket.Ask, error)                                                                                                             //perm:read
	MarketSetAsk(ctx context.Context, price types.BigInt, verifiedPrice types.BigInt, duration abi.ChainEpoch, minPieceSize abi.PaddedPieceSize, maxPieceSize abi.PaddedPieceSize) error //perm:admin
	MarketGetAsk(ctx context.Context) (*storagemarket.SignedStorageAsk, error)                                                                                                           //perm:read
	MarketListDataTransfers(ctx context.Context) ([]lapi.DataTransferChannel, error)                                                                                                     //perm:write
	MarketDataTransferUpdates(ctx context.Context) (<-chan lapi.DataTransferChannel, error)                                                                                              //perm:write
	MarketRestartDataTransfer(ctx context.Context, transferID datatransfer.TransferID, otherPeer peer.ID, isInitiator bool) error                                                        //perm:write
	MarketImportDealData(ctx context.Context, propcid cid.Cid, path string) error                                                                                                        //perm:write

	// MethodGroup: Actor
	ActorSectorSize(context.Context, address.Address) (abi.SectorSize, error) //perm:read

	// MethodGroup: Deals
	DealsConsiderOnlineStorageDeals(context.Context) (bool, error)      //perm:admin
	DealsSetConsiderOnlineStorageDeals(context.Context, bool) error     //perm:admin
	DealsConsiderOnlineRetrievalDeals(context.Context) (bool, error)    //perm:admin
	DealsSetConsiderOnlineRetrievalDeals(context.Context, bool) error   //perm:admin
	DealsPieceCidBlocklist(context.Context) ([]cid.Cid, error)          //perm:admin
	DealsSetPieceCidBlocklist(context.Context, []cid.Cid) error         //perm:admin
	DealsConsiderOfflineStorageDeals(context.Context) (bool, error)     //perm:admin
	DealsSetConsiderOfflineStorageDeals(context.Context, bool) error    //perm:admin
	DealsConsiderOfflineRetrievalDeals(context.Context) (bool, error)   //perm:admin
	DealsSetConsiderOfflineRetrievalDeals(context.Context, bool) error  //perm:admin
	DealsConsiderVerifiedStorageDeals(context.Context) (bool, error)    //perm:admin
	DealsSetConsiderVerifiedStorageDeals(context.Context, bool) error   //perm:admin
	DealsConsiderUnverifiedStorageDeals(context.Context) (bool, error)  //perm:admin
	DealsSetConsiderUnverifiedStorageDeals(context.Context, bool) error //perm:admin
}

// DagstoreShardInfo is the serialized form of dagstore.DagstoreShardInfo that
// we expose through JSON-RPC to avoid clients having to depend on the
// dagstore lib.
type DagstoreShardInfo struct {
	Key   string
	State string
	Error string
}

// DagstoreShardResult enumerates results per shard.
type DagstoreShardResult struct {
	Key     string
	Success bool
	Error   string
}

type DagstoreInitializeAllParams struct {
	MaxConcurrency int
	IncludeSealed  bool
}

// DagstoreInitializeAllEvent represents an initialization event.
type DagstoreInitializeAllEvent struct {
	Key     string
	Event   string // "start", "end"
	Success bool
	Error   string
	Total   int
	Current int
}
