package api

import (
	"context"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

type LegacyMarket interface {
	// MethodGroup: LegacyMarket

	//MarketImportDealData(ctx context.Context, propcid cid.Cid, path string) error                                                                                                        //perm:write
	//MarketListDeals(ctx context.Context) ([]MarketDeal, error)                                                                                                                           //perm:read
	MarketListRetrievalDeals(ctx context.Context) ([]retrievalmarket.ProviderDealState, error) //perm:read
	//MarketGetDealUpdates(ctx context.Context) (<-chan storagemarket.MinerDeal, error)                                                                                                    //perm:read
	//MarketListIncompleteDeals(ctx context.Context) ([]storagemarket.MinerDeal, error)                                                                                                    //perm:read
	//MarketSetAsk(ctx context.Context, price types.BigInt, verifiedPrice types.BigInt, duration abi.ChainEpoch, minPieceSize abi.PaddedPieceSize, maxPieceSize abi.PaddedPieceSize) error //perm:admin
	//MarketGetAsk(ctx context.Context) (*storagemarket.SignedStorageAsk, error)                                                                                                           //perm:read
	MarketSetRetrievalAsk(ctx context.Context, rask *retrievalmarket.Ask) error //perm:admin
	MarketGetRetrievalAsk(ctx context.Context) (*retrievalmarket.Ask, error)    //perm:read

	MarketListDataTransfers(ctx context.Context) ([]lapi.DataTransferChannel, error) //perm:write

	MarketDataTransferUpdates(ctx context.Context) (<-chan lapi.DataTransferChannel, error) //perm:write
	//// MarketDataTransferDiagnostics generates debugging information about current data transfers over graphsync
	//MarketDataTransferDiagnostics(ctx context.Context, p peer.ID) (*TransferDiagnostics, error) //perm:write

	// MarketRestartDataTransfer attempts to restart a data transfer with the given transfer ID and other peer
	MarketRestartDataTransfer(ctx context.Context, transferID datatransfer.TransferID, otherPeer peer.ID, isInitiator bool) error //perm:write
	//// MarketCancelDataTransfer cancels a data transfer with the given transfer ID and other peer
	//MarketCancelDataTransfer(ctx context.Context, transferID datatransfer.TransferID, otherPeer peer.ID, isInitiator bool) error //perm:write
	//MarketPendingDeals(ctx context.Context) (PendingDealInfo, error)                                                             //perm:write
	//MarketPublishPendingDeals(ctx context.Context) error                                                                         //perm:admin
	//MarketRetryPublishDeal(ctx context.Context, propcid cid.Cid) error                                                           //perm:admin

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
