package api

import (
	"context"

	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/google/uuid"
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

type Market interface {
	// MethodGroup: Market

	MarketDummyDeal(context.Context, smtypes.DealParams) (*ProviderDealRejectionInfo, error)                                  //perm:admin
	Deal(ctx context.Context, dealUuid uuid.UUID) (*smtypes.ProviderDealState, error)                                         //perm:admin
	IndexerAnnounceAllDeals(ctx context.Context) error                                                                        //perm:admin
	MakeOfflineDealWithData(dealUuid uuid.UUID, filePath string) (*ProviderDealRejectionInfo, error)                          //perm:admin
	DagstoreInitializeShard(ctx context.Context, key string) error                                                            //perm:admin
	DagstoreInitializeAll(ctx context.Context, params DagstoreInitializeAllParams) (<-chan DagstoreInitializeAllEvent, error) //perm:admin
}

// ProviderDealRejectionInfo is the information sent by the Storage Provider
// to the Client when it accepts or rejects a deal.
type ProviderDealRejectionInfo struct {
	Accepted bool
	Reason   string // The rejection reason, if the deal is rejected
}
