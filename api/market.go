package api

import (
	"context"

	"github.com/google/uuid"

	smtypes "github.com/filecoin-project/boost/storagemarket/types"
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

	MarketDummyDeal(context.Context, smtypes.DealParams) (*ProviderDealRejectionInfo, error) //perm:admin
	Deal(ctx context.Context, dealUuid uuid.UUID) (*smtypes.ProviderDealState, error)        //perm:admin
}

// ProviderDealRejectionInfo is the information sent by the Storage Provider to the Client when it rejects a valid deal.
type ProviderDealRejectionInfo struct {
	Reason string
}
