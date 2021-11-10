package api

import (
	"context"
	"time"
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

	MarketDummyDeal(context.Context) (*ProviderDealRejectionInfo, error) //perm:admin
}

// ProviderDealRejectionInfo is the information sent by the Storage Provider to the Client when it rejects a valid deal.
type ProviderDealRejectionInfo struct {
	Reason  string
	Backoff time.Duration
}
