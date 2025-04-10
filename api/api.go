package api

import (
	"context"

	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
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
	BoostIndexerRemoveAll(ctx context.Context) ([]cid.Cid, error)                                                                               //perm:admin
	BoostIndexerAnnounceAllDeals(ctx context.Context) error                                                                                     //perm:admin
	BoostIndexerListMultihashes(ctx context.Context, contextID []byte) ([]multihash.Multihash, error)                                           //perm:admin
	BoostIndexerAnnounceLatest(ctx context.Context) (cid.Cid, error)                                                                            //perm:admin
	BoostIndexerAnnounceLatestHttp(ctx context.Context, urls []string) (cid.Cid, error)                                                         //perm:admin
	BoostOfflineDealWithData(ctx context.Context, dealUuid uuid.UUID, filePath string, delAfterImport bool) (*ProviderDealRejectionInfo, error) //perm:admin
	BoostDeal(ctx context.Context, dealUuid uuid.UUID) (*smtypes.ProviderDealState, error)                                                      //perm:admin
	BoostDealBySignedProposalCid(ctx context.Context, proposalCid cid.Cid) (*smtypes.ProviderDealState, error)                                  //perm:admin
	BoostDummyDeal(context.Context, smtypes.DealParams) (*ProviderDealRejectionInfo, error)                                                     //perm:admin
	BoostIndexerAnnounceDealRemoved(ctx context.Context, propCid cid.Cid) (cid.Cid, error)                                                      //perm:admin
	BoostLegacyDealByProposalCid(ctx context.Context, propCid cid.Cid) (legacytypes.MinerDeal, error)                                           //perm:admin
	BoostIndexerAnnounceDeal(ctx context.Context, deal *smtypes.ProviderDealState) (cid.Cid, error)                                             //perm:admin
	BoostIndexerAnnounceLegacyDeal(ctx context.Context, proposalCid cid.Cid) (cid.Cid, error)                                                   //perm:admin
	BoostDirectDeal(ctx context.Context, params smtypes.DirectDealParams) (*ProviderDealRejectionInfo, error)                                   //perm:admin
	MarketGetAsk(ctx context.Context) (*legacytypes.SignedStorageAsk, error)                                                                    //perm:read

	// MethodGroup: Blockstore
	BlockstoreGet(ctx context.Context, c cid.Cid) ([]byte, error)  //perm:read
	BlockstoreHas(ctx context.Context, c cid.Cid) (bool, error)    //perm:read
	BlockstoreGetSize(ctx context.Context, c cid.Cid) (int, error) //perm:read

	// MethodGroup: PieceDirectory
	PdBuildIndexForPieceCid(ctx context.Context, piececid cid.Cid) error             //perm:admin
	PdRemoveDealForPiece(ctx context.Context, piececid cid.Cid, dealID string) error //perm:admin
	PdCleanup(ctx context.Context) error                                             //perm:admin

	// MethodGroup: Misc
	OnlineBackup(context.Context, string) error //perm:admin
}
