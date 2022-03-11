package impl

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/filecoin-project/boost/indexprovider"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-jsonrpc/auth"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/markets/storageadapter"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/storage/sectorblocks"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/host"
	"go.uber.org/fx"
)

type BoostAPI struct {
	fx.In

	api.Common
	api.Net

	Full lapi.FullNode
	//LocalStore  *stores.Local
	//RemoteStore *stores.Remote

	Host     host.Host
	DAGStore *dagstore.DAGStore

	// Boost
	StorageProvider *storagemarket.Provider
	IndexProvider   *indexprovider.Wrapper

	// Lotus Markets
	SectorBlocks *sectorblocks.SectorBlocks
	PieceStore   lotus_dtypes.ProviderPieceStore
	DataTransfer lotus_dtypes.ProviderDataTransfer

	RetrievalProvider retrievalmarket.RetrievalProvider
	SectorAccessor    retrievalmarket.SectorAccessor
	DealPublisher     *storageadapter.DealPublisher

	// Sealing Pipeline API
	Sps sealingpipeline.API
	// TODO: Figure out how to start graphql server without it needing
	// to be a dependency of another fx object
	GraphqlServer *gql.Server

	DS lotus_dtypes.MetadataDS

	ConsiderOnlineStorageDealsConfigFunc        lotus_dtypes.ConsiderOnlineStorageDealsConfigFunc        `optional:"true"`
	SetConsiderOnlineStorageDealsConfigFunc     lotus_dtypes.SetConsiderOnlineStorageDealsConfigFunc     `optional:"true"`
	ConsiderOnlineRetrievalDealsConfigFunc      lotus_dtypes.ConsiderOnlineRetrievalDealsConfigFunc      `optional:"true"`
	SetConsiderOnlineRetrievalDealsConfigFunc   lotus_dtypes.SetConsiderOnlineRetrievalDealsConfigFunc   `optional:"true"`
	StorageDealPieceCidBlocklistConfigFunc      lotus_dtypes.StorageDealPieceCidBlocklistConfigFunc      `optional:"true"`
	SetStorageDealPieceCidBlocklistConfigFunc   lotus_dtypes.SetStorageDealPieceCidBlocklistConfigFunc   `optional:"true"`
	ConsiderOfflineStorageDealsConfigFunc       lotus_dtypes.ConsiderOfflineStorageDealsConfigFunc       `optional:"true"`
	SetConsiderOfflineStorageDealsConfigFunc    lotus_dtypes.SetConsiderOfflineStorageDealsConfigFunc    `optional:"true"`
	ConsiderOfflineRetrievalDealsConfigFunc     lotus_dtypes.ConsiderOfflineRetrievalDealsConfigFunc     `optional:"true"`
	SetConsiderOfflineRetrievalDealsConfigFunc  lotus_dtypes.SetConsiderOfflineRetrievalDealsConfigFunc  `optional:"true"`
	ConsiderVerifiedStorageDealsConfigFunc      lotus_dtypes.ConsiderVerifiedStorageDealsConfigFunc      `optional:"true"`
	SetConsiderVerifiedStorageDealsConfigFunc   lotus_dtypes.SetConsiderVerifiedStorageDealsConfigFunc   `optional:"true"`
	ConsiderUnverifiedStorageDealsConfigFunc    lotus_dtypes.ConsiderUnverifiedStorageDealsConfigFunc    `optional:"true"`
	SetConsiderUnverifiedStorageDealsConfigFunc lotus_dtypes.SetConsiderUnverifiedStorageDealsConfigFunc `optional:"true"`
	SetSealingConfigFunc                        lotus_dtypes.SetSealingConfigFunc                        `optional:"true"`
	GetSealingConfigFunc                        lotus_dtypes.GetSealingConfigFunc                        `optional:"true"`
	GetExpectedSealDurationFunc                 lotus_dtypes.GetExpectedSealDurationFunc                 `optional:"true"`
	SetExpectedSealDurationFunc                 lotus_dtypes.SetExpectedSealDurationFunc                 `optional:"true"`
}

var _ api.Boost = &BoostAPI{}

func (sm *BoostAPI) ServeRemote(perm bool) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if perm {
			if !auth.HasPerm(r.Context(), nil, api.PermAdmin) {
				w.WriteHeader(401)
				_ = json.NewEncoder(w).Encode(struct{ Error string }{"unauthorized: missing write permission"})
				return
			}
		}

		//sm.StorageMgr.ServeHTTP(w, r)
	}
}

func (sm *BoostAPI) MarketDummyDeal(ctx context.Context, params types.DealParams) (*api.ProviderDealRejectionInfo, error) {
	info, _, err := sm.StorageProvider.ExecuteDeal(&params, "dummy")
	return info, err
}

func (sm *BoostAPI) Deal(ctx context.Context, dealUuid uuid.UUID) (*types.ProviderDealState, error) {
	return sm.StorageProvider.Deal(ctx, dealUuid)
}

func (sm *BoostAPI) IndexerAnnounceAllDeals(ctx context.Context) error {
	return sm.IndexProvider.IndexerAnnounceAllDeals(ctx)
}
