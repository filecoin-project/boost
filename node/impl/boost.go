package impl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/filecoin-project/boost/lib/legacy"
	"github.com/filecoin-project/boost/node/impl/backupmgr"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/tracing"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/boost/markets/storageadapter"
	retmarket "github.com/filecoin-project/boost/retrievalmarket/server"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/types"

	"github.com/filecoin-project/go-jsonrpc/auth"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/gateway"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/fx"
)

var log = logging.Logger("boost-api")

type BoostAPI struct {
	fx.In

	api.Common
	api.Net

	Full  lapi.FullNode
	SubCh *gateway.EthSubHandler

	Host host.Host

	// Boost
	StorageProvider *storagemarket.Provider
	IndexProvider   *indexprovider.Wrapper

	// Legacy Markets
	LegacyDealManager legacy.LegacyDealManager

	// Boost - Direct Data onboarding
	DirectDealsProvider *storagemarket.DirectDealsProvider

	// Lotus Markets
	DealPublisher *storageadapter.DealPublisher

	// Graphsync Unpaid Retrieval
	GraphsyncUnpaidRetrieval *retmarket.GraphsyncUnpaidRetrieval

	// Sealing Pipeline API
	Sps sealingpipeline.API

	// Piece Directory
	Pd *piecedirectory.PieceDirectory

	// GraphSQL server
	GraphqlServer *gql.Server

	// Tracing
	Tracing *tracing.Tracing

	DS lotus_dtypes.MetadataDS

	Bkp *backupmgr.BackupMgr

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

func (sm *BoostAPI) BoostDummyDeal(ctx context.Context, params types.DealParams) (*api.ProviderDealRejectionInfo, error) {
	return sm.StorageProvider.ExecuteDeal(ctx, &params, "dummy")
}

func (sm *BoostAPI) BoostDeal(ctx context.Context, dealUuid uuid.UUID) (*types.ProviderDealState, error) {
	// TODO: Use a middleware function that wraps the entire api implementation for all RPC calls
	// Testing for now until a middleware function is created
	ctx, span := tracing.Tracer.Start(ctx, "BoostAPI.BoostDeal")
	defer span.End()
	span.SetAttributes(attribute.String("dealUuid", dealUuid.String())) // Example of adding additional attributes

	return sm.StorageProvider.Deal(ctx, dealUuid)
}

func (sm *BoostAPI) BoostDealBySignedProposalCid(ctx context.Context, proposalCid cid.Cid) (*types.ProviderDealState, error) {
	return sm.StorageProvider.DealBySignedProposalCid(ctx, proposalCid)
}

func (sm *BoostAPI) BoostIndexerAnnounceAllDeals(ctx context.Context) error {
	return sm.IndexProvider.IndexerAnnounceAllDeals(ctx)
}

// BoostIndexerListMultihashes calls the index provider multihash lister for a given proposal cid
func (sm *BoostAPI) BoostIndexerListMultihashes(ctx context.Context, proposalCid cid.Cid) ([]multihash.Multihash, error) {
	it, err := sm.IndexProvider.MultihashLister(ctx, "", proposalCid.Bytes())
	if err != nil {
		return nil, err
	}

	var mhs []multihash.Multihash
	mh, err := it.Next()
	for {
		if err != nil {
			if errors.Is(err, io.EOF) {
				return mhs, nil
			}
			return nil, err
		}
		mhs = append(mhs, mh)

		mh, err = it.Next()
	}
}

func (sm *BoostAPI) BoostIndexerAnnounceLatest(ctx context.Context) (cid.Cid, error) {
	return sm.IndexProvider.IndexerAnnounceLatest(ctx)
}

func (sm *BoostAPI) BoostIndexerAnnounceLatestHttp(ctx context.Context, announceUrls []string) (cid.Cid, error) {
	return sm.IndexProvider.IndexerAnnounceLatestHttp(ctx, announceUrls)
}

func (sm *BoostAPI) BoostIndexerAnnounceDealRemoved(ctx context.Context, propCid cid.Cid) (cid.Cid, error) {
	return sm.IndexProvider.AnnounceBoostDealRemoved(ctx, propCid)
}

func (sm *BoostAPI) BoostLegacyDealByProposalCid(ctx context.Context, propCid cid.Cid) (gfm_storagemarket.MinerDeal, error) {
	return sm.LegacyStorageProvider.GetLocalDeal(propCid)
}

func (sm *BoostAPI) BoostOfflineDealWithData(ctx context.Context, dealUuid uuid.UUID, filePath string, delAfterImport bool) (*api.ProviderDealRejectionInfo, error) {
	res, err := sm.StorageProvider.ImportOfflineDealData(ctx, dealUuid, filePath, delAfterImport)
	return res, err
}

func (sm *BoostAPI) BoostDirectDeal(ctx context.Context, params types.DirectDealParams) (*api.ProviderDealRejectionInfo, error) {
	return nil, fmt.Errorf("not implemented")
	// return sm.DirectDealsProvider.Import(ctx, params)
}

func (sm *BoostAPI) BoostMakeDeal(ctx context.Context, params types.DealParams) (*api.ProviderDealRejectionInfo, error) {
	log.Infow("received json-rpc deal proposal", "id", params.DealUUID)
	return sm.StorageProvider.ExecuteDeal(ctx, &params, "json-rpc-deal")
}

func (sm *BoostAPI) BlockstoreGet(ctx context.Context, c cid.Cid) ([]byte, error) {
	return sm.Pd.BlockstoreGet(ctx, c)
}

func (sm *BoostAPI) BlockstoreHas(ctx context.Context, c cid.Cid) (bool, error) {
	return sm.Pd.BlockstoreHas(ctx, c)
}

func (sm *BoostAPI) BlockstoreGetSize(ctx context.Context, c cid.Cid) (int, error) {
	return sm.Pd.BlockstoreGetSize(ctx, c)
}

func (sm *BoostAPI) PdBuildIndexForPieceCid(ctx context.Context, piececid cid.Cid) error {
	ctx, span := tracing.Tracer.Start(ctx, "Boost.PdBuildIndexForPieceCid")
	span.SetAttributes(attribute.String("piececid", piececid.String()))
	defer span.End()

	return sm.Pd.BuildIndexForPiece(ctx, piececid)
}

func (sm *BoostAPI) OnlineBackup(ctx context.Context, dstDir string) error {
	return sm.Bkp.Backup(ctx, dstDir)
}
