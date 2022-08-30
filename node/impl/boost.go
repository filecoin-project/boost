package impl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"

	"github.com/multiformats/go-multihash"

	"github.com/filecoin-project/go-fil-markets/stores"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/boost/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	lotus_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-jsonrpc/auth"
	lapi "github.com/filecoin-project/lotus/api"
	mktsdagstore "github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/filecoin-project/lotus/markets/storageadapter"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/storage/sectorblocks"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"go.uber.org/fx"
)

var log = logging.Logger("boost-api")

type BoostAPI struct {
	fx.In

	api.Common
	api.Net

	Full lapi.FullNode
	//LocalStore  *stores.Local
	//RemoteStore *stores.Remote

	Host host.Host

	DAGStore        *dagstore.DAGStore
	DagStoreWrapper *mktsdagstore.Wrapper

	// Boost
	StorageProvider *storagemarket.Provider
	IndexProvider   *indexprovider.Wrapper

	// Legacy Lotus
	LegacyStorageProvider lotus_storagemarket.StorageProvider

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

func (sm *BoostAPI) BoostDummyDeal(ctx context.Context, params types.DealParams) (*api.ProviderDealRejectionInfo, error) {
	return sm.StorageProvider.ExecuteDeal(&params, "dummy")
}

func (sm *BoostAPI) BoostDeal(ctx context.Context, dealUuid uuid.UUID) (*types.ProviderDealState, error) {
	return sm.StorageProvider.Deal(ctx, dealUuid)
}

func (sm *BoostAPI) BoostDealBySignedProposalCid(ctx context.Context, proposalCid cid.Cid) (*types.ProviderDealState, error) {
	return sm.StorageProvider.DealBySignedProposalCid(ctx, proposalCid)
}

func (sm *BoostAPI) BoostIndexerAnnounceAllDeals(ctx context.Context) error {
	return sm.IndexProvider.IndexerAnnounceAllDeals(ctx)
}

func (sm *BoostAPI) BoostOfflineDealWithData(_ context.Context, dealUuid uuid.UUID, filePath string) (*api.ProviderDealRejectionInfo, error) {
	res, err := sm.StorageProvider.ImportOfflineDealData(dealUuid, filePath)
	return res, err
}

func (sm *BoostAPI) BoostDagstoreGC(ctx context.Context) ([]api.DagstoreShardResult, error) {
	if sm.DAGStore == nil {
		return nil, fmt.Errorf("dagstore not available on this node")
	}

	res, err := sm.DAGStore.GC(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to gc: %w", err)
	}

	ret := make([]api.DagstoreShardResult, 0, len(res.Shards))
	for k, err := range res.Shards {
		r := api.DagstoreShardResult{Key: k.String()}
		if err == nil {
			r.Success = true
		} else {
			r.Success = false
			r.Error = err.Error()
		}
		ret = append(ret, r)
	}

	return ret, nil
}

func (sm *BoostAPI) BoostDagstoreListShards(ctx context.Context) ([]api.DagstoreShardInfo, error) {
	if sm.DAGStore == nil {
		return nil, fmt.Errorf("dagstore not available on this node")
	}

	info := sm.DAGStore.AllShardsInfo()
	ret := make([]api.DagstoreShardInfo, 0, len(info))
	for k, i := range info {
		ret = append(ret, api.DagstoreShardInfo{
			Key:   k.String(),
			State: i.ShardState.String(),
			Error: func() string {
				if i.Error == nil {
					return ""
				}
				return i.Error.Error()
			}(),
		})
	}

	// order by key.
	sort.SliceStable(ret, func(i, j int) bool {
		return ret[i].Key < ret[j].Key
	})

	return ret, nil
}

func (sm *BoostAPI) BoostDagstorePiecesContainingMultihash(ctx context.Context, mh multihash.Multihash) ([]cid.Cid, error) {
	if sm.DAGStore == nil {
		return nil, fmt.Errorf("dagstore not available on this node")
	}

	ks, err := sm.DAGStore.ShardsContainingMultihash(ctx, mh)
	if err != nil {
		return nil, fmt.Errorf("getting pieces containing multihash %s from DAG store: %w", mh, err)
	}

	pieceCids := make([]cid.Cid, 0, len(ks))
	for _, k := range ks {
		pieceCid, err := cid.Parse(k.String())
		if err != nil {
			return nil, fmt.Errorf("parsing DAG store shard key '%s' into cid: %w", k, err)
		}
		pieceCids = append(pieceCids, pieceCid)
	}

	return pieceCids, nil
}

func (sm *BoostAPI) BoostDagstoreInitializeAll(ctx context.Context, params api.DagstoreInitializeAllParams) (<-chan api.DagstoreInitializeAllEvent, error) {
	if sm.DAGStore == nil {
		return nil, fmt.Errorf("dagstore not available on this node")
	}

	if sm.SectorAccessor == nil {
		return nil, fmt.Errorf("sector accessor not available on this node")
	}

	// prepare the thottler tokens.
	var throttle chan struct{}
	if c := params.MaxConcurrency; c > 0 {
		throttle = make(chan struct{}, c)
		for i := 0; i < c; i++ {
			throttle <- struct{}{}
		}
	}

	// are we initializing only unsealed pieces?
	onlyUnsealed := !params.IncludeSealed

	info := sm.DAGStore.AllShardsInfo()
	var toInitialize []string
	for k, i := range info {
		if i.ShardState != dagstore.ShardStateNew {
			continue
		}

		// if we're initializing only unsealed pieces, check if there's an
		// unsealed deal for this piece available.
		if onlyUnsealed {
			pieceCid, err := cid.Decode(k.String())
			if err != nil {
				log.Warnw("DagstoreInitializeAll: failed to decode shard key as piece CID; skipping", "shard_key", k.String(), "error", err)
				continue
			}

			pi, err := sm.PieceStore.GetPieceInfo(pieceCid)
			if err != nil {
				log.Warnw("DagstoreInitializeAll: failed to get piece info; skipping", "piece_cid", pieceCid, "error", err)
				continue
			}

			var isUnsealed bool
			for _, d := range pi.Deals {
				isUnsealed, err = sm.SectorAccessor.IsUnsealed(ctx, d.SectorID, d.Offset.Unpadded(), d.Length.Unpadded())
				if err != nil {
					log.Warnw("DagstoreInitializeAll: failed to get unsealed status; skipping deal", "deal_id", d.DealID, "error", err)
					continue
				}
				if isUnsealed {
					break
				}
			}

			if !isUnsealed {
				log.Infow("DagstoreInitializeAll: skipping piece because it's sealed", "piece_cid", pieceCid, "error", err)
				continue
			}
		}

		// yes, we're initializing this shard.
		toInitialize = append(toInitialize, k.String())
	}

	total := len(toInitialize)
	if total == 0 {
		out := make(chan api.DagstoreInitializeAllEvent)
		close(out)
		return out, nil
	}

	// response channel must be closed when we're done, or the context is cancelled.
	// this buffering is necessary to prevent inflight children goroutines from
	// publishing to a closed channel (res) when the context is cancelled.
	out := make(chan api.DagstoreInitializeAllEvent, 32) // internal buffer.
	res := make(chan api.DagstoreInitializeAllEvent, 32) // returned to caller.

	// pump events back to caller.
	// two events per shard.
	go func() {
		defer close(res)

		for i := 0; i < total*2; i++ {
			select {
			case res <- <-out:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		for i, k := range toInitialize {
			if throttle != nil {
				select {
				case <-throttle:
					// acquired a throttle token, proceed.
				case <-ctx.Done():
					return
				}
			}

			go func(k string, i int) {
				r := api.DagstoreInitializeAllEvent{
					Key:     k,
					Event:   "start",
					Total:   total,
					Current: i + 1, // start with 1
				}
				select {
				case out <- r:
				case <-ctx.Done():
					return
				}

				err := sm.BoostDagstoreInitializeShard(ctx, k)

				if throttle != nil {
					throttle <- struct{}{}
				}

				r.Event = "end"
				if err == nil {
					r.Success = true
				} else {
					r.Success = false
					r.Error = err.Error()
				}

				select {
				case out <- r:
				case <-ctx.Done():
				}
			}(k, i)
		}
	}()

	return res, nil
}

func (sm *BoostAPI) BoostDagstoreInitializeShard(ctx context.Context, key string) error {
	if sm.DAGStore == nil {
		return fmt.Errorf("dagstore not available on this node")
	}

	k := shard.KeyFromString(key)

	info, err := sm.DAGStore.GetShardInfo(k)
	if err != nil {
		return fmt.Errorf("failed to get shard info: %w", err)
	}
	if st := info.ShardState; st != dagstore.ShardStateNew {
		return fmt.Errorf("cannot initialize shard; expected state ShardStateNew, was: %s", st.String())
	}

	ch := make(chan dagstore.ShardResult, 1)
	if err = sm.DAGStore.AcquireShard(ctx, k, ch, dagstore.AcquireOpts{}); err != nil {
		return fmt.Errorf("failed to acquire shard: %w", err)
	}

	var res dagstore.ShardResult
	select {
	case res = <-ch:
	case <-ctx.Done():
		return ctx.Err()
	}

	if err := res.Error; err != nil {
		return fmt.Errorf("failed to acquire shard: %w", err)
	}

	if res.Accessor != nil {
		err = res.Accessor.Close()
		if err != nil {
			log.Warnw("failed to close shard accessor; continuing", "shard_key", k, "error", err)
		}
	}

	return nil
}

func (sm *BoostAPI) BoostDagstoreRegisterShard(ctx context.Context, key string) error {
	if sm.DAGStore == nil {
		return fmt.Errorf("dagstore not available on this node")
	}

	// First check if the shard has already been registered
	k := shard.KeyFromString(key)
	_, err := sm.DAGStore.GetShardInfo(k)
	if err == nil {
		// Shard already registered, nothing further to do
		return nil
	}
	// If the shard is not registered we would expect ErrShardUnknown
	if !errors.Is(err, dagstore.ErrShardUnknown) {
		return fmt.Errorf("getting shard info from DAG store: %w", err)
	}

	pieceCid, err := cid.Parse(key)
	if err != nil {
		return fmt.Errorf("parsing shard key as piece cid: %w", err)
	}
	if err = stores.RegisterShardSync(ctx, sm.DagStoreWrapper, pieceCid, "", true); err != nil {
		return fmt.Errorf("failed to register shard: %w", err)
	}

	return nil
}

func (sm *BoostAPI) BoostDagstoreRecoverShard(ctx context.Context, key string) error {
	if sm.DAGStore == nil {
		return fmt.Errorf("dagstore not available on this node")
	}

	k := shard.KeyFromString(key)

	info, err := sm.DAGStore.GetShardInfo(k)
	if err != nil {
		return fmt.Errorf("failed to get shard info: %w", err)
	}
	if st := info.ShardState; st != dagstore.ShardStateErrored {
		return fmt.Errorf("cannot recover shard; expected state ShardStateErrored, was: %s", st.String())
	}

	ch := make(chan dagstore.ShardResult, 1)
	if err = sm.DAGStore.RecoverShard(ctx, k, ch, dagstore.RecoverOpts{}); err != nil {
		return fmt.Errorf("failed to recover shard: %w", err)
	}

	var res dagstore.ShardResult
	select {
	case res = <-ch:
	case <-ctx.Done():
		return ctx.Err()
	}

	return res.Error
}

func (sm *BoostAPI) BoostDagstoreDestroyShard(ctx context.Context, key string) error {
	if sm.DAGStore == nil {
		return fmt.Errorf("dagstore not available on this node")
	}

	// First check if the shard has already been registered
	k := shard.KeyFromString(key)
	_, err := sm.DAGStore.GetShardInfo(k)
	if err != nil {
		return fmt.Errorf("unable to query dagstore for shard info: %w", err)
	}

	pieceCid, err := cid.Parse(key)
	if err != nil {
		return fmt.Errorf("parsing shard key as piece cid: %w", err)
	}
	if err = stores.DestroyShardSync(ctx, sm.DagStoreWrapper, pieceCid); err != nil {
		return fmt.Errorf("failed to destroy shard: %w", err)
	}
	return nil
}
