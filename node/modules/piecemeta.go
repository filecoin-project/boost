package modules

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/piecemeta"
	"github.com/filecoin-project/boostd-data/couchbase"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/stores"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/markets/sectoraccessor"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/sealer"
	"github.com/filecoin-project/lotus/storage/sectorblocks"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	carindex "github.com/ipld/go-car/v2/index"
	"go.uber.org/fx"
)

func NewPieceMetaStore(cfg *config.Boost) func(lc fx.Lifecycle, r lotus_repo.LockedRepo) piecemeta.Store {
	return func(lc fx.Lifecycle, r lotus_repo.LockedRepo) piecemeta.Store {
		client := piecemeta.NewStore()

		var cancel context.CancelFunc
		var svcCtx context.Context
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				svcCtx, cancel = context.WithCancel(ctx)

				var bdsvc *svc.Service
				if cfg.PieceDirectory.Couchbase.ConnectString != "" {
					// If the couchbase connect string is defined, set up a
					// couchbase client
					bdsvc = svc.NewCouchbase(couchbase.DBSettings{
						ConnectString: cfg.PieceDirectory.Couchbase.ConnectString,
						Auth: couchbase.DBSettingsAuth{
							Username: cfg.PieceDirectory.Couchbase.Username,
							Password: cfg.PieceDirectory.Couchbase.Password,
						},
						Bucket: couchbase.DBSettingsBucket{
							RAMQuotaMB: cfg.PieceDirectory.Couchbase.RAMQuotaMB,
						},
					})
				} else {
					// Setup a leveldb client
					var err error
					bdsvc, err = svc.NewLevelDB(r.Path())
					if err != nil {
						return fmt.Errorf("creating leveldb piece directory: %w", err)
					}
				}
				addr, err := bdsvc.Start(svcCtx)
				if err != nil {
					return fmt.Errorf("starting piece directory service: %w", err)
				}

				return client.Dial(ctx, "http://"+addr)
			},
			OnStop: func(ctx context.Context) error {
				cancel()
				client.Close(ctx)
				return nil
			},
		})

		return client
	}
}

func NewPieceMeta(maddr dtypes.MinerAddress, store piecemeta.Store, secb sectorblocks.SectorBuilder, pp sealer.PieceProvider, full v1api.FullNode) *piecemeta.PieceMeta {
	sa := sectoraccessor.NewSectorAccessor(maddr, secb, pp, full)

	return piecemeta.NewPieceMeta(store, sa)
}

func NewPieceStore(pm *piecemeta.PieceMeta) piecestore.PieceStore {
	return &boostPieceStoreWrapper{pieceMeta: pm}
}

type boostPieceStoreWrapper struct {
	pieceMeta *piecemeta.PieceMeta
}

func (pw *boostPieceStoreWrapper) Start(ctx context.Context) error {
	return nil
}

func (pw *boostPieceStoreWrapper) OnReady(ready shared.ReadyFunc) {
	go ready(nil)
}

func (pw *boostPieceStoreWrapper) AddDealForPiece(pieceCID cid.Cid, dealInfo piecestore.DealInfo) error {
	di := model.DealInfo{
		DealUuid:    uuid.New(),
		ChainDealID: dealInfo.DealID,
		SectorID:    dealInfo.SectorID,
		PieceOffset: dealInfo.Offset,
		PieceLength: dealInfo.Length,
		// TODO: It would be nice if there's some way to figure out the CAR
		// file size here (but I don't think there is an easy way in legacy
		// markets without having access to the piece data itself)
		CarLength: 0,
	}
	return pw.pieceMeta.AddDealForPiece(context.Background(), pieceCID, di)
}

func (pw *boostPieceStoreWrapper) AddPieceBlockLocations(pieceCID cid.Cid, blockLocations map[cid.Cid]piecestore.BlockLocation) error {
	// This method is no longer needed, we keep the CAR file index in the piece metadata store
	return nil
}

func (pw *boostPieceStoreWrapper) GetPieceInfo(pieceCID cid.Cid) (piecestore.PieceInfo, error) {
	pieceDeals, err := pw.pieceMeta.GetPieceDeals(context.TODO(), pieceCID)
	if err != nil {
		return piecestore.PieceInfo{}, fmt.Errorf("getting piece deals from piece metadata store: %w", err)
	}

	dis := make([]piecestore.DealInfo, 0, len(pieceDeals))
	for _, pd := range pieceDeals {
		dis = append(dis, piecestore.DealInfo{
			DealID:   pd.ChainDealID,
			SectorID: pd.SectorID,
			Offset:   pd.PieceOffset,
			Length:   pd.PieceLength,
		})
	}
	return piecestore.PieceInfo{
		PieceCID: pieceCID,
		Deals:    dis,
	}, nil
}

func (pw *boostPieceStoreWrapper) GetCIDInfo(payloadCID cid.Cid) (piecestore.CIDInfo, error) {
	// This is no longer used (CLI calls piece metadata store instead)
	return piecestore.CIDInfo{}, nil
}

func (pw *boostPieceStoreWrapper) ListCidInfoKeys() ([]cid.Cid, error) {
	// This is no longer used (CLI calls piece metadata store instead)
	return nil, nil
}

func (pw *boostPieceStoreWrapper) ListPieceInfoKeys() ([]cid.Cid, error) {
	// This is no longer used (CLI calls piece metadata store instead)
	return nil, nil
}

func NewDAGStoreWrapper(pm *piecemeta.PieceMeta) stores.DAGStoreWrapper {
	// TODO: lotus_modules.NewStorageMarketProvider and lotus_modules.RetrievalProvider
	// take a concrete *dagstore.Wrapper as a parameter. Create boost versions of these
	// that instead take a stores.DAGStoreWrapper parameter
	return &boostDAGStoreWrapper{pieceMeta: pm}
}

type boostDAGStoreWrapper struct {
	pieceMeta *piecemeta.PieceMeta
}

func (dw *boostDAGStoreWrapper) DestroyShard(ctx context.Context, pieceCid cid.Cid, resch chan dagstore.ShardResult) error {
	// This is no longer used (CLI calls piece metadata store instead)
	return nil
}

// Legacy markets calls piecestore.AddDealForPiece before RegisterShard,
// so we do the real work in AddDealForPiece.
func (dw *boostDAGStoreWrapper) RegisterShard(ctx context.Context, pieceCid cid.Cid, carPath string, eagerInit bool, resch chan dagstore.ShardResult) error {
	res := dagstore.ShardResult{
		Key:      shard.KeyFromCID(pieceCid),
		Error:    nil,
		Accessor: nil,
	}

	select {
	case resch <- res:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (dw *boostDAGStoreWrapper) LoadShard(ctx context.Context, pieceCid cid.Cid) (stores.ClosableBlockstore, error) {
	bs, err := dw.pieceMeta.GetBlockstore(ctx, pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting blockstore in LoadShard: %w", err)
	}
	return closableBlockstore{Blockstore: bs}, nil
}

func (dw *boostDAGStoreWrapper) MigrateDeals(ctx context.Context, deals []storagemarket.MinerDeal) (bool, error) {
	// MigrateDeals is no longer needed - it's handled by the piece metadata store
	return false, nil
}

func (dw *boostDAGStoreWrapper) GetPiecesContainingBlock(blockCID cid.Cid) ([]cid.Cid, error) {
	return dw.pieceMeta.PiecesContainingMultihash(context.TODO(), blockCID.Hash())
}

func (dw *boostDAGStoreWrapper) GetIterableIndexForPiece(pieceCid cid.Cid) (carindex.IterableIndex, error) {
	return dw.pieceMeta.GetIterableIndex(context.TODO(), pieceCid)
}

func (dw *boostDAGStoreWrapper) Close() error {
	return nil
}

type closableBlockstore struct {
	bstore.Blockstore
}

func (c closableBlockstore) Close() error {
	return nil
}
