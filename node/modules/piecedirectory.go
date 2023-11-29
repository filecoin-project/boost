package modules

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost-gfm/piecestore"
	"github.com/filecoin-project/boost-gfm/shared"
	"github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost-gfm/stores"
	"github.com/filecoin-project/boost/cmd/lib"
	bdclient "github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/svc"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	bstore "github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	carindex "github.com/ipld/go-car/v2/index"
	"go.uber.org/fx"
)

func NewPieceDirectoryStore(cfg *config.Boost) func(lc fx.Lifecycle, r lotus_repo.LockedRepo, maddr lotus_dtypes.MinerAddress) *bdclient.Store {
	return func(lc fx.Lifecycle, r lotus_repo.LockedRepo, maddr lotus_dtypes.MinerAddress) *bdclient.Store {
		svcDialOpts := []jsonrpc.Option{
			jsonrpc.WithTimeout(time.Duration(cfg.LocalIndexDirectory.ServiceRPCTimeout)),
		}
		client := bdclient.NewStore(svcDialOpts...)

		var cancel context.CancelFunc
		var svcCtx context.Context
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				if cfg.LocalIndexDirectory.ServiceApiInfo != "" {
					log.Infow("local index directory: dialing the service api", "service-api-info", cfg.LocalIndexDirectory.ServiceApiInfo)
					return client.Dial(ctx, cfg.LocalIndexDirectory.ServiceApiInfo)
				}

				port := int(cfg.LocalIndexDirectory.EmbeddedServicePort)
				if port == 0 {
					return fmt.Errorf("starting local index directory client:" +
						"either LocalIndexDirectory.ServiceApiInfo must be defined or " +
						"LocalIndexDirectory.EmbeddedServicePort must be non-zero")
				}

				svcCtx, cancel = context.WithCancel(ctx)
				var bdsvc *svc.Service
				switch {
				case cfg.LocalIndexDirectory.Yugabyte.Enabled:
					log.Infow("local index directory: connecting to yugabyte server",
						"connect-string", cfg.LocalIndexDirectory.Yugabyte.ConnectString,
						"hosts", cfg.LocalIndexDirectory.Yugabyte.Hosts)

					// Set up a local index directory service that connects to the yugabyte db
					settings := yugabyte.DBSettings{
						Hosts:         cfg.LocalIndexDirectory.Yugabyte.Hosts,
						ConnectString: cfg.LocalIndexDirectory.Yugabyte.ConnectString,
					}
					migrator := yugabyte.NewMigrator(settings, address.Address(maddr))
					bdsvc = svc.NewYugabyte(settings, migrator)

				case cfg.LocalIndexDirectory.Leveldb.Enabled:
					log.Infow("local index directory: connecting to leveldb instance")

					// Setup a local index directory service that connects to the leveldb
					var err error
					bdsvc, err = svc.NewLevelDB(r.Path())
					if err != nil {
						return fmt.Errorf("creating leveldb local index directory: %w", err)
					}

				default:
					return fmt.Errorf("starting local index directory client: " +
						"neither yugabyte nor leveldb is enabled in config - " +
						"you must explicitly configure either LocalIndexDirectory.Yugabyte " +
						"or LocalIndexDirectory.Leveldb as the local index directory implementation")
				}

				// Start the embedded local index directory service
				addr := fmt.Sprintf("localhost:%d", port)
				_, err := bdsvc.Start(svcCtx, addr)
				if err != nil {
					return fmt.Errorf("starting local index directory service: %w", err)
				}

				// Connect to the embedded service
				return client.Dial(ctx, fmt.Sprintf("ws://%s", addr))
			},
			OnStop: func(ctx context.Context) error {
				// cancel is nil if we use the service api (boostd-data process)
				if cancel != nil {
					cancel()
				}

				client.Close(ctx)
				return nil
			},
		})

		return client
	}
}

func NewMultiminerSectorAccessor(cfg *config.Boost) func(full v1api.FullNode) *lib.MultiMinerAccessor {
	return func(full v1api.FullNode) *lib.MultiMinerAccessor {
		// Get the endpoints of all the miners that this boost node can query
		// for retrieval data when serving graphsync retrievals
		storageApiInfos := cfg.Dealmaking.GraphsyncStorageAccessApiInfo
		if len(storageApiInfos) == 0 {
			// If the endpoints aren't explicitly configured, fall back to just
			// serving retrieval data from the same endpoint where data is stored to
			storageApiInfos = []string{cfg.SectorIndexApiInfo}
		}

		// Create a reader that muxes between all the storage access endpoints
		return lib.NewMultiMinerAccessor(storageApiInfos, full)
	}
}

func NewPieceDirectory(cfg *config.Boost) func(lc fx.Lifecycle, maddr dtypes.MinerAddress, store *bdclient.Store, sa *lib.MultiMinerAccessor) *piecedirectory.PieceDirectory {
	return func(lc fx.Lifecycle, maddr dtypes.MinerAddress, store *bdclient.Store, sa *lib.MultiMinerAccessor) *piecedirectory.PieceDirectory {

		// Create the piece directory implementation
		pdctx, cancel := context.WithCancel(context.Background())
		pd := piecedirectory.NewPieceDirectory(store, sa,
			cfg.LocalIndexDirectory.ParallelAddIndexLimit,
			piecedirectory.WithAddIndexConcurrency(cfg.LocalIndexDirectory.AddIndexConcurrency))
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				err := sa.Start(ctx, log)
				if err != nil {
					return fmt.Errorf("starting piece directory: connecting to miners: %w", err)
				}
				pd.Start(pdctx)
				return nil
			},
			OnStop: func(ctx context.Context) error {
				cancel()
				sa.Close()
				return nil
			},
		})

		return pd
	}
}

func NewPieceStore(pm *piecedirectory.PieceDirectory, maddr address.Address) piecestore.PieceStore {
	return &boostPieceStoreWrapper{piecedirectory: pm, maddr: maddr}
}

func NewPieceDoctor(lc fx.Lifecycle, maddr lotus_dtypes.MinerAddress, store *bdclient.Store, ssm *sectorstatemgr.SectorStateMgr, fullnodeApi api.FullNode) *piecedirectory.Doctor {
	doc := piecedirectory.NewDoctor(address.Address(maddr), store, ssm, fullnodeApi)
	docctx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go doc.Run(docctx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			return nil
		},
	})
	return doc
}

type boostPieceStoreWrapper struct {
	piecedirectory *piecedirectory.PieceDirectory
	maddr          address.Address
}

func (pw *boostPieceStoreWrapper) Start(ctx context.Context) error {
	return nil
}

func (pw *boostPieceStoreWrapper) OnReady(ready shared.ReadyFunc) {
	go ready(nil)
}

func (pw *boostPieceStoreWrapper) AddDealForPiece(pieceCID cid.Cid, proposalCid cid.Cid, dealInfo piecestore.DealInfo) error {
	di := model.DealInfo{
		DealUuid:    proposalCid.String(),
		IsLegacy:    true,
		ChainDealID: dealInfo.DealID,
		MinerAddr:   pw.maddr,
		SectorID:    dealInfo.SectorID,
		PieceOffset: dealInfo.Offset,
		PieceLength: dealInfo.Length,
		// TODO: It would be nice if there's some way to figure out the CAR
		// file size here (but I don't think there is an easy way in legacy
		// markets without having access to the piece data itself)
		CarLength:    0,
		IsDirectDeal: false, // There should be no direct deals from legacy code base
	}
	return pw.piecedirectory.AddDealForPiece(context.Background(), pieceCID, di)
}

func (pw *boostPieceStoreWrapper) AddPieceBlockLocations(pieceCID cid.Cid, blockLocations map[cid.Cid]piecestore.BlockLocation) error {
	// This method is no longer needed, we keep the CAR file index in the piece metadata store
	return nil
}

func (pw *boostPieceStoreWrapper) GetPieceInfo(pieceCID cid.Cid) (piecestore.PieceInfo, error) {
	pieceDeals, err := pw.piecedirectory.GetPieceDeals(context.TODO(), pieceCID)
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

func NewDAGStoreWrapper(pm *piecedirectory.PieceDirectory) stores.DAGStoreWrapper {
	// TODO: lotus_modules.NewStorageMarketProvider and lotus_modules.RetrievalProvider
	// take a concrete *dagstore.Wrapper as a parameter. Create boost versions of these
	// that instead take a stores.DAGStoreWrapper parameter
	return &boostDAGStoreWrapper{piecedirectory: pm}
}

type boostDAGStoreWrapper struct {
	piecedirectory *piecedirectory.PieceDirectory
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
	bs, err := dw.piecedirectory.GetBlockstore(ctx, pieceCid)
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
	return dw.piecedirectory.PiecesContainingMultihash(context.TODO(), blockCID.Hash())
}

func (dw *boostDAGStoreWrapper) GetIterableIndexForPiece(pieceCid cid.Cid) (carindex.IterableIndex, error) {
	return dw.piecedirectory.GetIterableIndex(context.TODO(), pieceCid)
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

func NewBlockGetter(pd *piecedirectory.PieceDirectory) gql.BlockGetter {
	return &pdBlockGetter{pd: pd}
}

type pdBlockGetter struct {
	pd *piecedirectory.PieceDirectory
}

func (p *pdBlockGetter) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	bz, err := p.pd.BlockstoreGet(ctx, c)
	if err != nil {
		return nil, err
	}

	return blocks.NewBlockWithCid(bz, c)
}
