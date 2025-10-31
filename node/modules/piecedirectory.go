package modules

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/cmd/lib"
	bdclient "github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/svc"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"go.uber.org/fx"
)

func NewPieceDirectoryStore(cfg *config.Boost) func(lc fx.Lifecycle, r lotus_repo.LockedRepo, maddr dtypes.MinerAddress) *bdclient.Store {
	return func(lc fx.Lifecycle, r lotus_repo.LockedRepo, maddr dtypes.MinerAddress) *bdclient.Store {
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
						Username:      cfg.LocalIndexDirectory.Yugabyte.Username,
						Password:      cfg.LocalIndexDirectory.Yugabyte.Password,
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
		storageApiInfos := cfg.Retrievals.Graphsync.GraphsyncStorageAccessApiInfo
		if len(storageApiInfos) == 0 {
			// If the endpoints aren't explicitly configured, fall back to just
			// serving retrieval data from the same endpoint where data is stored to
			storageApiInfos = []string{cfg.SectorIndexApiInfo}
		}

		// Create a reader that muxes between all the storage access endpoints
		return lib.NewMultiMinerAccessor(storageApiInfos, full, time.Duration(cfg.Dealmaking.IsUnsealedCacheExpiry))
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

func NewPieceDoctor(cfg *config.Boost) func(lc fx.Lifecycle, maddr dtypes.MinerAddress, store *bdclient.Store, ssm *sectorstatemgr.SectorStateMgr, fullnodeApi api.FullNode) *piecedirectory.Doctor {
	return func(lc fx.Lifecycle, maddr dtypes.MinerAddress, store *bdclient.Store, ssm *sectorstatemgr.SectorStateMgr, fullnodeApi api.FullNode) *piecedirectory.Doctor {
		if !cfg.LocalIndexDirectory.EnablePieceDoctor {
			return &piecedirectory.Doctor{}
		}
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
}
