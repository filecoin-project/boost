package modules

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/sealingpipeline"
	"github.com/filecoin-project/boost/storage/sectorblocks"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/lp2pimpl"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/tracing"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/index"
	"github.com/filecoin-project/dagstore/indexbs"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/shared"
	lotus_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api/v1api"
	ctypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/journal"
	"github.com/filecoin-project/lotus/lib/sigs"
	mdagstore "github.com/filecoin-project/lotus/markets/dagstore"
	mktsdagstore "github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/filecoin-project/lotus/markets/storageadapter"
	lotus_config "github.com/filecoin-project/lotus/node/config"
	"github.com/filecoin-project/lotus/node/modules"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/filecoin-project/lotus/node/repo"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	levelds "github.com/ipfs/go-ds-leveldb"
	measure "github.com/ipfs/go-ds-measure"
	"github.com/libp2p/go-libp2p-core/host"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
	"go.uber.org/fx"
	"go.uber.org/multierr"
	"golang.org/x/xerrors"
)

const lotusScheme = "lotus"

const (
	EnvDAGStoreCopyConcurrency = "LOTUS_DAGSTORE_COPY_CONCURRENCY"
	DefaultDAGStoreDir         = "dagstore"
)

var (
	StorageCounterDSPrefix = "/storage/nextid"
)

func RetrievalDealFilter(userFilter dtypes.RetrievalDealFilter) func(onlineOk dtypes.ConsiderOnlineRetrievalDealsConfigFunc,
	offlineOk dtypes.ConsiderOfflineRetrievalDealsConfigFunc) dtypes.RetrievalDealFilter {
	return func(onlineOk dtypes.ConsiderOnlineRetrievalDealsConfigFunc,
		offlineOk dtypes.ConsiderOfflineRetrievalDealsConfigFunc) dtypes.RetrievalDealFilter {
		return func(ctx context.Context, state retrievalmarket.ProviderDealState) (bool, string, error) {
			b, err := onlineOk()
			if err != nil {
				return false, "miner error", err
			}

			if !b {
				log.Warn("online retrieval deal consideration disabled; rejecting retrieval deal proposal from client")
				return false, "miner is not accepting online retrieval deals", nil
			}

			b, err = offlineOk()
			if err != nil {
				return false, "miner error", err
			}

			if !b {
				log.Info("offline retrieval has not been implemented yet")
			}

			if userFilter != nil {
				return userFilter(ctx, state)
			}

			return true, "", nil
		}
	}
}

func NewConsiderOnlineStorageDealsConfigFunc(r lotus_repo.LockedRepo) (dtypes.ConsiderOnlineStorageDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderOnlineStorageDeals
		})
		return
	}, nil
}

func NewSetConsideringOnlineStorageDealsFunc(r lotus_repo.LockedRepo) (dtypes.SetConsiderOnlineStorageDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderOnlineStorageDeals = b
		})
		return
	}, nil
}

func NewConsiderOnlineRetrievalDealsConfigFunc(r lotus_repo.LockedRepo) (dtypes.ConsiderOnlineRetrievalDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderOnlineRetrievalDeals
		})
		return
	}, nil
}

func NewSetConsiderOnlineRetrievalDealsConfigFunc(r lotus_repo.LockedRepo) (dtypes.SetConsiderOnlineRetrievalDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderOnlineRetrievalDeals = b
		})
		return
	}, nil
}

func NewStorageDealPieceCidBlocklistConfigFunc(r lotus_repo.LockedRepo) (dtypes.StorageDealPieceCidBlocklistConfigFunc, error) {
	return func() (out []cid.Cid, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.PieceCidBlocklist
		})
		return
	}, nil
}

func NewSetStorageDealPieceCidBlocklistConfigFunc(r lotus_repo.LockedRepo) (dtypes.SetStorageDealPieceCidBlocklistConfigFunc, error) {
	return func(blocklist []cid.Cid) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.PieceCidBlocklist = blocklist
		})
		return
	}, nil
}

func NewConsiderOfflineStorageDealsConfigFunc(r lotus_repo.LockedRepo) (dtypes.ConsiderOfflineStorageDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderOfflineStorageDeals
		})
		return
	}, nil
}

func NewSetConsideringOfflineStorageDealsFunc(r lotus_repo.LockedRepo) (dtypes.SetConsiderOfflineStorageDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderOfflineStorageDeals = b
		})
		return
	}, nil
}

func NewConsiderOfflineRetrievalDealsConfigFunc(r lotus_repo.LockedRepo) (dtypes.ConsiderOfflineRetrievalDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderOfflineRetrievalDeals
		})
		return
	}, nil
}

func NewSetConsiderOfflineRetrievalDealsConfigFunc(r lotus_repo.LockedRepo) (dtypes.SetConsiderOfflineRetrievalDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderOfflineRetrievalDeals = b
		})
		return
	}, nil
}

func NewConsiderVerifiedStorageDealsConfigFunc(r lotus_repo.LockedRepo) (dtypes.ConsiderVerifiedStorageDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderVerifiedStorageDeals
		})
		return
	}, nil
}

func NewSetConsideringVerifiedStorageDealsFunc(r lotus_repo.LockedRepo) (dtypes.SetConsiderVerifiedStorageDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderVerifiedStorageDeals = b
		})
		return
	}, nil
}

func NewConsiderUnverifiedStorageDealsConfigFunc(r lotus_repo.LockedRepo) (dtypes.ConsiderUnverifiedStorageDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderUnverifiedStorageDeals
		})
		return
	}, nil
}

func NewSetConsideringUnverifiedStorageDealsFunc(r lotus_repo.LockedRepo) (dtypes.SetConsiderUnverifiedStorageDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderUnverifiedStorageDeals = b
		})
		return
	}, nil
}

func NewSetExpectedSealDurationFunc(r lotus_repo.LockedRepo) (dtypes.SetExpectedSealDurationFunc, error) {
	return func(delay time.Duration) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ExpectedSealDuration = config.Duration(delay)
		})
		return
	}, nil
}

func NewGetExpectedSealDurationFunc(r lotus_repo.LockedRepo) (dtypes.GetExpectedSealDurationFunc, error) {
	return func() (out time.Duration, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = time.Duration(cfg.Dealmaking.ExpectedSealDuration)
		})
		return
	}, nil
}

func NewSetMaxDealStartDelayFunc(r lotus_repo.LockedRepo) (dtypes.SetMaxDealStartDelayFunc, error) {
	return func(delay time.Duration) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.MaxDealStartDelay = config.Duration(delay)
		})
		return
	}, nil
}

func NewGetMaxDealStartDelayFunc(r lotus_repo.LockedRepo) (dtypes.GetMaxDealStartDelayFunc, error) {
	return func() (out time.Duration, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = time.Duration(cfg.Dealmaking.MaxDealStartDelay)
		})
		return
	}, nil
}

func readCfg(r lotus_repo.LockedRepo, accessor func(*config.Boost)) error {
	raw, err := r.Config()
	if err != nil {
		return err
	}

	cfg, ok := raw.(*config.Boost)
	if !ok {
		return errors.New("expected address of config.Boost")
	}

	accessor(cfg)

	return nil
}

func mutateCfg(r lotus_repo.LockedRepo, mutator func(*config.Boost)) error {
	var typeErr error

	setConfigErr := r.SetConfig(func(raw interface{}) {
		cfg, ok := raw.(*config.Boost)
		if !ok {
			typeErr = errors.New("expected boost config")
			return
		}

		mutator(cfg)
	})

	return multierr.Combine(typeErr, setConfigErr)
}

func StorageNetworkName(ctx helpers.MetricsCtx, a v1api.FullNode) (dtypes.NetworkName, error) {
	n, err := a.StateNetworkName(ctx)
	if err != nil {
		return "", err
	}
	return dtypes.NetworkName(n), nil
}

func NewBoostDB(r lotus_repo.LockedRepo) (*sql.DB, error) {
	// fixes error "database is locked", caused by concurrent access from deal goroutines to a single sqlite3 db connection
	// see: https://github.com/mattn/go-sqlite3#:~:text=Error%3A%20database%20is%20locked
	dbPath := path.Join(r.Path(), "boost.db?cache=shared")
	return db.SqlDB(dbPath)
}

type LogSqlDB struct {
	db *sql.DB
}

func NewLogsSqlDB(r repo.LockedRepo) (*LogSqlDB, error) {
	// fixes error "database is locked", caused by concurrent access from deal goroutines to a single sqlite3 db connection
	// see: https://github.com/mattn/go-sqlite3#:~:text=Error%3A%20database%20is%20locked
	dbPath := path.Join(r.Path(), "boost.logs.db?cache=shared")
	d, err := db.SqlDB(dbPath)
	if err != nil {
		return nil, err
	}
	return &LogSqlDB{d}, nil
}

func NewDealsDB(sqldb *sql.DB) *db.DealsDB {
	return db.NewDealsDB(sqldb)
}

func NewLogsDB(logsSqlDB *LogSqlDB) *db.LogsDB {
	return db.NewLogsDB(logsSqlDB.db)
}

func NewProposalLogsDB(sqldb *sql.DB) *db.ProposalLogsDB {
	return db.NewProposalLogsDB(sqldb)
}

func NewFundsDB(sqldb *sql.DB) *db.FundsDB {
	return db.NewFundsDB(sqldb)
}

func HandleLegacyDeals(mctx helpers.MetricsCtx, lc fx.Lifecycle, host host.Host, lsp lotus_storagemarket.StorageProvider, j journal.Journal) error {
	log.Info("starting legacy storage provider")
	modules.HandleDeals(mctx, lc, host, lsp, j)
	return nil
}

func HandleBoostDeals(lc fx.Lifecycle, h host.Host, prov *storagemarket.Provider, a v1api.FullNode, legacySP lotus_storagemarket.StorageProvider, idxProv *indexprovider.Wrapper, plDB *db.ProposalLogsDB, spApi sealingpipeline.API) {
	lp2pnet := lp2pimpl.NewDealProvider(h, prov, a, plDB, spApi)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			// Wait for the legacy SP to fire the "ready" event before starting
			// the boost SP.
			// Boost overrides some listeners so it must start after the legacy SP.
			errch := make(chan error, 1)
			log.Info("waiting for legacy storage provider 'ready' event")
			legacySP.OnReady(func(err error) {
				errch <- err
			})
			err := <-errch
			if err != nil {
				log.Errorf("failed to start legacy storage provider: %w", err)
				return err
			}
			log.Info("legacy storage provider started successfully")

			// Start the Boost SP
			log.Info("starting boost storage provider")
			err = prov.Start()
			if err != nil {
				return fmt.Errorf("starting storage provider: %w", err)
			}
			lp2pnet.Start(ctx)
			log.Info("boost storage provider started successfully")

			// Start the Boost Index Provider.
			// It overrides the multihash lister registered by the legacy
			// index provider so it must start after the legacy SP.
			log.Info("starting boost index provider wrapper")
			idxProv.Start(ctx)
			log.Info("boost index provider wrapper started successfully")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			lp2pnet.Stop()
			prov.Stop()
			return nil
		},
	})
}

type signatureVerifier struct {
	fn v1api.FullNode
}

func (s *signatureVerifier) VerifySignature(ctx context.Context, sig crypto.Signature, addr address.Address, input []byte, encodedTs shared.TipSetToken) (bool, error) {
	addr, err := s.fn.StateAccountKey(ctx, addr, ctypes.EmptyTSK)
	if err != nil {
		return false, err
	}

	err = sigs.Verify(&sig, addr, input)
	return err == nil, err
}

func NewChainDealManager(a v1api.FullNode) *storagemarket.ChainDealManager {
	cdmCfg := storagemarket.ChainDealManagerCfg{PublishDealsConfidence: 2 * build.MessageConfidence}
	return storagemarket.NewChainDealManager(a, cdmCfg)
}

func NewStorageMarketProvider(provAddr address.Address, cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, a v1api.FullNode, sqldb *sql.DB, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storageadapter.DealPublisher, secb *sectorblocks.SectorBlocks, commpc types.CommpCalculator, sps sealingpipeline.API, df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB, dagst *Wrapper, ps lotus_dtypes.ProviderPieceStore, ip *indexprovider.Wrapper, lp lotus_storagemarket.StorageProvider, cdm *storagemarket.ChainDealManager) (*storagemarket.Provider, error) {
	return func(lc fx.Lifecycle, h host.Host, a v1api.FullNode, sqldb *sql.DB, dealsDB *db.DealsDB,
		fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storageadapter.DealPublisher, secb *sectorblocks.SectorBlocks,
		commpc types.CommpCalculator, sps sealingpipeline.API,
		df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB,
		dagst *Wrapper, ps lotus_dtypes.ProviderPieceStore, ip *indexprovider.Wrapper,
		lp lotus_storagemarket.StorageProvider, cdm *storagemarket.ChainDealManager) (*storagemarket.Provider, error) {

		prvCfg := storagemarket.Config{
			MaxTransferDuration:     time.Duration(cfg.Dealmaking.MaxTransferDuration),
			RemoteCommp:             cfg.Dealmaking.RemoteCommp,
			MaxConcurrentLocalCommp: cfg.Dealmaking.MaxConcurrentLocalCommp,
			TransferLimiter: storagemarket.TransferLimiterConfig{
				MaxConcurrent:    cfg.Dealmaking.HttpTransferMaxConcurrentDownloads,
				StallCheckPeriod: time.Duration(cfg.Dealmaking.HttpTransferStallCheckPeriod),
				StallTimeout:     time.Duration(cfg.Dealmaking.HttpTransferStallTimeout),
			},
		}
		dl := logs.NewDealLogger(logsDB)
		tspt := httptransport.New(h, dl)
		prov, err := storagemarket.NewProvider(prvCfg, sqldb, dealsDB, fundMgr, storageMgr, a, dp, provAddr, secb, commpc,
			sps, cdm, df, logsSqlDB.db, logsDB, dagst, ps, ip, lp, &signatureVerifier{a}, dl, tspt)
		if err != nil {
			return nil, err
		}

		return prov, nil
	}
}

func NewGraphqlServer(cfg *config.Boost) func(lc fx.Lifecycle, r repo.LockedRepo, h host.Host, prov *storagemarket.Provider, dealsDB *db.DealsDB, logsDB *db.LogsDB, plDB *db.ProposalLogsDB, fundsDB *db.FundsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, publisher *storageadapter.DealPublisher, spApi sealingpipeline.API, legacyProv lotus_storagemarket.StorageProvider, legacyDT lotus_dtypes.ProviderDataTransfer, ps lotus_dtypes.ProviderPieceStore, sa retrievalmarket.SectorAccessor, dagst dagstore.Interface, fullNode v1api.FullNode) *gql.Server {
	return func(lc fx.Lifecycle, r repo.LockedRepo, h host.Host, prov *storagemarket.Provider, dealsDB *db.DealsDB, logsDB *db.LogsDB, plDB *db.ProposalLogsDB, fundsDB *db.FundsDB, fundMgr *fundmanager.FundManager,
		storageMgr *storagemanager.StorageManager, publisher *storageadapter.DealPublisher, spApi sealingpipeline.API,
		legacyProv lotus_storagemarket.StorageProvider, legacyDT lotus_dtypes.ProviderDataTransfer,
		ps lotus_dtypes.ProviderPieceStore, sa retrievalmarket.SectorAccessor, dagst dagstore.Interface, fullNode v1api.FullNode) *gql.Server {

		resolver := gql.NewResolver(cfg, r, h, dealsDB, logsDB, plDB, fundsDB, fundMgr, storageMgr, spApi, prov, legacyProv, legacyDT, ps, sa, dagst, publisher, fullNode)
		server := gql.NewServer(resolver)

		lc.Append(fx.Hook{
			OnStart: server.Start,
			OnStop:  server.Stop,
		})

		return server
	}
}

func NewIndexBackedBlockstore(dagst dagstore.Interface, ps lotus_dtypes.ProviderPieceStore, sa retrievalmarket.SectorAccessor, rp retrievalmarket.RetrievalProvider, h host.Host) (dtypes.IndexBackedBlockstore, error) {
	sf := indexbs.ShardSelectorF(func(c cid.Cid, shards []shard.Key) (shard.Key, error) {
		for _, sk := range shards {
			// parse piece CID
			pieceCid, err := cid.Parse(sk.String())
			if err != nil {
				return shard.Key{}, fmt.Errorf("failed to parse cid")
			}
			// read piece info from piece store
			pieceInfo, err := ps.GetPieceInfo(pieceCid)
			if err != nil {
				return shard.Key{}, fmt.Errorf("failed to get piece info: %w", err)
			}

			// check if piece is in unsealed sector
			isUnsealed := false
			for _, di := range pieceInfo.Deals {
				isUnsealed, err = sa.IsUnsealed(context.TODO(), di.SectorID, di.Offset.Unpadded(), di.Length.Unpadded())
				if err != nil {
					log.Errorf("failed to find out if sector %d is unsealed, err=%s", di.SectorID, err)
					continue
				}
				if isUnsealed {
					break
				}
			}

			// sealed sector, skip
			if !isUnsealed {
				continue
			}

			// The piece is in an unsealed sector
			// Is it marked for free retrieval ?
			// we don't pass the payload cid so we can potentially cache on a piece cid basis
			input := retrievalmarket.PricingInput{
				// piece from which the payload will be retrieved
				PieceCID: pieceInfo.PieceCID,
				Unsealed: true,
			}

			var dealsIds []abi.DealID
			for _, d := range pieceInfo.Deals {
				dealsIds = append(dealsIds, d.DealID)
			}

			ask, err := rp.GetDynamicAsk(context.TODO(), input, dealsIds)
			if err != nil {
				return shard.Key{}, fmt.Errorf("failed to get retrieval ask: %w", err)
			}

			// if piece is free we found a match
			// otherwise, go to the next shard
			if ask.PricePerByte.NilOrZero() {
				return sk, nil
			}
		}

		return shard.Key{}, indexbs.ErrNoShardSelected
	})

	rbs, err := indexbs.NewIndexBackedBlockstore(dagst, sf, 100, tracing.Tracer)
	if err != nil {
		return nil, fmt.Errorf("failed to create index backed blockstore: %w", err)
	}
	return dtypes.IndexBackedBlockstore(rbs), nil
}

func NewTracing(cfg *config.Boost) func(lc fx.Lifecycle) (*tracing.Tracing, error) {
	return func(lc fx.Lifecycle) (*tracing.Tracing, error) {
		if cfg.Tracing.Enabled {
			// Instantiate the tracer and exporter
			stop, err := tracing.New(cfg.Tracing.ServiceName, cfg.Tracing.Endpoint)
			if err != nil {
				return nil, fmt.Errorf("failed to instantiate tracer: %w", err)
			}
			lc.Append(fx.Hook{
				OnStop: stop,
			})
		}

		return &tracing.Tracing{}, nil
	}
}

// DAGStore constructs a DAG store using the supplied minerAPI, and the
// user configuration. It returns both the DAGStore and the Wrapper suitable for
// passing to markets.
func DAGStore(cfg lotus_config.DAGStoreConfig) func(lc fx.Lifecycle, r lotus_repo.LockedRepo, minerAPI mdagstore.MinerAPI, h host.Host) (*dagstore.DAGStore, *Wrapper, error) {
	return func(lc fx.Lifecycle, r lotus_repo.LockedRepo, minerAPI mdagstore.MinerAPI, h host.Host) (*dagstore.DAGStore, *Wrapper, error) {
		// fall back to default root directory if not explicitly set in the config.
		if cfg.RootDir == "" {
			cfg.RootDir = filepath.Join(r.Path(), DefaultDAGStoreDir)
		}

		v, ok := os.LookupEnv(EnvDAGStoreCopyConcurrency)
		if ok {
			concurrency, err := strconv.Atoi(v)
			if err == nil {
				cfg.MaxConcurrentReadyFetches = concurrency
			}
		}

		dagst, w, err := NewDAGStore(cfg, minerAPI, h)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to create DAG store: %w", err)
		}

		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				return w.Start(ctx)
			},
			OnStop: func(context.Context) error {
				return w.Close()
			},
		})

		return dagst, w, nil
	}
}

func NewDAGStore(cfg lotus_config.DAGStoreConfig, minerApi mdagstore.MinerAPI, h host.Host) (*dagstore.DAGStore, *Wrapper, error) {
	// construct the DAG Store.
	registry := mount.NewRegistry()
	if err := registry.Register(lotusScheme, mountTemplate(minerApi)); err != nil {
		return nil, nil, xerrors.Errorf("failed to create registry: %w", err)
	}

	// The dagstore will write Shard failures to the `failureCh` here.
	failureCh := make(chan dagstore.ShardResult, 1)

	// The dagstore will write Trace events to the `traceCh` here.
	traceCh := make(chan dagstore.Trace, 32)

	var (
		transientsDir = filepath.Join(cfg.RootDir, "transients")
		datastoreDir  = filepath.Join(cfg.RootDir, "datastore")
		indexDir      = filepath.Join(cfg.RootDir, "index")
	)

	dstore, err := newDatastore(datastoreDir)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to create dagstore datastore in %s: %w", datastoreDir, err)
	}

	irepo, err := index.NewFSRepo(indexDir)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to initialise dagstore index repo: %w", err)
	}

	topIndex := index.NewInverted(dstore)
	dcfg := dagstore.Config{
		TransientsDir: transientsDir,
		IndexRepo:     irepo,
		Datastore:     dstore,
		MountRegistry: registry,
		FailureCh:     failureCh,
		TraceCh:       traceCh,
		TopLevelIndex: topIndex,
		// not limiting fetches globally, as the Lotus mount does
		// conditional throttling.
		MaxConcurrentIndex:        cfg.MaxConcurrentIndex,
		MaxConcurrentReadyFetches: cfg.MaxConcurrentReadyFetches,
		RecoverOnStart:            dagstore.RecoverOnAcquire,
		Tracer:                    tracing.Tracer, // TODO: use proper dep injection
	}

	dagst, err := dagstore.NewDAGStore(dcfg)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to create DAG store: %w", err)
	}

	w := &Wrapper{
		cfg:        cfg,
		dagst:      dagst,
		minerAPI:   minerApi,
		failureCh:  failureCh,
		traceCh:    traceCh,
		gcInterval: time.Duration(cfg.GCInterval),
	}

	return dagst, w, nil
}

// newDatastore creates a datastore under the given base directory
// for dagstore metadata.
func newDatastore(dir string) (ds.Batching, error) {
	// Create the datastore directory if it doesn't exist yet.
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, xerrors.Errorf("failed to create directory %s for DAG store datastore: %w", dir, err)
	}

	// Create a new LevelDB datastore
	dstore, err := levelds.NewDatastore(dir, &levelds.Options{
		Compression: ldbopts.NoCompression,
		NoSync:      false,
		Strict:      ldbopts.StrictAll,
		ReadOnly:    false,
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to open datastore for DAG store: %w", err)
	}
	// Keep statistics about the datastore
	mds := measure.New("measure.", dstore)
	return mds, nil
}

func mountTemplate(api mktsdagstore.MinerAPI) *mktsdagstore.LotusMount {
	return &mktsdagstore.LotusMount{API: api}
}
