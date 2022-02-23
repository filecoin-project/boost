package modules

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/filecoin-project/lotus/markets/dagstore"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/sealingpipeline"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/lp2pimpl"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	retrievalimpl "github.com/filecoin-project/go-fil-markets/retrievalmarket/impl"
	rmnet "github.com/filecoin-project/go-fil-markets/retrievalmarket/network"
	lotus_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statestore"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/blockstore"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"
	"github.com/filecoin-project/lotus/journal"
	"github.com/filecoin-project/lotus/markets"
	marketevents "github.com/filecoin-project/lotus/markets/loggers"
	"github.com/filecoin-project/lotus/markets/pricing"
	lotus_config "github.com/filecoin-project/lotus/node/config"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/filecoin-project/lotus/node/repo"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/sectorblocks"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/libp2p/go-libp2p-core/host"
	"go.uber.org/fx"
	"go.uber.org/multierr"
	"golang.org/x/xerrors"
)

var (
	StorageCounterDSPrefix = "/storage/nextid"
	StagingAreaDirName     = "deal-staging"
)

func HandleRetrieval(host host.Host, lc fx.Lifecycle, m retrievalmarket.RetrievalProvider, j journal.Journal) {
	m.OnReady(marketevents.ReadyLogger("retrieval provider"))
	lc.Append(fx.Hook{

		OnStart: func(ctx context.Context) error {
			m.SubscribeToEvents(marketevents.RetrievalProviderLogger)

			evtType := j.RegisterEventType("markets/retrieval/provider", "state_change")
			m.SubscribeToEvents(markets.RetrievalProviderJournaler(j, evtType))

			return m.Start(ctx)
		},
		OnStop: func(context.Context) error {
			return m.Stop()
		},
	})
}

func HandleMigrateProviderFunds(lc fx.Lifecycle, ds lotus_dtypes.MetadataDS, node lapi.FullNode, minerAddress lotus_dtypes.MinerAddress) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			b, err := ds.Get(ctx, datastore.NewKey("/marketfunds/provider"))
			if err != nil {
				if xerrors.Is(err, datastore.ErrNotFound) {
					return nil
				}
				return err
			}

			var value abi.TokenAmount
			if err = value.UnmarshalCBOR(bytes.NewReader(b)); err != nil {
				return err
			}
			ts, err := node.ChainHead(ctx)
			if err != nil {
				log.Errorf("provider funds migration - getting chain head: %v", err)
				return nil
			}

			mi, err := node.StateMinerInfo(ctx, address.Address(minerAddress), ts.Key())
			if err != nil {
				log.Errorf("provider funds migration - getting miner info %s: %v", minerAddress, err)
				return nil
			}

			_, err = node.MarketReserveFunds(ctx, mi.Worker, address.Address(minerAddress), value)
			if err != nil {
				log.Errorf("provider funds migration - reserving funds (wallet %s, addr %s, funds %d): %v",
					mi.Worker, minerAddress, value, err)
				return nil
			}

			return ds.Delete(ctx, datastore.NewKey("/marketfunds/provider"))
		},
	})
}

// StagingBlockstore creates a blockstore for staging blocks for a miner
// in a storage deal, prior to sealing
func StagingBlockstore(lc fx.Lifecycle, mctx helpers.MetricsCtx, r lotus_repo.LockedRepo) (lotus_dtypes.StagingBlockstore, error) {
	ctx := helpers.LifecycleCtx(mctx, lc)
	stagingds, err := r.Datastore(ctx, "/staging")
	if err != nil {
		return nil, err
	}

	return blockstore.FromDatastore(stagingds), nil
}

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

func RetrievalNetwork(h host.Host) rmnet.RetrievalMarketNetwork {
	return rmnet.NewFromLibp2pHost(h)
}

var WorkerCallsPrefix = datastore.NewKey("/worker/calls")
var ManagerWorkPrefix = datastore.NewKey("/stmgr/calls")

func LocalStorage(mctx helpers.MetricsCtx, lc fx.Lifecycle, ls stores.LocalStorage, si stores.SectorIndex, urls stores.URLs) (*stores.Local, error) {
	ctx := helpers.LifecycleCtx(mctx, lc)
	return stores.NewLocal(ctx, ls, si, urls)
}

func RemoteStorage(lstor *stores.Local, si stores.SectorIndex, sa sectorstorage.StorageAuth, sc sectorstorage.SealerConfig) *stores.Remote {
	return stores.NewRemote(lstor, si, http.Header(sa), sc.ParallelFetchLimit, &stores.DefaultPartialFileHandler{})
}

func SectorStorage(mctx helpers.MetricsCtx, lc fx.Lifecycle, lstor *stores.Local, stor *stores.Remote, ls stores.LocalStorage, si stores.SectorIndex, sc sectorstorage.SealerConfig, ds lotus_dtypes.MetadataDS) (*sectorstorage.Manager, error) {
	ctx := helpers.LifecycleCtx(mctx, lc)

	wsts := statestore.New(namespace.Wrap(ds, WorkerCallsPrefix))
	smsts := statestore.New(namespace.Wrap(ds, ManagerWorkPrefix))

	sst, err := sectorstorage.New(ctx, lstor, stor, ls, si, sc, wsts, smsts)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: sst.Close,
	})

	return sst, nil
}

// LotusRetrievalPricingFunc configures the pricing function to use for retrieval deals. // TODO(anteva): Fix me
func LotusRetrievalPricingFunc() func(_ lotus_dtypes.ConsiderOnlineRetrievalDealsConfigFunc,
	_ lotus_dtypes.ConsiderOfflineRetrievalDealsConfigFunc) lotus_dtypes.RetrievalPricingFunc {

	cfg := lotus_config.DealmakingConfig{}

	return func(_ lotus_dtypes.ConsiderOnlineRetrievalDealsConfigFunc,
		_ lotus_dtypes.ConsiderOfflineRetrievalDealsConfigFunc) lotus_dtypes.RetrievalPricingFunc {
		if cfg.RetrievalPricing.Strategy == lotus_config.RetrievalPricingExternalMode {
			return pricing.ExternalRetrievalPricingFunc(cfg.RetrievalPricing.External.Path)
		}

		return retrievalimpl.DefaultPricingFunc(cfg.RetrievalPricing.Default.VerifiedDealsFreeTransfer)
	}
}

func StorageAuth(ctx helpers.MetricsCtx, ca v0api.Common) (sectorstorage.StorageAuth, error) {
	token, err := ca.AuthNew(ctx, []auth.Permission{"admin"})
	if err != nil {
		return nil, xerrors.Errorf("creating storage auth header: %w", err)
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(token))
	return sectorstorage.StorageAuth(headers), nil
}

func StorageAuthWithURL(apiInfo string) func(ctx helpers.MetricsCtx, ca v0api.Common) (sectorstorage.StorageAuth, error) {
	return func(ctx helpers.MetricsCtx, ca v0api.Common) (sectorstorage.StorageAuth, error) {
		s := strings.Split(apiInfo, ":")
		if len(s) != 2 {
			return nil, errors.New("unexpected format of `apiInfo`")
		}
		headers := http.Header{}
		headers.Add("Authorization", "Bearer "+s[0])
		return sectorstorage.StorageAuth(headers), nil
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
		return xerrors.New("expected address of config.Boost")
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
	dbPath := path.Join(r.Path(), "boost.db")
	return db.SqlDB(dbPath)
}

type LogSqlDB struct {
	db *sql.DB
}

func NewLogsSqlDB(r repo.LockedRepo) (*LogSqlDB, error) {
	dbPath := path.Join(r.Path(), "boost.logs.db")
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

func NewStorageMarketProvider(provAddr address.Address) func(lc fx.Lifecycle, r repo.LockedRepo, h host.Host, a v1api.FullNode,
	sqldb *sql.DB, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager,
	dp *storagemarket.DealPublisher, secb *sectorblocks.SectorBlocks, sps sealingpipeline.API, df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB,
	dagst *dagstore.Wrapper, ps lotus_dtypes.ProviderPieceStore) (*storagemarket.Provider, error) {
	return func(lc fx.Lifecycle, r repo.LockedRepo, h host.Host, a v1api.FullNode, sqldb *sql.DB, dealsDB *db.DealsDB,
		fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storagemarket.DealPublisher, secb *sectorblocks.SectorBlocks, sps sealingpipeline.API,
		df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB,
		dagst *dagstore.Wrapper, ps lotus_dtypes.ProviderPieceStore) (*storagemarket.Provider, error) {

		prov, err := storagemarket.NewProvider(r.Path(), h, sqldb, dealsDB, fundMgr, storageMgr, a, dp, provAddr, secb,
			sps, storagemarket.NewChainDealManager(a), df, logsSqlDB.db, logsDB, dagst, ps)
		lp2pnet := lp2pimpl.NewDealProvider(h, prov)

		if err != nil {
			return nil, err
		}

		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				_, err := prov.Start()
				if err != nil {
					return fmt.Errorf("starting storage provider: %w", err)
				}
				lp2pnet.Start()
				return nil
			},
			OnStop: func(ctx context.Context) error {
				lp2pnet.Stop()
				prov.Stop()
				return nil
			},
		})

		return prov, nil
	}
}

func NewGraphqlServer(lc fx.Lifecycle, prov *storagemarket.Provider, dealsDB *db.DealsDB, logsDB *db.LogsDB, fundMgr *fundmanager.FundManager,
	storageMgr *storagemanager.StorageManager, publisher *storagemarket.DealPublisher, spApi sealingpipeline.API,
	legacyProv lotus_storagemarket.StorageProvider, fullNode v1api.FullNode) *gql.Server {
	resolver := gql.NewResolver(dealsDB, logsDB, fundMgr, storageMgr, spApi, prov, legacyProv, publisher, fullNode)
	server := gql.NewServer(resolver)

	lc.Append(fx.Hook{
		OnStart: server.Serve,
	})

	return server
}
