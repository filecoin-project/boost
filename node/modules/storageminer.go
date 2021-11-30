package modules

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/storagemanager"

	"go.uber.org/fx"
	"go.uber.org/multierr"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/storage/sectorblocks"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/go-address"
	dtimpl "github.com/filecoin-project/go-data-transfer/impl"
	dtnet "github.com/filecoin-project/go-data-transfer/network"
	dtgstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	piecestoreimpl "github.com/filecoin-project/go-fil-markets/piecestore/impl"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	retrievalimpl "github.com/filecoin-project/go-fil-markets/retrievalmarket/impl"
	rmnet "github.com/filecoin-project/go-fil-markets/retrievalmarket/network"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-statestore"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/libp2p/go-libp2p-core/host"

	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/extern/sector-storage/stores"

	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/node/modules/helpers"
	"github.com/filecoin-project/boost/node/repo"
	lapi "github.com/filecoin-project/lotus/api"

	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/filecoin-project/lotus/journal"
	"github.com/filecoin-project/lotus/markets"
	"github.com/filecoin-project/lotus/markets/dagstore"
	marketevents "github.com/filecoin-project/lotus/markets/loggers"
)

var (
	StorageCounterDSPrefix = "/storage/nextid"
	StagingAreaDirName     = "deal-staging"
)

func minerAddrFromDS(ds dtypes.MetadataDS) (address.Address, error) {
	maddrb, err := ds.Get(datastore.NewKey("miner-address"))
	if err != nil {
		return address.Undef, err
	}

	return address.NewFromBytes(maddrb)
}

func MinerAddress(ds dtypes.MetadataDS) (dtypes.MinerAddress, error) {
	ma, err := minerAddrFromDS(ds)
	return dtypes.MinerAddress(ma), err
}

func MinerID(ma dtypes.MinerAddress) (dtypes.MinerID, error) {
	id, err := address.IDFromAddress(address.Address(ma))
	return dtypes.MinerID(id), err
}

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

func HandleMigrateProviderFunds(lc fx.Lifecycle, ds dtypes.MetadataDS, node lapi.FullNode, minerAddress dtypes.MinerAddress) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			b, err := ds.Get(datastore.NewKey("/marketfunds/provider"))
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

			return ds.Delete(datastore.NewKey("/marketfunds/provider"))
		},
	})
}

// NewProviderDAGServiceDataTransfer returns a data transfer manager that just
// uses the provider's Staging DAG service for transfers
func NewProviderDAGServiceDataTransfer(lc fx.Lifecycle, h host.Host, gs dtypes.StagingGraphsync, ds dtypes.MetadataDS, r repo.LockedRepo) (dtypes.ProviderDataTransfer, error) {
	net := dtnet.NewFromLibp2pHost(h)

	dtDs := namespace.Wrap(ds, datastore.NewKey("/datatransfer/provider/transfers"))
	transport := dtgstransport.NewTransport(h.ID(), gs, net)
	err := os.MkdirAll(filepath.Join(r.Path(), "data-transfer"), 0755) //nolint: gosec
	if err != nil && !os.IsExist(err) {
		return nil, err
	}

	dt, err := dtimpl.NewDataTransfer(dtDs, filepath.Join(r.Path(), "data-transfer"), net, transport)
	if err != nil {
		return nil, err
	}

	dt.OnReady(marketevents.ReadyLogger("provider data transfer"))
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			dt.SubscribeToEvents(marketevents.DataTransferLogger)
			return dt.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			return dt.Stop(ctx)
		},
	})
	return dt, nil
}

// NewProviderPieceStore creates a statestore for storing metadata about pieces
// shared by the storage and retrieval providers
func NewProviderPieceStore(lc fx.Lifecycle, ds dtypes.MetadataDS) (dtypes.ProviderPieceStore, error) {
	ps, err := piecestoreimpl.NewPieceStore(namespace.Wrap(ds, datastore.NewKey("/storagemarket")))
	if err != nil {
		return nil, err
	}
	ps.OnReady(marketevents.ReadyLogger("piecestore"))
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return ps.Start(ctx)
		},
	})
	return ps, nil
}

// StagingBlockstore creates a blockstore for staging blocks for a miner
// in a storage deal, prior to sealing
func StagingBlockstore(lc fx.Lifecycle, mctx helpers.MetricsCtx, r repo.LockedRepo) (dtypes.StagingBlockstore, error) {
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

// RetrievalProvider creates a new retrieval provider attached to the provider blockstore
func RetrievalProvider(
	maddr dtypes.MinerAddress,
	adapter retrievalmarket.RetrievalProviderNode,
	sa retrievalmarket.SectorAccessor,
	netwk rmnet.RetrievalMarketNetwork,
	ds dtypes.MetadataDS,
	pieceStore dtypes.ProviderPieceStore,
	dt dtypes.ProviderDataTransfer,
	pricingFnc dtypes.RetrievalPricingFunc,
	userFilter dtypes.RetrievalDealFilter,
	dagStore *dagstore.Wrapper,
) (retrievalmarket.RetrievalProvider, error) {
	opt := retrievalimpl.DealDeciderOpt(retrievalimpl.DealDecider(userFilter))
	return retrievalimpl.NewProvider(
		address.Address(maddr),
		adapter,
		sa,
		netwk,
		pieceStore,
		dagStore,
		dt,
		namespace.Wrap(ds, datastore.NewKey("/retrievals/provider")),
		retrievalimpl.RetrievalPricingFunc(pricingFnc),
		opt,
	)
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

func SectorStorage(mctx helpers.MetricsCtx, lc fx.Lifecycle, lstor *stores.Local, stor *stores.Remote, ls stores.LocalStorage, si stores.SectorIndex, sc sectorstorage.SealerConfig, ds dtypes.MetadataDS) (*sectorstorage.Manager, error) {
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

func NewConsiderOnlineStorageDealsConfigFunc(r repo.LockedRepo) (dtypes.ConsiderOnlineStorageDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderOnlineStorageDeals
		})
		return
	}, nil
}

func NewSetConsideringOnlineStorageDealsFunc(r repo.LockedRepo) (dtypes.SetConsiderOnlineStorageDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderOnlineStorageDeals = b
		})
		return
	}, nil
}

func NewConsiderOnlineRetrievalDealsConfigFunc(r repo.LockedRepo) (dtypes.ConsiderOnlineRetrievalDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderOnlineRetrievalDeals
		})
		return
	}, nil
}

func NewSetConsiderOnlineRetrievalDealsConfigFunc(r repo.LockedRepo) (dtypes.SetConsiderOnlineRetrievalDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderOnlineRetrievalDeals = b
		})
		return
	}, nil
}

func NewStorageDealPieceCidBlocklistConfigFunc(r repo.LockedRepo) (dtypes.StorageDealPieceCidBlocklistConfigFunc, error) {
	return func() (out []cid.Cid, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.PieceCidBlocklist
		})
		return
	}, nil
}

func NewSetStorageDealPieceCidBlocklistConfigFunc(r repo.LockedRepo) (dtypes.SetStorageDealPieceCidBlocklistConfigFunc, error) {
	return func(blocklist []cid.Cid) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.PieceCidBlocklist = blocklist
		})
		return
	}, nil
}

func NewConsiderOfflineStorageDealsConfigFunc(r repo.LockedRepo) (dtypes.ConsiderOfflineStorageDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderOfflineStorageDeals
		})
		return
	}, nil
}

func NewSetConsideringOfflineStorageDealsFunc(r repo.LockedRepo) (dtypes.SetConsiderOfflineStorageDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderOfflineStorageDeals = b
		})
		return
	}, nil
}

func NewConsiderOfflineRetrievalDealsConfigFunc(r repo.LockedRepo) (dtypes.ConsiderOfflineRetrievalDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderOfflineRetrievalDeals
		})
		return
	}, nil
}

func NewSetConsiderOfflineRetrievalDealsConfigFunc(r repo.LockedRepo) (dtypes.SetConsiderOfflineRetrievalDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderOfflineRetrievalDeals = b
		})
		return
	}, nil
}

func NewConsiderVerifiedStorageDealsConfigFunc(r repo.LockedRepo) (dtypes.ConsiderVerifiedStorageDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderVerifiedStorageDeals
		})
		return
	}, nil
}

func NewSetConsideringVerifiedStorageDealsFunc(r repo.LockedRepo) (dtypes.SetConsiderVerifiedStorageDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderVerifiedStorageDeals = b
		})
		return
	}, nil
}

func NewConsiderUnverifiedStorageDealsConfigFunc(r repo.LockedRepo) (dtypes.ConsiderUnverifiedStorageDealsConfigFunc, error) {
	return func() (out bool, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = cfg.Dealmaking.ConsiderUnverifiedStorageDeals
		})
		return
	}, nil
}

func NewSetConsideringUnverifiedStorageDealsFunc(r repo.LockedRepo) (dtypes.SetConsiderUnverifiedStorageDealsConfigFunc, error) {
	return func(b bool) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ConsiderUnverifiedStorageDeals = b
		})
		return
	}, nil
}

func NewSetExpectedSealDurationFunc(r repo.LockedRepo) (dtypes.SetExpectedSealDurationFunc, error) {
	return func(delay time.Duration) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.ExpectedSealDuration = config.Duration(delay)
		})
		return
	}, nil
}

func NewGetExpectedSealDurationFunc(r repo.LockedRepo) (dtypes.GetExpectedSealDurationFunc, error) {
	return func() (out time.Duration, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = time.Duration(cfg.Dealmaking.ExpectedSealDuration)
		})
		return
	}, nil
}

func NewSetMaxDealStartDelayFunc(r repo.LockedRepo) (dtypes.SetMaxDealStartDelayFunc, error) {
	return func(delay time.Duration) (err error) {
		err = mutateCfg(r, func(cfg *config.Boost) {
			cfg.Dealmaking.MaxDealStartDelay = config.Duration(delay)
		})
		return
	}, nil
}

func NewGetMaxDealStartDelayFunc(r repo.LockedRepo) (dtypes.GetMaxDealStartDelayFunc, error) {
	return func() (out time.Duration, err error) {
		err = readCfg(r, func(cfg *config.Boost) {
			out = time.Duration(cfg.Dealmaking.MaxDealStartDelay)
		})
		return
	}, nil
}

func readCfg(r repo.LockedRepo, accessor func(*config.Boost)) error {
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

func mutateCfg(r repo.LockedRepo, mutator func(*config.Boost)) error {
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

func NewBoostDB(r repo.LockedRepo) (*sql.DB, error) {
	dbPath := path.Join(r.Path(), "boost.db")
	return db.SqlDB(dbPath)
}

func NewDealsDB(sqldb *sql.DB) *db.DealsDB {
	return db.NewDealsDB(sqldb)
}

func NewStorageMarketProvider(provAddr address.Address) func(lc fx.Lifecycle, r repo.LockedRepo, a v1api.FullNode, sqldb *sql.DB, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storagemarket.DealPublisher, secb *sectorblocks.SectorBlocks) (*storagemarket.Provider, error) {
	return func(lc fx.Lifecycle, r repo.LockedRepo, a v1api.FullNode, sqldb *sql.DB, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storagemarket.DealPublisher, secb *sectorblocks.SectorBlocks) (*storagemarket.Provider, error) {
		prov, err := storagemarket.NewProvider(r.Path(), sqldb, dealsDB, fundMgr, storageMgr, a, dp, provAddr, secb)

		if err != nil {
			return nil, err
		}

		lc.Append(fx.Hook{OnStart: prov.Start})

		return prov, nil
	}
}

func NewGraphqlServer(lc fx.Lifecycle, prov *storagemarket.Provider, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, publisher *storagemarket.DealPublisher, fullNode v1api.FullNode) *gql.Server {
	resolver := gql.NewResolver(dealsDB, fundMgr, prov, publisher, fullNode)
	server := gql.NewServer(resolver)

	lc.Append(fx.Hook{
		OnStart: server.Serve,
	})

	return server
}
