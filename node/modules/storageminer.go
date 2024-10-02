package modules

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/filecoin-project/boost/cmd/lib"
	dtnet "github.com/filecoin-project/boost/datatransfer/network"
	dtgstransport "github.com/filecoin-project/boost/datatransfer/transport/graphsync"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/tracing"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/boost/lib/legacy"
	"github.com/filecoin-project/boost/lib/mpoolmonitor"
	"github.com/filecoin-project/boost/markets/sectoraccessor"
	"github.com/filecoin-project/boost/markets/storageadapter"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/impl/backupmgr"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/retrievalmarket/server"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/lp2pimpl"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/storedask"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/go-address"
	vfsm "github.com/filecoin-project/go-ds-versioning/pkg/fsm"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v9/account"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-statemachine/fsm"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/build"
	ctypes "github.com/filecoin-project/lotus/chain/types"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/gateway"
	"github.com/filecoin-project/lotus/lib/backupds"
	"github.com/filecoin-project/lotus/lib/sigs"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/filecoin-project/lotus/node/repo"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/fx"
	"go.uber.org/multierr"
)

var (
	StorageCounterDSPrefix = "/storage/nextid"
)

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

func NewBoostDB(r lotus_repo.LockedRepo) (*sql.DB, error) {
	// fixes error "database is locked", caused by concurrent access from deal goroutines to a single sqlite3 db connection
	// see: https://github.com/mattn/go-sqlite3#:~:text=Error%3A%20database%20is%20locked
	dbPath := path.Join(r.Path(), db.DealsDBName+"?cache=shared")
	return db.SqlDB(dbPath)
}

type LogSqlDB struct {
	db *sql.DB
}

func NewLogsSqlDB(r repo.LockedRepo) (*LogSqlDB, error) {
	// fixes error "database is locked", caused by concurrent access from deal goroutines to a single sqlite3 db connection
	// see: https://github.com/mattn/go-sqlite3#:~:text=Error%3A%20database%20is%20locked
	dbPath := path.Join(r.Path(), db.LogsDBName+"?cache=shared")
	d, err := db.SqlDB(dbPath)
	if err != nil {
		return nil, err
	}
	return &LogSqlDB{d}, nil
}

func NewDealsDB(sqldb *sql.DB) *db.DealsDB {
	return db.NewDealsDB(sqldb)
}

func NewDirectDealsDB(sqldb *sql.DB) *db.DirectDealsDB {
	return db.NewDirectDealsDB(sqldb)
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

func HandleQueryAsk(lc fx.Lifecycle, h host.Host, maddr lotus_dtypes.MinerAddress, pd *piecedirectory.PieceDirectory, sa *lib.MultiMinerAccessor, askStore server.RetrievalAskGetter, full v1api.FullNode) {
	handler := server.NewQueryAskHandler(h, address.Address(maddr), pd, sa, askStore, full)
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			handler.Start()
			return nil
		},
		OnStop: func(context.Context) error {
			handler.Stop()
			return nil
		},
	})
}

func NewSectorStateDB(sqldb *sql.DB) *db.SectorStateDB {
	return db.NewSectorStateDB(sqldb)
}

func HandleBoostLibp2pDeals(cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, prov *storagemarket.Provider, directProv *storagemarket.DirectDealsProvider, a v1api.FullNode, idxProv *indexprovider.Wrapper, plDB *db.ProposalLogsDB, spApi sealingpipeline.API) {
	return func(lc fx.Lifecycle, h host.Host, prov *storagemarket.Provider, directProv *storagemarket.DirectDealsProvider, a v1api.FullNode, idxProv *indexprovider.Wrapper, plDB *db.ProposalLogsDB, spApi sealingpipeline.API) {

		lp2pnet := lp2pimpl.NewDealProvider(h, prov, a, plDB, spApi)

		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				// Start the Boost SP
				log.Info("starting boost storage provider")
				err := prov.Start()
				if err != nil {
					return fmt.Errorf("starting storage provider: %w", err)
				}
				err = directProv.Start(ctx)
				if err != nil {
					return fmt.Errorf("starting direct deals provider: %w", err)
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
				idxProv.Stop()
				return nil
			},
		})
	}
}

func HandleContractDeals(c *config.ContractDealsConfig) func(mctx helpers.MetricsCtx, lc fx.Lifecycle, prov *storagemarket.Provider, a v1api.FullNode, subCh *gateway.EthSubHandler, maddr lotus_dtypes.MinerAddress) {
	return func(mctx helpers.MetricsCtx, lc fx.Lifecycle, prov *storagemarket.Provider, a v1api.FullNode, subCh *gateway.EthSubHandler, maddr lotus_dtypes.MinerAddress) {
		if !c.Enabled {
			log.Info("Contract deals monitor is currently disabled. Update config.toml if you want to enable it.")
			return
		}

		monitor := storagemarket.NewContractDealMonitor(prov, a, subCh, c, address.Address(maddr))

		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				log.Info("contract deals monitor starting")

				go func() {
					err := monitor.Start(ctx)
					if err != nil {
						log.Errorw("contract deals monitor erred", "err", err)
						return
					}

					log.Info("contract deals monitor started")
				}()

				return nil
			},
			OnStop: func(ctx context.Context) error {
				err := monitor.Stop()
				if err != nil {
					return err
				}
				return nil
			},
		})
	}
}

type signatureVerifier struct {
	fn v1api.FullNode
}

func (s *signatureVerifier) VerifySignature(ctx context.Context, sig crypto.Signature, addr address.Address, input []byte) (bool, error) {
	addr, err := s.fn.StateAccountKey(ctx, addr, ctypes.EmptyTSK)
	if err != nil {
		return false, err
	}

	// Check if the client is an f4 address, ie an FVM contract
	clientAddr := addr.String()
	if len(clientAddr) >= 2 && (clientAddr[:2] == "t4" || clientAddr[:2] == "f4") {
		// Verify authorization by simulating an AuthenticateMessage
		return s.verifyContractSignature(ctx, sig, addr, input)
	}

	// Otherwise do local signature verification
	err = sigs.Verify(&sig, addr, input)
	return err == nil, err
}

// verifyContractSignature simulates sending an AuthenticateMessage to authenticate the signer
func (s *signatureVerifier) verifyContractSignature(ctx context.Context, sig crypto.Signature, addr address.Address, input []byte) (bool, error) {
	var params account.AuthenticateMessageParams
	params.Message = input
	params.Signature = sig.Data

	var msg ltypes.Message
	buf := new(bytes.Buffer)

	var err error
	err = params.MarshalCBOR(buf)
	if err != nil {
		return false, err
	}
	msg.Params = buf.Bytes()

	msg.From = builtin.StorageMarketActorAddr
	msg.To = addr
	msg.Nonce = 1

	msg.Method, err = builtin.GenerateFRCMethodNum("AuthenticateMessage") // abi.MethodNum(2643134072)
	if err != nil {
		return false, err
	}

	res, err := s.fn.StateCall(ctx, &msg, ltypes.EmptyTSK)
	if err != nil {
		return false, fmt.Errorf("state call to %s returned an error: %w", addr, err)
	}

	return res.MsgRct.ExitCode == exitcode.Ok, nil
}

func NewChainDealManager(a v1api.FullNode) *storagemarket.ChainDealManager {
	cdmCfg := storagemarket.ChainDealManagerCfg{PublishDealsConfidence: 2 * build.MessageConfidence}
	return storagemarket.NewChainDealManager(a, cdmCfg)
}

func NewLegacyDealsManager(lc fx.Lifecycle, legacyFSM fsm.Group) legacy.LegacyDealManager {
	ctx, cancel := context.WithCancel(context.Background())
	mgr := legacy.NewLegacyDealsManager(legacyFSM)
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			go mgr.Run(ctx)
			return nil
		},
		OnStop: func(_ context.Context) error {
			cancel()
			return nil
		},
	})
	return mgr
}

func NewStorageMarketProvider(provAddr address.Address, cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, a v1api.FullNode, sqldb *sql.DB, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, sask storedask.StoredAsk, dp *storageadapter.DealPublisher, secb types.PieceAdder, commpc types.CommpCalculator, commpt storagemarket.CommpThrottle, sps sealingpipeline.API, df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB, piecedirectory *piecedirectory.PieceDirectory, ip *indexprovider.Wrapper, cdm *storagemarket.ChainDealManager) (*storagemarket.Provider, error) {
	return func(lc fx.Lifecycle, h host.Host, a v1api.FullNode, sqldb *sql.DB, dealsDB *db.DealsDB,
		fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, sask storedask.StoredAsk, dp *storageadapter.DealPublisher, secb types.PieceAdder,
		commpc types.CommpCalculator, commpt storagemarket.CommpThrottle, sps sealingpipeline.API,
		df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB,
		piecedirectory *piecedirectory.PieceDirectory, ip *indexprovider.Wrapper, cdm *storagemarket.ChainDealManager) (*storagemarket.Provider, error) {

		prvCfg := storagemarket.Config{
			MaxTransferDuration: time.Duration(cfg.Dealmaking.MaxTransferDuration),
			RemoteCommp:         cfg.Dealmaking.RemoteCommp,
			TransferLimiter: storagemarket.TransferLimiterConfig{
				MaxConcurrent:    cfg.HttpDownload.HttpTransferMaxConcurrentDownloads,
				StallCheckPeriod: time.Duration(cfg.HttpDownload.HttpTransferStallCheckPeriod),
				StallTimeout:     time.Duration(cfg.HttpDownload.HttpTransferStallTimeout),
			},
			DealLogDurationDays:         cfg.Dealmaking.DealLogDurationDays,
			StorageFilter:               cfg.Dealmaking.Filter,
			SealingPipelineCacheTimeout: time.Duration(cfg.Dealmaking.SealingPipelineCacheTimeout),
			CurioMigration:              cfg.CurioMigration.Enable,
		}
		dl := logs.NewDealLogger(logsDB)
		tspt := httptransport.New(h, dl, httptransport.NChunksOpt(cfg.HttpDownload.NChunks), httptransport.AllowPrivateIPsOpt(cfg.HttpDownload.AllowPrivateIPs))
		prov, err := storagemarket.NewProvider(prvCfg, sqldb, dealsDB, fundMgr, storageMgr, a, dp, provAddr, secb, commpc, commpt,
			sps, cdm, df, logsSqlDB.db, logsDB, piecedirectory, ip, sask, &signatureVerifier{a}, dl, tspt)
		if err != nil {
			return nil, err
		}

		return prov, nil
	}
}

func NewCommpThrottle(cfg *config.Boost) func() storagemarket.CommpThrottle {
	return func() storagemarket.CommpThrottle {
		size := uint64(1)
		if cfg.Dealmaking.MaxConcurrentLocalCommp > 1 {
			size = cfg.Dealmaking.MaxConcurrentLocalCommp
		}
		return make(chan struct{}, size)
	}
}

// Use a caching sector accessor
func NewSectorAccessor(cfg *config.Boost) sectoraccessor.SectorAccessorConstructor {
	// The cache just holds booleans, so there's no harm in using a big number
	// for cache size
	const maxCacheSize = 4096
	return sectoraccessor.NewCachingSectorAccessor(maxCacheSize, time.Duration(cfg.Dealmaking.IsUnsealedCacheExpiry))
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

func NewOnlineBackupMgr(cfg *config.Boost) func(lc fx.Lifecycle, r lotus_repo.LockedRepo, ds lotus_dtypes.MetadataDS, dealsDB *sql.DB) *backupmgr.BackupMgr {
	return func(lc fx.Lifecycle, r lotus_repo.LockedRepo, ds lotus_dtypes.MetadataDS, dealsDB *sql.DB) *backupmgr.BackupMgr {
		return backupmgr.NewBackupMgr(r, ds, db.DealsDBName, dealsDB)
	}
}

// NewProviderTransferNetwork sets up the libp2p protocol networking for data transfer
func NewProviderTransferNetwork(h host.Host) dtypes.ProviderTransferNetwork {
	// Leave it up to the client to reconnect
	return dtnet.NewFromLibp2pHost(h, dtnet.RetryParameters(0, 0, 0, 0))
}

// NewProviderTransport sets up a data transfer transport over graphsync
func NewProviderTransport(h host.Host, gs dtypes.StagingGraphsync) dtypes.ProviderTransport {
	return dtgstransport.NewTransport(h.ID(), gs)
}

func NewMpoolMonitor(cfg *config.Boost) func(lc fx.Lifecycle, a v1api.FullNode) *mpoolmonitor.MpoolMonitor {
	return func(lc fx.Lifecycle, a v1api.FullNode) *mpoolmonitor.MpoolMonitor {
		mpm := mpoolmonitor.NewMonitor(a, cfg.Monitoring.MpoolAlertEpochs)

		lc.Append(fx.Hook{
			OnStart: mpm.Start,
			OnStop:  mpm.Stop,
		})

		return mpm
	}
}

func NewLegacyDealsFSM(cfg *config.Boost) func(lc fx.Lifecycle, mds lotus_dtypes.MetadataDS) (fsm.Group, error) {
	return func(lc fx.Lifecycle, mds lotus_dtypes.MetadataDS) (fsm.Group, error) {
		// Get the deals FSM
		bds, err := backupds.Wrap(mds, "")
		if err != nil {
			return nil, fmt.Errorf("opening backupds: %w", err)
		}
		provDS := namespace.Wrap(bds, datastore.NewKey("/deals/provider"))
		deals, migrate, err := vfsm.NewVersionedFSM(provDS, fsm.Parameters{
			StateType:     legacytypes.MinerDeal{},
			StateKeyField: "State",
		}, nil, "2")
		if err != nil {
			return nil, fmt.Errorf("reading legacy deals from datastore: %w", err)
		}
		ctx := context.Background()

		err = migrate(ctx)
		if err != nil {
			return nil, fmt.Errorf("running provider fsm migration script: %w", err)
		}

		return deals, err
	}
}
