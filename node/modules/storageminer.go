package modules

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	retrievalimpl "github.com/filecoin-project/boost-gfm/retrievalmarket/impl"
	rmnet "github.com/filecoin-project/boost-gfm/retrievalmarket/network"
	"github.com/filecoin-project/boost/markets/pricing"
	"github.com/filecoin-project/go-state-types/big"
	"path"
	"time"

	piecestoreimpl "github.com/filecoin-project/boost-gfm/piecestore/impl"
	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/boost-gfm/shared"
	gfm_storagemarket "github.com/filecoin-project/boost-gfm/storagemarket"
	storageimpl "github.com/filecoin-project/boost-gfm/storagemarket/impl"
	"github.com/filecoin-project/boost-gfm/storagemarket/impl/storedask"
	"github.com/filecoin-project/boost-gfm/stores"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/gql"
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/boost/markets/idxprov"
	marketevents "github.com/filecoin-project/boost/markets/loggers"
	"github.com/filecoin-project/boost/markets/sectoraccessor"
	"github.com/filecoin-project/boost/markets/storageadapter"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/impl/backupmgr"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	brm "github.com/filecoin-project/boost/retrievalmarket/lib"
	"github.com/filecoin-project/boost/retrievalmarket/rtvllog"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/lp2pimpl"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/dagstore/indexbs"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/go-address"
	dtnet "github.com/filecoin-project/go-data-transfer/network"
	dtgstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	lotus_gfm_shared "github.com/filecoin-project/go-fil-markets/shared"
	lotus_gfm_storagemarket "github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v8/verifreg"
	"github.com/filecoin-project/go-state-types/builtin/v9/account"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/build"
	ctypes "github.com/filecoin-project/lotus/chain/types"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/gateway"
	"github.com/filecoin-project/lotus/lib/sigs"
	mdagstore "github.com/filecoin-project/lotus/markets/dagstore"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/filecoin-project/lotus/node/repo"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/sectorblocks"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/metadata"
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/fx"
	"go.uber.org/multierr"
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

func NewLogsDB(logsSqlDB *LogSqlDB) *db.LogsDB {
	return db.NewLogsDB(logsSqlDB.db)
}

func NewProposalLogsDB(sqldb *sql.DB) *db.ProposalLogsDB {
	return db.NewProposalLogsDB(sqldb)
}

func NewFundsDB(sqldb *sql.DB) *db.FundsDB {
	return db.NewFundsDB(sqldb)
}

func HandleRetrieval(host host.Host, lc fx.Lifecycle, m retrievalmarket.RetrievalProvider) {
	m.OnReady(marketevents.ReadyLogger("retrieval provider"))
	lc.Append(fx.Hook{

		OnStart: func(ctx context.Context) error {
			m.SubscribeToEvents(marketevents.RetrievalProviderLogger)
			return m.Start(ctx)
		},
		OnStop: func(context.Context) error {
			return m.Stop()
		},
	})
}

func NewSectorStateDB(sqldb *sql.DB) *db.SectorStateDB {
	return db.NewSectorStateDB(sqldb)
}

func HandleLegacyDeals(mctx helpers.MetricsCtx, lc fx.Lifecycle, lsp gfm_storagemarket.StorageProvider) error {
	log.Info("starting legacy storage provider")
	ctx := helpers.LifecycleCtx(mctx, lc)
	lsp.OnReady(marketevents.ReadyLogger("storage provider"))
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			lsp.SubscribeToEvents(marketevents.StorageProviderLogger)
			return lsp.Start(ctx)
		},
		OnStop: func(context.Context) error {
			return lsp.Stop()
		},
	})
	return nil
}

func HandleBoostLibp2pDeals(lc fx.Lifecycle, h host.Host, prov *storagemarket.Provider, a v1api.FullNode, legacySP gfm_storagemarket.StorageProvider, idxProv *indexprovider.Wrapper, plDB *db.ProposalLogsDB, spApi sealingpipeline.API) {
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
			idxProv.Stop()
			return nil
		},
	})
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

func NewStorageAsk(ctx helpers.MetricsCtx, fapi v1api.FullNode, ds lotus_dtypes.MetadataDS, minerAddress lotus_dtypes.MinerAddress, spn gfm_storagemarket.StorageProviderNode) (*storedask.StoredAsk, error) {
	mi, err := fapi.StateMinerInfo(ctx, address.Address(minerAddress), ltypes.EmptyTSK)
	if err != nil {
		return nil, err
	}

	providerDs := namespace.Wrap(ds, datastore.NewKey("/deals/provider"))
	// legacy this was mistake where this key was place -- so we move the legacy key if need be
	err = shared.MoveKey(providerDs, "/latest-ask", "/storage-ask/latest")
	if err != nil {
		return nil, err
	}
	return storedask.NewStoredAsk(namespace.Wrap(providerDs, datastore.NewKey("/storage-ask")), datastore.NewKey("latest"), spn, address.Address(minerAddress),
		gfm_storagemarket.MaxPieceSize(abi.PaddedPieceSize(mi.SectorSize)))
}

// NewLegacyStorageProvider wraps lotus's storage provider function but additionally sets up the metadata announcement
// for legacy deals based off of Boost's configured protocols
func NewLegacyStorageProvider(cfg *config.Boost) func(minerAddress lotus_dtypes.MinerAddress,
	storedAsk *storedask.StoredAsk,
	h host.Host, ds lotus_dtypes.MetadataDS,
	r repo.LockedRepo,
	pieceStore dtypes.ProviderPieceStore,
	indexer provider.Interface,
	dataTransfer dtypes.ProviderDataTransfer,
	spn gfm_storagemarket.StorageProviderNode,
	df storageimpl.DealDeciderFunc,
	dsw stores.DAGStoreWrapper,
	meshCreator idxprov.MeshCreator,
) (gfm_storagemarket.StorageProvider, error) {
	return func(minerAddress lotus_dtypes.MinerAddress,
		storedAsk *storedask.StoredAsk,
		h host.Host, ds lotus_dtypes.MetadataDS,
		r repo.LockedRepo,
		pieceStore dtypes.ProviderPieceStore,
		indexer provider.Interface,
		dataTransfer dtypes.ProviderDataTransfer,
		spn gfm_storagemarket.StorageProviderNode,
		df storageimpl.DealDeciderFunc,
		dsw stores.DAGStoreWrapper,
		meshCreator idxprov.MeshCreator,
	) (gfm_storagemarket.StorageProvider, error) {
		prov, err := StorageProvider(minerAddress, storedAsk, h, ds, r, pieceStore, indexer, dataTransfer, spn, df, dsw, meshCreator)
		if err != nil {
			return prov, err
		}
		p := prov.(*storageimpl.Provider)
		p.Configure(storageimpl.CustomMetadataGenerator(func(deal gfm_storagemarket.MinerDeal) metadata.Metadata {

			// Announce deal to network Indexer
			protocols := []metadata.Protocol{
				&metadata.GraphsyncFilecoinV1{
					PieceCID:      deal.Proposal.PieceCID,
					FastRetrieval: deal.FastRetrieval,
					VerifiedDeal:  deal.Proposal.VerifiedDeal,
				},
			}

			return metadata.Default.New(protocols...)

		}))
		return p, nil
	}
}

func NewStorageMarketProvider(provAddr address.Address, cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, a v1api.FullNode, sqldb *sql.DB, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storageadapter.DealPublisher, secb *sectorblocks.SectorBlocks, commpc types.CommpCalculator, sps sealingpipeline.API, df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB, dagst *mdagstore.Wrapper, ps dtypes.ProviderPieceStore, ip *indexprovider.Wrapper, lp gfm_storagemarket.StorageProvider, cdm *storagemarket.ChainDealManager) (*storagemarket.Provider, error) {
	return func(lc fx.Lifecycle, h host.Host, a v1api.FullNode, sqldb *sql.DB, dealsDB *db.DealsDB,
		fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storageadapter.DealPublisher, secb *sectorblocks.SectorBlocks,
		commpc types.CommpCalculator, sps sealingpipeline.API,
		df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB,
		dagst *mdagstore.Wrapper, ps dtypes.ProviderPieceStore, ip *indexprovider.Wrapper,
		lp gfm_storagemarket.StorageProvider, cdm *storagemarket.ChainDealManager) (*storagemarket.Provider, error) {

		prvCfg := storagemarket.Config{
			MaxTransferDuration:     time.Duration(cfg.Dealmaking.MaxTransferDuration),
			RemoteCommp:             cfg.Dealmaking.RemoteCommp,
			MaxConcurrentLocalCommp: cfg.Dealmaking.MaxConcurrentLocalCommp,
			TransferLimiter: storagemarket.TransferLimiterConfig{
				MaxConcurrent:    cfg.Dealmaking.HttpTransferMaxConcurrentDownloads,
				StallCheckPeriod: time.Duration(cfg.Dealmaking.HttpTransferStallCheckPeriod),
				StallTimeout:     time.Duration(cfg.Dealmaking.HttpTransferStallTimeout),
			},
			DealLogDurationDays:         cfg.Dealmaking.DealLogDurationDays,
			StorageFilter:               cfg.Dealmaking.Filter,
			SealingPipelineCacheTimeout: time.Duration(cfg.Dealmaking.SealingPipelineCacheTimeout),
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

func NewGraphqlServer(cfg *config.Boost) func(lc fx.Lifecycle, r repo.LockedRepo, h host.Host, prov *storagemarket.Provider, dealsDB *db.DealsDB, logsDB *db.LogsDB, retDB *rtvllog.RetrievalLogDB, plDB *db.ProposalLogsDB, fundsDB *db.FundsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, publisher *storageadapter.DealPublisher, spApi sealingpipeline.API, legacyProv gfm_storagemarket.StorageProvider, legacyDT dtypes.ProviderDataTransfer, ps dtypes.ProviderPieceStore, sa retrievalmarket.SectorAccessor, dagst dagstore.Interface, fullNode v1api.FullNode, bg gql.BlockGetter) *gql.Server {
	return func(lc fx.Lifecycle, r repo.LockedRepo, h host.Host, prov *storagemarket.Provider, dealsDB *db.DealsDB, logsDB *db.LogsDB, retDB *rtvllog.RetrievalLogDB, plDB *db.ProposalLogsDB, fundsDB *db.FundsDB, fundMgr *fundmanager.FundManager,
		storageMgr *storagemanager.StorageManager, publisher *storageadapter.DealPublisher, spApi sealingpipeline.API,
		legacyProv gfm_storagemarket.StorageProvider, legacyDT dtypes.ProviderDataTransfer,
		ps dtypes.ProviderPieceStore, sa retrievalmarket.SectorAccessor, dagst dagstore.Interface,
		fullNode v1api.FullNode, bg gql.BlockGetter) *gql.Server {

		resolver := gql.NewResolver(cfg, r, h, dealsDB, logsDB, retDB, plDB, fundsDB, fundMgr, storageMgr, spApi, prov, legacyProv, legacyDT, ps, sa, dagst, publisher, fullNode)
		server := gql.NewServer(resolver, bg)

		lc.Append(fx.Hook{
			OnStart: server.Start,
			OnStop:  server.Stop,
		})

		return server
	}
}

// Use a caching sector accessor
func NewSectorAccessor(cfg *config.Boost) sectoraccessor.SectorAccessorConstructor {
	// The cache just holds booleans, so there's no harm in using a big number
	// for cache size
	const maxCacheSize = 4096
	return sectoraccessor.NewCachingSectorAccessor(maxCacheSize, time.Duration(cfg.Dealmaking.IsUnsealedCacheExpiry))
}

// ShardSelector helps to resolve a circular dependency:
// The IndexBackedBlockstore has a shard selector, which needs to query the
// RetrievalProviderNode's ask to find out if it's free to retrieve a
// particular piece.
// However the RetrievalProviderNode depends on the DAGStore which depends on
// IndexBackedBlockstore.
// So we
//   - create a ShardSelector that has no dependencies with a default shard
//     selection function that just selects no shards
//   - later call SetShardSelectorFunc to create a real shard selector function
//     with all its dependencies, and set it on the ShardSelector object.
type ShardSelector struct {
	Proxy  indexbs.ShardSelectorF
	Target indexbs.ShardSelectorF
}

func NewShardSelector() *ShardSelector {
	ss := &ShardSelector{
		// The default target function always selects no shards
		Target: func(c cid.Cid, shards []shard.Key) (shard.Key, error) {
			return shard.Key{}, indexbs.ErrNoShardSelected
		},
	}
	ss.Proxy = func(c cid.Cid, shards []shard.Key) (shard.Key, error) {
		return ss.Target(c, shards)
	}

	return ss
}

func SetShardSelectorFunc(lc fx.Lifecycle, shardSelector *ShardSelector, ps dtypes.ProviderPieceStore, sa retrievalmarket.SectorAccessor, rp retrievalmarket.RetrievalProvider) error {
	ctx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			cancel()
			return nil
		},
	})

	ss, err := brm.NewShardSelector(ctx, ps, sa, rp)
	if err != nil {
		return fmt.Errorf("creating shard selector: %w", err)
	}

	shardSelector.Target = ss.ShardSelectorF

	return nil
}

func NewIndexBackedBlockstore(cfg *config.Boost) func(lc fx.Lifecycle, dagst dagstore.Interface, ss *ShardSelector) (dtypes.IndexBackedBlockstore, error) {
	return func(lc fx.Lifecycle, dagst dagstore.Interface, ss *ShardSelector) (dtypes.IndexBackedBlockstore, error) {
		ctx, cancel := context.WithCancel(context.Background())
		lc.Append(fx.Hook{
			OnStop: func(ctx context.Context) error {
				cancel()
				return nil
			},
		})

		ibsds := brm.NewIndexBackedBlockstoreDagstore(dagst)
		rbs, err := indexbs.NewIndexBackedBlockstore(ctx, ibsds, ss.Proxy, cfg.Dealmaking.BlockstoreCacheMaxShards, time.Duration(cfg.Dealmaking.BlockstoreCacheExpiry))
		if err != nil {
			return nil, fmt.Errorf("failed to create index backed blockstore: %w", err)
		}
		return dtypes.IndexBackedBlockstore(rbs), nil
	}
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

// NewProviderPieceStore creates a statestore for storing metadata about pieces
// shared by the storage and retrieval providers
func NewProviderPieceStore(lc fx.Lifecycle, ds lotus_dtypes.MetadataDS) (dtypes.ProviderPieceStore, error) {
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

func RetrievalNetwork(h host.Host) rmnet.RetrievalMarketNetwork {
	return rmnet.NewFromLibp2pHost(h)
}

// RetrievalPricingFunc configures the pricing function to use for retrieval deals.
func RetrievalPricingFunc(cfg config.DealmakingConfig) func(_ dtypes.ConsiderOnlineRetrievalDealsConfigFunc,
	_ dtypes.ConsiderOfflineRetrievalDealsConfigFunc) dtypes.RetrievalPricingFunc {

	return func(_ dtypes.ConsiderOnlineRetrievalDealsConfigFunc,
		_ dtypes.ConsiderOfflineRetrievalDealsConfigFunc) dtypes.RetrievalPricingFunc {
		if cfg.RetrievalPricing.Strategy == config.RetrievalPricingExternalMode {
			return pricing.ExternalRetrievalPricingFunc(cfg.RetrievalPricing.External.Path)
		}

		return retrievalimpl.DefaultPricingFunc(cfg.RetrievalPricing.Default.VerifiedDealsFreeTransfer)
	}
}

// RetrievalProvider creates a new retrieval provider attached to the provider blockstore
func RetrievalProvider(
	maddr lotus_dtypes.MinerAddress,
	adapter retrievalmarket.RetrievalProviderNode,
	sa retrievalmarket.SectorAccessor,
	netwk rmnet.RetrievalMarketNetwork,
	ds lotus_dtypes.MetadataDS,
	pieceStore dtypes.ProviderPieceStore,
	dt dtypes.ProviderDataTransfer,
	pricingFnc dtypes.RetrievalPricingFunc,
	userFilter dtypes.RetrievalDealFilter,
	dagStore stores.DAGStoreWrapper,
) (retrievalmarket.RetrievalProvider, error) {
	opt := retrievalimpl.DealDeciderOpt(retrievalimpl.DealDecider(userFilter))

	retrievalmarket.DefaultPricePerByte = big.Zero() // todo: for whatever reason this is a global var in markets

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

func LotusGFMStorageProviderNode(spn gfm_storagemarket.StorageProviderNode) lotus_gfm_storagemarket.StorageProviderNode {
	return &lotusGFMSPN{StorageProviderNode: spn}
}

type lotusGFMSPN struct {
	gfm_storagemarket.StorageProviderNode
}

func (l *lotusGFMSPN) GetChainHead(ctx context.Context) (lotus_gfm_shared.TipSetToken, abi.ChainEpoch, error) {
	tst, ce, err := l.StorageProviderNode.GetChainHead(ctx)
	return lotus_gfm_shared.TipSetToken(tst), ce, err
}

func (l *lotusGFMSPN) GetBalance(ctx context.Context, addr address.Address, tok lotus_gfm_shared.TipSetToken) (lotus_gfm_storagemarket.Balance, error) {
	//TODO implement me
	panic("implement me")
}

func (l *lotusGFMSPN) VerifySignature(ctx context.Context, signature crypto.Signature, signer address.Address, plaintext []byte, tok lotus_gfm_shared.TipSetToken) (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (l *lotusGFMSPN) OnDealSectorPreCommitted(ctx context.Context, provider address.Address, dealID abi.DealID, proposal market.DealProposal, publishCid *cid.Cid, cb lotus_gfm_storagemarket.DealSectorPreCommittedCallback) error {
	//TODO implement me
	panic("implement me")
}

func (l *lotusGFMSPN) OnDealSectorCommitted(ctx context.Context, provider address.Address, dealID abi.DealID, sectorNumber abi.SectorNumber, proposal market.DealProposal, publishCid *cid.Cid, cb lotus_gfm_storagemarket.DealSectorCommittedCallback) error {
	//TODO implement me
	panic("implement me")
}

func (l *lotusGFMSPN) OnDealExpiredOrSlashed(ctx context.Context, dealID abi.DealID, onDealExpired lotus_gfm_storagemarket.DealExpiredCallback, onDealSlashed lotus_gfm_storagemarket.DealSlashedCallback) error {
	//TODO implement me
	panic("implement me")
}

func (l *lotusGFMSPN) PublishDeals(ctx context.Context, deal lotus_gfm_storagemarket.MinerDeal) (cid.Cid, error) {
	//TODO implement me
	panic("implement me")
}

func (l *lotusGFMSPN) WaitForPublishDeals(ctx context.Context, mcid cid.Cid, proposal market.DealProposal) (*lotus_gfm_storagemarket.PublishDealsWaitResult, error) {
	//TODO implement me
	panic("implement me")
}

func (l *lotusGFMSPN) OnDealComplete(ctx context.Context, deal lotus_gfm_storagemarket.MinerDeal, pieceSize abi.UnpaddedPieceSize, pieceReader lotus_gfm_shared.ReadSeekStarter) (*lotus_gfm_storagemarket.PackingResult, error) {
	//TODO implement me
	panic("implement me")
}

func (l *lotusGFMSPN) GetMinerWorkerAddress(ctx context.Context, addr address.Address, tok lotus_gfm_shared.TipSetToken) (address.Address, error) {
	//TODO implement me
	panic("implement me")
}

func (l *lotusGFMSPN) GetDataCap(ctx context.Context, addr address.Address, tok lotus_gfm_shared.TipSetToken) (*verifreg.DataCap, error) {
	//TODO implement me
	panic("implement me")
}

func (l *lotusGFMSPN) GetProofType(ctx context.Context, addr address.Address, tok lotus_gfm_shared.TipSetToken) (abi.RegisteredSealProof, error) {
	//TODO implement me
	panic("implement me")
}
