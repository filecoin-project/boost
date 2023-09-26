package modules

import (
	"database/sql"

	gfm_storagemarket "github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/boost/markets/storageadapter"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/storage/sectorblocks"
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/fx"
)

func NewDirectDealsProvider(provAddr address.Address, cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, fullnodeApi v1api.FullNode, sqldb *sql.DB, directDealsDB *db.DirectDealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storageadapter.DealPublisher, secb *sectorblocks.SectorBlocks, commpc types.CommpCalculator, commpt storagemarket.CommpThrottle, sps sealingpipeline.API, df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB, piecedirectory *piecedirectory.PieceDirectory, ip *indexprovider.Wrapper, lp gfm_storagemarket.StorageProvider, cdm *storagemarket.ChainDealManager) (*storagemarket.DirectDealsProvider, error) {
	return func(lc fx.Lifecycle, h host.Host, fullnodeApi v1api.FullNode, sqldb *sql.DB, directDealsDB *db.DirectDealsDB,
		fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storageadapter.DealPublisher, secb *sectorblocks.SectorBlocks,
		commpc types.CommpCalculator, commpt storagemarket.CommpThrottle, sps sealingpipeline.API,
		df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB,
		piecedirectory *piecedirectory.PieceDirectory, ip *indexprovider.Wrapper,
		lp gfm_storagemarket.StorageProvider, cdm *storagemarket.ChainDealManager) (*storagemarket.DirectDealsProvider, error) {

		//prvCfg := storagemarket.Config{
		//MaxTransferDuration:     time.Duration(cfg.Dealmaking.MaxTransferDuration),
		//RemoteCommp:             cfg.Dealmaking.RemoteCommp,
		//MaxConcurrentLocalCommp: cfg.Dealmaking.MaxConcurrentLocalCommp,
		//TransferLimiter: storagemarket.TransferLimiterConfig{
		//MaxConcurrent:    cfg.Dealmaking.HttpTransferMaxConcurrentDownloads,
		//StallCheckPeriod: time.Duration(cfg.Dealmaking.HttpTransferStallCheckPeriod),
		//StallTimeout:     time.Duration(cfg.Dealmaking.HttpTransferStallTimeout),
		//},
		//DealLogDurationDays:         cfg.Dealmaking.DealLogDurationDays,
		//StorageFilter:               cfg.Dealmaking.Filter,
		//SealingPipelineCacheTimeout: time.Duration(cfg.Dealmaking.SealingPipelineCacheTimeout),
		//}
		dl := logs.NewDealLogger(logsDB)
		//tspt := httptransport.New(h, dl)
		//prov, err := storagemarket.NewProvider(prvCfg, sqldb, dealsDB, fundMgr, storageMgr, a, dp, provAddr, secb, commpc,
		//sps, cdm, df, logsSqlDB.db, logsDB, piecedirectory, ip, lp, &signatureVerifier{a}, dl, tspt)
		//if err != nil {
		//return nil, err
		//}

		//func NewDirectDealsProvider(fullnodeApi v1api.FullNode, pieceAdder types.PieceAdder, directDealsDB *db.DirectDataDB, dealLogger *logs.DealLogger) *DirectDealsProvider {

		ddpCfg := storagemarket.DDPConfig{
			StartEpochSealingBuffer: abi.ChainEpoch(cfg.Dealmaking.StartEpochSealingBuffer),
			RemoteCommp:             cfg.Dealmaking.RemoteCommp,
		}

		prov := storagemarket.NewDirectDealsProvider(ddpCfg, fullnodeApi, secb, commpc, commpt, sps, directDealsDB, dl, piecedirectory, ip)
		return prov, nil
	}
}
