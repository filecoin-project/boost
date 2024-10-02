package modules

import (
	"database/sql"

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
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/fx"
)

func NewDirectDealsProvider(provAddr address.Address, cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, fullnodeApi v1api.FullNode, sqldb *sql.DB, directDealsDB *db.DirectDealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storageadapter.DealPublisher, secb types.PieceAdder, commpc types.CommpCalculator, commpt storagemarket.CommpThrottle, sps sealingpipeline.API, df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB, piecedirectory *piecedirectory.PieceDirectory, ip *indexprovider.Wrapper, cdm *storagemarket.ChainDealManager) (*storagemarket.DirectDealsProvider, error) {
	return func(lc fx.Lifecycle, h host.Host, fullnodeApi v1api.FullNode, sqldb *sql.DB, directDealsDB *db.DirectDealsDB,
		fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storageadapter.DealPublisher, secb types.PieceAdder,
		commpc types.CommpCalculator, commpt storagemarket.CommpThrottle, sps sealingpipeline.API,
		df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB,
		piecedirectory *piecedirectory.PieceDirectory, ip *indexprovider.Wrapper, cdm *storagemarket.ChainDealManager) (*storagemarket.DirectDealsProvider, error) {

		dl := logs.NewDealLogger(logsDB)

		ddpCfg := storagemarket.DDPConfig{
			StartEpochSealingBuffer: abi.ChainEpoch(cfg.Dealmaking.StartEpochSealingBuffer),
			RemoteCommp:             cfg.Dealmaking.RemoteCommp,
			CurioMigration:          cfg.CurioMigration.Enable,
		}

		prov := storagemarket.NewDirectDealsProvider(ddpCfg, provAddr, fullnodeApi, secb, commpc, commpt, sps, directDealsDB, dl, piecedirectory, ip)
		return prov, nil
	}
}
