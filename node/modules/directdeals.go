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
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/storage/sectorblocks"
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/fx"
)

func NewDirectDealsProvider(provAddr address.Address, cfg *config.Boost) func(lc fx.Lifecycle, h host.Host, fullnodeApi v1api.FullNode, sqldb *sql.DB, directDealsDB *db.DirectDealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storageadapter.DealPublisher, secb *sectorblocks.SectorBlocks, commpt storagemarket.CommpThrottle, me types.MinerEndpoints, df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB, piecedirectory *piecedirectory.PieceDirectory, ip *indexprovider.Wrapper, lp gfm_storagemarket.StorageProvider, cdm *storagemarket.ChainDealManager) (*storagemarket.DirectDealsProvider, error) {
	return func(lc fx.Lifecycle, h host.Host, fullnodeApi v1api.FullNode, sqldb *sql.DB, directDealsDB *db.DirectDealsDB,
		fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, dp *storageadapter.DealPublisher, secb *sectorblocks.SectorBlocks,
		commpt storagemarket.CommpThrottle, me types.MinerEndpoints,
		df dtypes.StorageDealFilter, logsSqlDB *LogSqlDB, logsDB *db.LogsDB,
		piecedirectory *piecedirectory.PieceDirectory, ip *indexprovider.Wrapper,
		lp gfm_storagemarket.StorageProvider, cdm *storagemarket.ChainDealManager) (*storagemarket.DirectDealsProvider, error) {

		dl := logs.NewDealLogger(logsDB)

		ddpCfg := storagemarket.DDPConfig{
			StartEpochSealingBuffer: abi.ChainEpoch(cfg.Dealmaking.StartEpochSealingBuffer),
			RemoteCommp:             cfg.Dealmaking.RemoteCommp,
		}

		prov, err := storagemarket.NewDirectDealsProvider(ddpCfg, fullnodeApi, secb, commpt, me, directDealsDB, dl, piecedirectory, ip)
		if err != nil {
			return nil, err
		}
		return prov, nil
	}
}
