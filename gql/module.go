package gql

import (
	"context"

	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/boost/lib/legacy"
	"github.com/filecoin-project/boost/lib/mpoolmonitor"
	"github.com/filecoin-project/boost/markets/storageadapter"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/retrievalmarket/rtvllog"
	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/storedask"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/node/repo"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	provider "github.com/ipni/index-provider"
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/fx"
)

func NewGraphqlServer(cfg *config.Boost) func(lc fx.Lifecycle, r repo.LockedRepo, h host.Host, prov *storagemarket.Provider, ddProv *storagemarket.DirectDealsProvider, dealsDB *db.DealsDB, directDealsDB *db.DirectDealsDB, logsDB *db.LogsDB, retDB *rtvllog.RetrievalLogDB, plDB *db.ProposalLogsDB, fundsDB *db.FundsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, publisher *storageadapter.DealPublisher, spApi sealingpipeline.API, legacyDeals legacy.LegacyDealManager, piecedirectory *piecedirectory.PieceDirectory, indexProv provider.Interface, idxProvWrapper *indexprovider.Wrapper, fullNode v1api.FullNode, bg BlockGetter, ssm *sectorstatemgr.SectorStateMgr, mpool *mpoolmonitor.MpoolMonitor, mma *lib.MultiMinerAccessor, sask storedask.StoredAsk) (*Server, error) {
	return func(lc fx.Lifecycle, r repo.LockedRepo, h host.Host, prov *storagemarket.Provider, ddProv *storagemarket.DirectDealsProvider, dealsDB *db.DealsDB, directDealsDB *db.DirectDealsDB, logsDB *db.LogsDB, retDB *rtvllog.RetrievalLogDB, plDB *db.ProposalLogsDB, fundsDB *db.FundsDB, fundMgr *fundmanager.FundManager,
		storageMgr *storagemanager.StorageManager, publisher *storageadapter.DealPublisher, spApi sealingpipeline.API,
		legacyDeals legacy.LegacyDealManager, piecedirectory *piecedirectory.PieceDirectory,
		indexProv provider.Interface, idxProvWrapper *indexprovider.Wrapper, fullNode v1api.FullNode, bg BlockGetter,
		ssm *sectorstatemgr.SectorStateMgr, mpool *mpoolmonitor.MpoolMonitor, mma *lib.MultiMinerAccessor, sask storedask.StoredAsk) (*Server, error) {

		resolverCtx, cancel := context.WithCancel(context.Background())
		resolver, err := NewResolver(resolverCtx, cfg, r, h, dealsDB, directDealsDB, logsDB, retDB, plDB, fundsDB, fundMgr, storageMgr, spApi, prov, ddProv, legacyDeals, piecedirectory, publisher, indexProv, idxProvWrapper, fullNode, ssm, mpool, mma, sask)
		if err != nil {
			cancel()
			return nil, err
		}
		svr := NewServer(cfg, resolver, bg)

		lc.Append(fx.Hook{
			OnStart: svr.Start,
			OnStop: func(ctx context.Context) error {
				cancel()
				return svr.Stop(ctx)
			},
		})

		return svr, nil
	}
}

func NewBlockGetter(pd *piecedirectory.PieceDirectory) BlockGetter {
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
