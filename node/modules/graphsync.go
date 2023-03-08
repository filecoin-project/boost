package modules

import (
	"context"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/retrievalmarket/server"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	retrievalimpl "github.com/filecoin-project/go-fil-markets/retrievalmarket/impl"
	"github.com/filecoin-project/go-fil-markets/stores"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	lotus_helpers "github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/libp2p/go-libp2p/core/host"
	"go.uber.org/fx"
)

// Graphsync creates a graphsync instance used to serve retrievals.
func Graphsync(parallelTransfersForStorage uint64, parallelTransfersForStoragePerPeer uint64, parallelTransfersForRetrieval uint64) func(mctx lotus_helpers.MetricsCtx, lc fx.Lifecycle, ibs dtypes.IndexBackedBlockstore, h host.Host, net lotus_dtypes.ProviderTransferNetwork, dealDecider lotus_dtypes.RetrievalDealFilter, dagStore stores.DAGStoreWrapper, pstore lotus_dtypes.ProviderPieceStore, sa retrievalmarket.SectorAccessor, askStore retrievalmarket.AskStore) (*server.GraphsyncUnpaidRetrieval, error) {
	return func(mctx lotus_helpers.MetricsCtx, lc fx.Lifecycle, ibs dtypes.IndexBackedBlockstore, h host.Host, net lotus_dtypes.ProviderTransferNetwork, dealDecider lotus_dtypes.RetrievalDealFilter, dagStore stores.DAGStoreWrapper, pstore lotus_dtypes.ProviderPieceStore, sa retrievalmarket.SectorAccessor, askStore retrievalmarket.AskStore) (*server.GraphsyncUnpaidRetrieval, error) {
		// Create a Graphsync instance
		mkgs := lotus_modules.StagingGraphsync(parallelTransfersForStorage, parallelTransfersForStoragePerPeer, parallelTransfersForRetrieval)
		gs := mkgs(mctx, lc, ibs, h)

		// Wrap the Graphsync instance with a handler for unpaid retrieval requests
		vdeps := server.ValidationDeps{
			DealDecider:    retrievalimpl.DealDecider(dealDecider),
			DagStore:       dagStore,
			PieceStore:     pstore,
			SectorAccessor: sa,
			AskStore:       askStore,
		}
		gsupr, err := server.NewGraphsyncUnpaidRetrieval(h.ID(), gs, net, vdeps)

		// Set up a context that is cancelled when the boostd process exits
		gsctx, cancel := context.WithCancel(context.Background())
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				gsupr.Start(gsctx)
				return nil
			},
			OnStop: func(_ context.Context) error {
				cancel()
				return nil
			},
		})

		return gsupr, err
	}
}
