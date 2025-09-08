package modules

import (
	"context"
	"sync"
	"time"

	graphsync "github.com/filecoin-project/boost-graphsync/impl"
	gsnet "github.com/filecoin-project/boost-graphsync/network"
	"github.com/filecoin-project/boost-graphsync/storeutil"
	"github.com/filecoin-project/boost/cmd/lib"
	"github.com/filecoin-project/boost/cmd/lib/remoteblockstore"
	"github.com/filecoin-project/boost/metrics"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/retrievalmarket/server"
	lotus_helpers "github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/ipfs/kubo/core/node/helpers"
	"github.com/libp2p/go-libp2p/core/host"
	"go.opencensus.io/stats"
	"go.uber.org/fx"
)

// RetrievalGraphsync creates a graphsync instance used to serve retrievals.
func RetrievalGraphsync(parallelTransfersForRetrieval uint64) func(mctx lotus_helpers.MetricsCtx, lc fx.Lifecycle, pid *piecedirectory.PieceDirectory, h host.Host, net dtypes.ProviderTransferNetwork, dealDecider dtypes.RetrievalDealFilter, sa *lib.MultiMinerAccessor, askGetter server.RetrievalAskGetter) (*server.GraphsyncUnpaidRetrieval, error) {
	return func(mctx lotus_helpers.MetricsCtx, lc fx.Lifecycle, pid *piecedirectory.PieceDirectory, h host.Host, net dtypes.ProviderTransferNetwork, dealDecider dtypes.RetrievalDealFilter, sa *lib.MultiMinerAccessor, askGetter server.RetrievalAskGetter) (*server.GraphsyncUnpaidRetrieval, error) {
		// Graphsync tracks metrics separately, pass nil blockMetrics to the remote blockstore
		rb := remoteblockstore.NewRemoteBlockstore(pid, nil)

		// Create a Graphsync instance
		mkgs := Graphsync(parallelTransfersForRetrieval)
		gs := mkgs(mctx, lc, rb, pid, h)

		// Wrap the Graphsync instance with a handler for unpaid retrieval requests
		vdeps := server.ValidationDeps{
			DealDecider:    server.DealDecider(dealDecider),
			PieceDirectory: pid,
			SectorAccessor: sa,
			AskStore:       askGetter,
		}

		gsupr, err := server.NewGraphsyncUnpaidRetrieval(h.ID(), gs, net, vdeps)
		if err != nil {
			return nil, err
		}

		// Set up a context that is cancelled when the boostd process exits
		gsctx, cancel := context.WithCancel(context.Background())
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				return gsupr.Start(gsctx)
			},
			OnStop: func(_ context.Context) error {
				cancel()
				return nil
			},
		})

		return gsupr, nil
	}
}

func Graphsync(parallelTransfersForRetrieval uint64) func(mctx lotus_helpers.MetricsCtx, lc fx.Lifecycle, ibs dtypes.StagingBlockstore, pid *piecedirectory.PieceDirectory, h host.Host) dtypes.StagingGraphsync {
	return func(mctx lotus_helpers.MetricsCtx, lc fx.Lifecycle, ibs dtypes.StagingBlockstore, pid *piecedirectory.PieceDirectory, h host.Host) dtypes.StagingGraphsync {
		graphsyncNetwork := gsnet.NewFromLibp2pHost(h)
		lsys := storeutil.LinkSystemForBlockstore(ibs)
		gs := graphsync.New(helpers.LifecycleCtx(mctx, lc),
			graphsyncNetwork,
			lsys,
			graphsync.RejectAllRequestsByDefault(),
			graphsync.MaxInProgressOutgoingRequests(parallelTransfersForRetrieval),
			graphsync.MaxLinksPerIncomingRequests(config.MaxTraversalLinks),
			graphsync.MaxLinksPerOutgoingRequests(config.MaxTraversalLinks))

		graphsyncStats(mctx, lc, gs)

		return gs
	}
}

func graphsyncStats(mctx helpers.MetricsCtx, lc fx.Lifecycle, gs dtypes.Graphsync) {
	var closeOnce sync.Once
	stopStats := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				t := time.NewTicker(10 * time.Second)
				for {
					select {
					case <-t.C:

						st := gs.Stats()
						stats.Record(mctx, metrics.GraphsyncReceivingPeersCount.M(int64(st.OutgoingRequests.TotalPeers)))
						stats.Record(mctx, metrics.GraphsyncReceivingActiveCount.M(int64(st.OutgoingRequests.Active)))
						stats.Record(mctx, metrics.GraphsyncReceivingCountCount.M(int64(st.OutgoingRequests.Pending)))
						stats.Record(mctx, metrics.GraphsyncReceivingTotalMemoryAllocated.M(int64(st.IncomingResponses.TotalAllocatedAllPeers)))
						stats.Record(mctx, metrics.GraphsyncReceivingTotalPendingAllocations.M(int64(st.IncomingResponses.TotalPendingAllocations)))
						stats.Record(mctx, metrics.GraphsyncReceivingPeersPending.M(int64(st.IncomingResponses.NumPeersWithPendingAllocations)))
						stats.Record(mctx, metrics.GraphsyncSendingPeersCount.M(int64(st.IncomingRequests.TotalPeers)))
						stats.Record(mctx, metrics.GraphsyncSendingActiveCount.M(int64(st.IncomingRequests.Active)))
						stats.Record(mctx, metrics.GraphsyncSendingCountCount.M(int64(st.IncomingRequests.Pending)))
						stats.Record(mctx, metrics.GraphsyncSendingTotalMemoryAllocated.M(int64(st.OutgoingResponses.TotalAllocatedAllPeers)))
						stats.Record(mctx, metrics.GraphsyncSendingTotalPendingAllocations.M(int64(st.OutgoingResponses.TotalPendingAllocations)))
						stats.Record(mctx, metrics.GraphsyncSendingPeersPending.M(int64(st.OutgoingResponses.NumPeersWithPendingAllocations)))

					case <-stopStats:
						return
					}
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			closeOnce.Do(func() { close(stopStats) })
			return nil
		},
	})
}
