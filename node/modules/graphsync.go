package modules

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	retrievalimpl "github.com/filecoin-project/boost-gfm/retrievalmarket/impl"
	"github.com/filecoin-project/boost-gfm/stores"
	graphsync "github.com/filecoin-project/boost-graphsync/impl"
	gsnet "github.com/filecoin-project/boost-graphsync/network"
	"github.com/filecoin-project/boost-graphsync/storeutil"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/retrievalmarket/server"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/metrics"
	lotus_helpers "github.com/filecoin-project/lotus/node/modules/helpers"
	"github.com/ipfs/kubo/core/node/helpers"
	"github.com/ipld/go-ipld-prime"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"
	"github.com/libp2p/go-libp2p/core/host"
	"go.opencensus.io/stats"
	"go.uber.org/fx"
)

var _ server.AskGetter = (*ProxyAskGetter)(nil)

// ProxyAskGetter is used to avoid circular dependencies:
// RetrievalProvider depends on RetrievalGraphsync, which depends on RetrievalProvider's
// GetAsk method.
// We create an AskGetter that returns zero-priced asks by default.
// Then we set the AskGetter to the RetrievalProvider after it's been created.
type ProxyAskGetter struct {
	server.AskGetter
}

func (ag *ProxyAskGetter) GetAsk() *retrievalmarket.Ask {
	if ag.AskGetter == nil {
		return &retrievalmarket.Ask{
			PricePerByte: abi.NewTokenAmount(0),
			UnsealPrice:  abi.NewTokenAmount(0),
		}
	}
	return ag.AskGetter.GetAsk()
}

func NewAskGetter() *ProxyAskGetter {
	return &ProxyAskGetter{}
}

func SetAskGetter(proxy *ProxyAskGetter, rp retrievalmarket.RetrievalProvider) {
	proxy.AskGetter = rp
}

// LinkSystemProv is used to avoid circular dependencies
type LinkSystemProv struct {
	*ipld.LinkSystem
}

func NewLinkSystemProvider() *LinkSystemProv {
	return &LinkSystemProv{}
}

func (p *LinkSystemProv) LinkSys() *ipld.LinkSystem {
	return p.LinkSystem
}

func SetLinkSystem(proxy *LinkSystemProv, prov provider.Interface) {
	e := prov.(*engine.Engine)
	proxy.LinkSystem = e.LinkSystem()
}

// RetrievalGraphsync creates a graphsync instance used to serve retrievals.
func RetrievalGraphsync(parallelTransfersForStorage uint64, parallelTransfersForStoragePerPeer uint64, parallelTransfersForRetrieval uint64) func(mctx lotus_helpers.MetricsCtx, lc fx.Lifecycle, ibs dtypes.IndexBackedBlockstore, h host.Host, net dtypes.ProviderTransferNetwork, dealDecider dtypes.RetrievalDealFilter, dagStore stores.DAGStoreWrapper, pstore dtypes.ProviderPieceStore, sa retrievalmarket.SectorAccessor, askGetter server.AskGetter, ls server.LinkSystemProvider) (*server.GraphsyncUnpaidRetrieval, error) {
	return func(mctx lotus_helpers.MetricsCtx, lc fx.Lifecycle, ibs dtypes.IndexBackedBlockstore, h host.Host, net dtypes.ProviderTransferNetwork, dealDecider dtypes.RetrievalDealFilter, dagStore stores.DAGStoreWrapper, pstore dtypes.ProviderPieceStore, sa retrievalmarket.SectorAccessor, askGetter server.AskGetter, ls server.LinkSystemProvider) (*server.GraphsyncUnpaidRetrieval, error) {
		// Create a Graphsync instance
		mkgs := Graphsync(parallelTransfersForStorage, parallelTransfersForStoragePerPeer, parallelTransfersForRetrieval)
		gs := mkgs(mctx, lc, ibs, h)

		// Wrap the Graphsync instance with a handler for unpaid retrieval requests
		vdeps := server.ValidationDeps{
			DealDecider:    retrievalimpl.DealDecider(dealDecider),
			DagStore:       dagStore,
			PieceStore:     pstore,
			SectorAccessor: sa,
			AskStore:       askGetter,
		}
		gsupr, err := server.NewGraphsyncUnpaidRetrieval(h.ID(), gs, net, vdeps, ls)

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

		return gsupr, err
	}
}

func Graphsync(parallelTransfersForStorage uint64, parallelTransfersForStoragePerPeer uint64, parallelTransfersForRetrieval uint64) func(mctx helpers.MetricsCtx, lc fx.Lifecycle, ibs dtypes.StagingBlockstore, h host.Host) dtypes.StagingGraphsync {
	return func(mctx helpers.MetricsCtx, lc fx.Lifecycle, ibs dtypes.StagingBlockstore, h host.Host) dtypes.StagingGraphsync {
		graphsyncNetwork := gsnet.NewFromLibp2pHost(h)
		lsys := storeutil.LinkSystemForBlockstore(ibs)
		gs := graphsync.New(helpers.LifecycleCtx(mctx, lc),
			graphsyncNetwork,
			lsys,
			graphsync.RejectAllRequestsByDefault(),
			graphsync.MaxInProgressIncomingRequests(parallelTransfersForRetrieval),
			graphsync.MaxInProgressIncomingRequestsPerPeer(parallelTransfersForStoragePerPeer),
			graphsync.MaxInProgressOutgoingRequests(parallelTransfersForStorage),
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
