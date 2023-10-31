package metrics

import (
	"context"
	"time"

	rpcmetrics "github.com/filecoin-project/go-jsonrpc/metrics"
	lotusmetrics "github.com/filecoin-project/lotus/metrics"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// Distribution
var defaultMillisecondsDistribution = view.Distribution(0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 3000, 4000, 5000, 7500, 10000, 20000, 50000, 100000)

// Global Tags
var (
	// common
	Version, _   = tag.NewKey("version")
	Commit, _    = tag.NewKey("commit")
	NodeType, _  = tag.NewKey("node_type")
	StartedAt, _ = tag.NewKey("started_at")

	Endpoint, _     = tag.NewKey("endpoint")
	APIInterface, _ = tag.NewKey("api")
)

// Measures
var (
	BoostInfo          = stats.Int64("info", "Arbitrary counter to tag boost info to", stats.UnitDimensionless)
	APIRequestDuration = stats.Float64("api/request_duration_ms", "Duration of API requests", stats.UnitMilliseconds)

	// http
	HttpPieceByCidRequestCount     = stats.Int64("http/piece_by_cid_request_count", "Counter of /piece/<piece-cid> requests", stats.UnitDimensionless)
	HttpPieceByCidRequestDuration  = stats.Float64("http/piece_by_cid_request_duration_ms", "Time spent retrieving a piece by cid", stats.UnitMilliseconds)
	HttpPieceByCid200ResponseCount = stats.Int64("http/piece_by_cid_200_response_count", "Counter of /piece/<piece-cid> 200 responses", stats.UnitDimensionless)
	HttpPieceByCid400ResponseCount = stats.Int64("http/piece_by_cid_400_response_count", "Counter of /piece/<piece-cid> 400 responses", stats.UnitDimensionless)
	HttpPieceByCid404ResponseCount = stats.Int64("http/piece_by_cid_404_response_count", "Counter of /piece/<piece-cid> 404 responses", stats.UnitDimensionless)
	HttpPieceByCid500ResponseCount = stats.Int64("http/piece_by_cid_500_response_count", "Counter of /piece/<piece-cid> 500 responses", stats.UnitDimensionless)

	// http remote blockstore
	HttpRblsGetRequestCount             = stats.Int64("http/rbls_get_request_count", "Counter of RemoteBlockstore Get requests", stats.UnitDimensionless)
	HttpRblsGetSuccessResponseCount     = stats.Int64("http/rbls_get_success_response_count", "Counter of successful RemoteBlockstore Get responses", stats.UnitDimensionless)
	HttpRblsGetFailResponseCount        = stats.Int64("http/rbls_get_fail_response_count", "Counter of failed RemoteBlockstore Get responses", stats.UnitDimensionless)
	HttpRblsGetSizeRequestCount         = stats.Int64("http/rbls_getsize_request_count", "Counter of RemoteBlockstore GetSize requests", stats.UnitDimensionless)
	HttpRblsGetSizeSuccessResponseCount = stats.Int64("http/rbls_getsize_success_response_count", "Counter of successful RemoteBlockstore GetSize responses", stats.UnitDimensionless)
	HttpRblsGetSizeFailResponseCount    = stats.Int64("http/rbls_getsize_fail_response_count", "Counter of failed RemoteBlockstore GetSize responses", stats.UnitDimensionless)
	HttpRblsHasRequestCount             = stats.Int64("http/rbls_has_request_count", "Counter of RemoteBlockstore Has requests", stats.UnitDimensionless)
	HttpRblsHasSuccessResponseCount     = stats.Int64("http/rbls_has_success_response_count", "Counter of successful RemoteBlockstore Has responses", stats.UnitDimensionless)
	HttpRblsHasFailResponseCount        = stats.Int64("http/rbls_has_fail_response_count", "Counter of failed RemoteBlockstore Has responses", stats.UnitDimensionless)
	HttpRblsBytesSentCount              = stats.Int64("http/rbls_bytes_sent_count", "Counter of the number of bytes sent by bitswap since startup", stats.UnitBytes)

	// bitswap
	BitswapRblsGetRequestCount             = stats.Int64("bitswap/rbls_get_request_count", "Counter of RemoteBlockstore Get requests", stats.UnitDimensionless)
	BitswapRblsGetSuccessResponseCount     = stats.Int64("bitswap/rbls_get_success_response_count", "Counter of successful RemoteBlockstore Get responses", stats.UnitDimensionless)
	BitswapRblsGetFailResponseCount        = stats.Int64("bitswap/rbls_get_fail_response_count", "Counter of failed RemoteBlockstore Get responses", stats.UnitDimensionless)
	BitswapRblsGetSizeRequestCount         = stats.Int64("bitswap/rbls_getsize_request_count", "Counter of RemoteBlockstore GetSize requests", stats.UnitDimensionless)
	BitswapRblsGetSizeSuccessResponseCount = stats.Int64("bitswap/rbls_getsize_success_response_count", "Counter of successful RemoteBlockstore GetSize responses", stats.UnitDimensionless)
	BitswapRblsGetSizeFailResponseCount    = stats.Int64("bitswap/rbls_getsize_fail_response_count", "Counter of failed RemoteBlockstore GetSize responses", stats.UnitDimensionless)
	BitswapRblsHasRequestCount             = stats.Int64("bitswap/rbls_has_request_count", "Counter of RemoteBlockstore Has requests", stats.UnitDimensionless)
	BitswapRblsHasSuccessResponseCount     = stats.Int64("bitswap/rbls_has_success_response_count", "Counter of successful RemoteBlockstore Has responses", stats.UnitDimensionless)
	BitswapRblsHasFailResponseCount        = stats.Int64("bitswap/rbls_has_fail_response_count", "Counter of failed RemoteBlockstore Has responses", stats.UnitDimensionless)
	BitswapRblsBytesSentCount              = stats.Int64("bitswap/rbls_bytes_sent_count", "Counter of the number of bytes sent by bitswap since startup", stats.UnitBytes)

	// graphsync
	GraphsyncRequestQueuedCount                 = stats.Int64("graphsync/request_queued_count", "Counter of Graphsync requests queued", stats.UnitDimensionless)
	GraphsyncRequestQueuedPaidCount             = stats.Int64("graphsync/request_queued_paid_count", "Counter of Graphsync paid requests queued", stats.UnitDimensionless)
	GraphsyncRequestQueuedUnpaidCount           = stats.Int64("graphsync/request_queued_unpaid_count", "Counter of Graphsync unpaid requests queued", stats.UnitDimensionless)
	GraphsyncRequestStartedCount                = stats.Int64("graphsync/request_started_count", "Counter of Graphsync requests started", stats.UnitDimensionless)
	GraphsyncRequestStartedPaidCount            = stats.Int64("graphsync/request_started_paid_count", "Counter of Graphsync paid requests started", stats.UnitDimensionless)
	GraphsyncRequestStartedUnpaidCount          = stats.Int64("graphsync/request_started_unpaid_count", "Counter of Graphsync unpaid requests started", stats.UnitDimensionless)
	GraphsyncRequestStartedUnpaidSuccessCount   = stats.Int64("graphsync/request_started_unpaid_success_count", "Counter of Graphsync successful unpaid requests started", stats.UnitDimensionless)
	GraphsyncRequestStartedUnpaidFailCount      = stats.Int64("graphsync/request_started_unpaid_fail_count", "Counter of Graphsync failed unpaid requests started", stats.UnitDimensionless)
	GraphsyncRequestCompletedCount              = stats.Int64("graphsync/request_completed_count", "Counter of Graphsync requests completed", stats.UnitDimensionless)
	GraphsyncRequestCompletedPaidCount          = stats.Int64("graphsync/request_completed_paid_count", "Counter of Graphsync paid requests completed", stats.UnitDimensionless)
	GraphsyncRequestCompletedUnpaidCount        = stats.Int64("graphsync/request_completed_unpaid_count", "Counter of Graphsync unpaid requests completed", stats.UnitDimensionless)
	GraphsyncRequestCompletedUnpaidSuccessCount = stats.Int64("graphsync/request_completed_unpaid_success_count", "Counter of Graphsync successful unpaid requests completed", stats.UnitDimensionless)
	GraphsyncRequestCompletedUnpaidFailCount    = stats.Int64("graphsync/request_completed_unpaid_fail_count", "Counter of Graphsync failed unpaid requests completed", stats.UnitDimensionless)
	GraphsyncRequestClientCancelledCount        = stats.Int64("graphsync/request_client_cancelled_count", "Counter of Graphsync requests cancelled", stats.UnitDimensionless)
	GraphsyncRequestClientCancelledPaidCount    = stats.Int64("graphsync/request_client_cancelled_paid_count", "Counter of Graphsync paid requests cancelled", stats.UnitDimensionless)
	GraphsyncRequestClientCancelledUnpaidCount  = stats.Int64("graphsync/request_client_cancelled_unpaid_count", "Counter of Graphsync unpaid requests cancelled", stats.UnitDimensionless)
	GraphsyncRequestBlockSentCount              = stats.Int64("graphsync/request_block_sent_count", "Counter of Graphsync blocks sent", stats.UnitDimensionless)
	GraphsyncRequestBlockSentPaidCount          = stats.Int64("graphsync/request_block_sent_paid_count", "Counter of Graphsync paid blocks sent", stats.UnitDimensionless)
	GraphsyncRequestBlockSentUnpaidCount        = stats.Int64("graphsync/request_block_sent_unpaid_count", "Counter of Graphsync unpaid blocks sent", stats.UnitDimensionless)
	GraphsyncRequestBytesSentCount              = stats.Int64("graphsync/request_bytes_sent_count", "Counter of Graphsync paid bytes sent", stats.UnitBytes)
	GraphsyncRequestBytesSentPaidCount          = stats.Int64("graphsync/request_bytes_sent_paid_count", "Counter of Graphsync paid bytes sent", stats.UnitBytes)
	GraphsyncRequestBytesSentUnpaidCount        = stats.Int64("graphsync/request_bytes_sent_unpaid_count", "Counter of Graphsync unpaid bytes sent", stats.UnitBytes)
	GraphsyncRequestNetworkErrorCount           = stats.Int64("graphsync/request_network_error_count", "Counter of Graphsync network errors", stats.UnitDimensionless)
)

var (
	InfoView = &view.View{
		Name:        "info",
		Description: "Boost service information",
		Measure:     BoostInfo,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{Version, Commit, NodeType, StartedAt},
	}
	APIRequestDurationView = &view.View{
		Measure:     APIRequestDuration,
		Aggregation: defaultMillisecondsDistribution,
		TagKeys:     []tag.Key{APIInterface, Endpoint},
	}
	// http
	HttpPieceByCidRequestCountView = &view.View{
		Measure:     HttpPieceByCidRequestCount,
		Aggregation: view.Count(),
	}
	HttpPieceByCidRequestDurationView = &view.View{
		Measure:     HttpPieceByCidRequestDuration,
		Aggregation: defaultMillisecondsDistribution,
	}
	HttpPieceByCid200ResponseCountView = &view.View{
		Measure:     HttpPieceByCid200ResponseCount,
		Aggregation: view.Count(),
	}
	HttpPieceByCid400ResponseCountView = &view.View{
		Measure:     HttpPieceByCid400ResponseCount,
		Aggregation: view.Count(),
	}
	HttpPieceByCid404ResponseCountView = &view.View{
		Measure:     HttpPieceByCid404ResponseCount,
		Aggregation: view.Count(),
	}
	HttpPieceByCid500ResponseCountView = &view.View{
		Measure:     HttpPieceByCid500ResponseCount,
		Aggregation: view.Count(),
	}

	HttpRblsGetRequestCountView = &view.View{
		Measure:     HttpRblsGetRequestCount,
		Aggregation: view.Count(),
	}
	HttpRblsGetSuccessResponseCountView = &view.View{
		Measure:     HttpRblsGetSuccessResponseCount,
		Aggregation: view.Count(),
	}
	HttpRblsGetFailResponseCountView = &view.View{
		Measure:     HttpRblsGetFailResponseCount,
		Aggregation: view.Count(),
	}
	HttpRblsGetSizeRequestCountView = &view.View{
		Measure:     HttpRblsGetSizeRequestCount,
		Aggregation: view.Count(),
	}
	HttpRblsGetSizeSuccessResponseCountView = &view.View{
		Measure:     HttpRblsGetSizeSuccessResponseCount,
		Aggregation: view.Count(),
	}
	HttpRblsGetSizeFailResponseCountView = &view.View{
		Measure:     HttpRblsGetSizeFailResponseCount,
		Aggregation: view.Count(),
	}
	HttpRblsHasRequestCountView = &view.View{
		Measure:     HttpRblsHasRequestCount,
		Aggregation: view.Count(),
	}
	HttpRblsHasSuccessResponseCountView = &view.View{
		Measure:     HttpRblsHasSuccessResponseCount,
		Aggregation: view.Count(),
	}
	HttpRblsHasFailResponseCountView = &view.View{
		Measure:     HttpRblsHasFailResponseCount,
		Aggregation: view.Count(),
	}
	HttpRblsBytesSentCountView = &view.View{
		Measure:     HttpRblsBytesSentCount,
		Aggregation: view.Sum(),
	}

	// bitswap
	BitswapRblsGetRequestCountView = &view.View{
		Measure:     BitswapRblsGetRequestCount,
		Aggregation: view.Count(),
	}
	BitswapRblsGetSuccessResponseCountView = &view.View{
		Measure:     BitswapRblsGetSuccessResponseCount,
		Aggregation: view.Count(),
	}
	BitswapRblsGetFailResponseCountView = &view.View{
		Measure:     BitswapRblsGetFailResponseCount,
		Aggregation: view.Count(),
	}
	BitswapRblsGetSizeRequestCountView = &view.View{
		Measure:     BitswapRblsGetSizeRequestCount,
		Aggregation: view.Count(),
	}
	BitswapRblsGetSizeSuccessResponseCountView = &view.View{
		Measure:     BitswapRblsGetSizeSuccessResponseCount,
		Aggregation: view.Count(),
	}
	BitswapRblsGetSizeFailResponseCountView = &view.View{
		Measure:     BitswapRblsGetSizeFailResponseCount,
		Aggregation: view.Count(),
	}
	BitswapRblsHasRequestCountView = &view.View{
		Measure:     BitswapRblsHasRequestCount,
		Aggregation: view.Count(),
	}
	BitswapRblsHasSuccessResponseCountView = &view.View{
		Measure:     BitswapRblsHasSuccessResponseCount,
		Aggregation: view.Count(),
	}
	BitswapRblsHasFailResponseCountView = &view.View{
		Measure:     BitswapRblsHasFailResponseCount,
		Aggregation: view.Count(),
	}
	BitswapRblsBytesSentCountView = &view.View{
		Measure:     BitswapRblsBytesSentCount,
		Aggregation: view.Sum(),
	}

	// graphsync
	GraphsyncRequestQueuedCountView = &view.View{
		Measure:     GraphsyncRequestQueuedCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestQueuedPaidCountView = &view.View{
		Measure:     GraphsyncRequestQueuedPaidCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestQueuedUnpaidCountView = &view.View{
		Measure:     GraphsyncRequestQueuedUnpaidCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestStartedCountView = &view.View{
		Measure:     GraphsyncRequestStartedCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestStartedPaidCountView = &view.View{
		Measure:     GraphsyncRequestStartedPaidCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestStartedUnpaidCountView = &view.View{
		Measure:     GraphsyncRequestStartedUnpaidCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestStartedUnpaidSuccessCountView = &view.View{
		Measure:     GraphsyncRequestStartedUnpaidSuccessCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestStartedUnpaidFailCountView = &view.View{
		Measure:     GraphsyncRequestStartedUnpaidFailCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestCompletedCountView = &view.View{
		Measure:     GraphsyncRequestCompletedCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestCompletedPaidCountView = &view.View{
		Measure:     GraphsyncRequestCompletedPaidCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestCompletedUnpaidCountView = &view.View{
		Measure:     GraphsyncRequestCompletedUnpaidCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestCompletedUnpaidSuccessCountView = &view.View{
		Measure:     GraphsyncRequestCompletedUnpaidSuccessCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestCompletedUnpaidFailCountView = &view.View{
		Measure:     GraphsyncRequestCompletedUnpaidFailCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestClientCancelledCountView = &view.View{
		Measure:     GraphsyncRequestClientCancelledCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestClientCancelledPaidCountView = &view.View{
		Measure:     GraphsyncRequestClientCancelledPaidCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestClientCancelledUnpaidCountView = &view.View{
		Measure:     GraphsyncRequestClientCancelledUnpaidCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestBlockSentCountView = &view.View{
		Measure:     GraphsyncRequestBlockSentCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestPaidBlockSentCountView = &view.View{
		Measure:     GraphsyncRequestBlockSentPaidCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestUnpaidBlockSentCountView = &view.View{
		Measure:     GraphsyncRequestBlockSentUnpaidCount,
		Aggregation: view.Count(),
	}
	GraphsyncRequestBytesSentCountView = &view.View{
		Measure:     GraphsyncRequestBytesSentCount,
		Aggregation: view.Sum(),
	}
	GraphsyncRequestPaidBytesSentCountView = &view.View{
		Measure:     GraphsyncRequestBytesSentPaidCount,
		Aggregation: view.Sum(),
	}
	GraphsyncRequestUnpaidBytesSentCountView = &view.View{
		Measure:     GraphsyncRequestBytesSentUnpaidCount,
		Aggregation: view.Sum(),
	}
	GraphsyncRequestNetworkErrorCountView = &view.View{
		Measure:     GraphsyncRequestNetworkErrorCount,
		Aggregation: view.Count(),
	}
)

// DefaultViews is an array of OpenCensus views for metric gathering purposes
var DefaultViews = func() []*view.View {
	views := []*view.View{
		InfoView,
		APIRequestDurationView,
		HttpPieceByCidRequestCountView,
		HttpPieceByCidRequestDurationView,
		HttpPieceByCid200ResponseCountView,
		HttpPieceByCid400ResponseCountView,
		HttpPieceByCid404ResponseCountView,
		HttpPieceByCid500ResponseCountView,
		HttpRblsGetRequestCountView,
		HttpRblsGetSuccessResponseCountView,
		HttpRblsGetFailResponseCountView,
		HttpRblsGetSizeRequestCountView,
		HttpRblsGetSizeSuccessResponseCountView,
		HttpRblsGetSizeFailResponseCountView,
		HttpRblsHasRequestCountView,
		HttpRblsHasSuccessResponseCountView,
		HttpRblsHasFailResponseCountView,
		HttpRblsBytesSentCountView,
		BitswapRblsGetRequestCountView,
		BitswapRblsGetSuccessResponseCountView,
		BitswapRblsGetFailResponseCountView,
		BitswapRblsGetSizeRequestCountView,
		BitswapRblsGetSizeSuccessResponseCountView,
		BitswapRblsGetSizeFailResponseCountView,
		BitswapRblsHasRequestCountView,
		BitswapRblsHasSuccessResponseCountView,
		BitswapRblsHasFailResponseCountView,
		BitswapRblsBytesSentCountView,
		GraphsyncRequestQueuedCountView,
		GraphsyncRequestQueuedPaidCountView,
		GraphsyncRequestQueuedUnpaidCountView,
		GraphsyncRequestStartedCountView,
		GraphsyncRequestStartedPaidCountView,
		GraphsyncRequestStartedUnpaidCountView,
		GraphsyncRequestStartedUnpaidSuccessCountView,
		GraphsyncRequestStartedUnpaidFailCountView,
		GraphsyncRequestCompletedCountView,
		GraphsyncRequestCompletedPaidCountView,
		GraphsyncRequestCompletedUnpaidCountView,
		GraphsyncRequestCompletedUnpaidSuccessCountView,
		GraphsyncRequestCompletedUnpaidFailCountView,
		GraphsyncRequestClientCancelledCountView,
		GraphsyncRequestClientCancelledPaidCountView,
		GraphsyncRequestClientCancelledUnpaidCountView,
		GraphsyncRequestBlockSentCountView,
		GraphsyncRequestPaidBlockSentCountView,
		GraphsyncRequestUnpaidBlockSentCountView,
		GraphsyncRequestBytesSentCountView,
		GraphsyncRequestPaidBytesSentCountView,
		GraphsyncRequestUnpaidBytesSentCountView,
		GraphsyncRequestNetworkErrorCountView,
		lotusmetrics.DagStorePRBytesDiscardedView,
		lotusmetrics.DagStorePRBytesRequestedView,
		lotusmetrics.DagStorePRDiscardCountView,
		lotusmetrics.DagStorePRInitCountView,
		lotusmetrics.DagStorePRSeekBackBytesView,
		lotusmetrics.DagStorePRSeekBackCountView,
		lotusmetrics.DagStorePRSeekForwardBytesView,
		lotusmetrics.DagStorePRSeekForwardCountView,
	}
	//views = append(views, blockstore.DefaultViews...)
	views = append(views, rpcmetrics.DefaultViews...)
	return views
}()

// SinceInMilliseconds returns the duration of time since the provide time as a float64.
func SinceInMilliseconds(startTime time.Time) float64 {
	return float64(time.Since(startTime).Nanoseconds()) / 1e6
}

// Timer is a function stopwatch, calling it starts the timer,
// calling the returned function will record the duration.
func Timer(ctx context.Context, m *stats.Float64Measure) func() {
	start := time.Now()
	return func() {
		stats.Record(ctx, m.M(SinceInMilliseconds(start)))
	}
}
