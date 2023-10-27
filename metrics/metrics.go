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
	Version, _  = tag.NewKey("version")
	Commit, _   = tag.NewKey("commit")
	NodeType, _ = tag.NewKey("node_type")

	Endpoint, _     = tag.NewKey("endpoint")
	APIInterface, _ = tag.NewKey("api") // to distinguish between gateway api and full node api endpoint calls
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

	//boostd-data
	BoostdDataSuccessAddDealForPieceCount           = stats.Int64("boostddata/success_add_deal_for_piece_count", "Counter of add deal success", stats.UnitDimensionless)
	BoostdDataSuccessAddIndexCount                  = stats.Int64("boostddata/success_add_index_count", "Counter of add index success", stats.UnitDimensionless)
	BoostdDataSuccessIsIndexedCount                 = stats.Int64("boostddata/success_is_indexed_count", "Counter of is indexed success", stats.UnitDimensionless)
	BoostdDataSuccessIsCompleteIndexCount           = stats.Int64("boostddata/success_is_complete_index_count", "Counter of is complete index success", stats.UnitDimensionless)
	BoostdDataSuccessGetIndexCount                  = stats.Int64("boostddata/success_get_index_count", "Counter of get index success", stats.UnitDimensionless)
	BoostdDataSuccessGetOffsetSizeCount             = stats.Int64("boostddata/success_get_offset_size_count", "Counter of get offset size success", stats.UnitDimensionless)
	BoostdDataSuccessListPiecesCount                = stats.Int64("boostddata/success_list_pieces_count", "Counter of list pieces success", stats.UnitDimensionless)
	BoostdDataSuccessPiecesCountCount               = stats.Int64("boostddata/success_piece_count_count", "Counter of piece count success", stats.UnitDimensionless)
	BoostdDataSuccessScanProgressCount              = stats.Int64("boostddata/success_scan_progress_count", "Counter of scan progress success", stats.UnitDimensionless)
	BoostdDataSuccessGetPieceMetadataCount          = stats.Int64("boostddata/success_get_piece_metadata_count", "Counter of get piece metadata success", stats.UnitDimensionless)
	BoostdDataSuccessGetPieceDealsCount             = stats.Int64("boostddata/success_get_piece_deals_count", "Counter of get piece deals success", stats.UnitDimensionless)
	BoostdDataSuccessIndexedAtCount                 = stats.Int64("boostddata/success_indexed_at_count", "Counter of indexed at success", stats.UnitDimensionless)
	BoostdDataSuccessPiecesContainingMultihashCount = stats.Int64("boostddata/success_pieces_containing_multihashes_count", "Counter of pieces containing multihashes success", stats.UnitDimensionless)
	BoostdDataSuccessRemoveDealForPieceCount        = stats.Int64("boostddata/success_remove_deal_for_piece_count", "Counter of remove deal for piece success", stats.UnitDimensionless)
	BoostdDataSuccessRemovePieceMetadataCount       = stats.Int64("boostddata/success_remove_piece_metadata_count", "Counter of remove piece metadata success", stats.UnitDimensionless)
	BoostdDataSuccessRemoveIndexesCount             = stats.Int64("boostddata/success_remove_indexes_count", "Counter of remove indexes success", stats.UnitDimensionless)
	BoostdDataSuccessNextPiecesToCheckCount         = stats.Int64("boostddata/success_next_pieces_to_check_count", "Counter of next pieces to check success", stats.UnitDimensionless)
	BoostdDataSuccessFlagPieceCount                 = stats.Int64("boostddata/success_flag_piece_count", "Counter of flag piece success", stats.UnitDimensionless)
	BoostdDataSuccessUnflagPieceCount               = stats.Int64("boostddata/success_unflag_piece_count", "Counter of unflag piece success", stats.UnitDimensionless)
	BoostdDataSuccessFlaggedPiecesListCount         = stats.Int64("boostddata/success_flagged_pieces_list_count", "Counter of flagged pieces list success", stats.UnitDimensionless)
	BoostdDataSuccessFlaggedPiecesCountCount        = stats.Int64("boostddata/success_flagged_pieces_count_count", "Counter of flagged pieces count success", stats.UnitDimensionless)
	BoostdDataFailureAddDealForPieceCount           = stats.Int64("boostddata/failure_add_deal_for_piece_count", "Counter of add deal failure", stats.UnitDimensionless)
	BoostdDataFailureAddIndexCount                  = stats.Int64("boostddata/failure_add_index_count", "Counter of add index failure", stats.UnitDimensionless)
	BoostdDataFailureIsIndexedCount                 = stats.Int64("boostddata/failure_is_indexed_count", "Counter of is indexed failure", stats.UnitDimensionless)
	BoostdDataFailureIsCompleteIndexCount           = stats.Int64("boostddata/failure_is_complete_index_count", "Counter of is complete index failure", stats.UnitDimensionless)
	BoostdDataFailureGetIndexCount                  = stats.Int64("boostddata/failure_get_index_count", "Counter of get index failure", stats.UnitDimensionless)
	BoostdDataFailureGetOffsetSizeCount             = stats.Int64("boostddata/failure_get_offset_size_count", "Counter of get offset size failure", stats.UnitDimensionless)
	BoostdDataFailureListPiecesCount                = stats.Int64("boostddata/failure_list_pieces_count", "Counter of list pieces failure", stats.UnitDimensionless)
	BoostdDataFailurePiecesCountCount               = stats.Int64("boostddata/failure_piece_count_count", "Counter of piece count failure", stats.UnitDimensionless)
	BoostdDataFailureScanProgressCount              = stats.Int64("boostddata/failure_scan_progress_count", "Counter of scan progress failure", stats.UnitDimensionless)
	BoostdDataFailureGetPieceMetadataCount          = stats.Int64("boostddata/failure_get_piece_metadata_count", "Counter of get piece metadata failure", stats.UnitDimensionless)
	BoostdDataFailureGetPieceDealsCount             = stats.Int64("boostddata/failure_get_piece_deals_count", "Counter of get piece deals failure", stats.UnitDimensionless)
	BoostdDataFailureIndexedAtCount                 = stats.Int64("boostddata/failure_indexed_at_count", "Counter of indexed at failure", stats.UnitDimensionless)
	BoostdDataFailurePiecesContainingMultihashCount = stats.Int64("boostddata/failure_pieces_containing_multihashes_count", "Counter of pieces containing multihashes failure", stats.UnitDimensionless)
	BoostdDataFailureRemoveDealForPieceCount        = stats.Int64("boostddata/failure_remove_deal_for_piece_count", "Counter of remove deal for piece failure", stats.UnitDimensionless)
	BoostdDataFailureRemovePieceMetadataCount       = stats.Int64("boostddata/failure_remove_piece_metadata_count", "Counter of remove piece metadata failure", stats.UnitDimensionless)
	BoostdDataFailureRemoveIndexesCount             = stats.Int64("boostddata/failure_remove_indexes_count", "Counter of remove indexes failure", stats.UnitDimensionless)
	BoostdDataFailureNextPiecesToCheckCount         = stats.Int64("boostddata/failure_next_pieces_to_check_count", "Counter of next pieces to check failure", stats.UnitDimensionless)
	BoostdDataFailureFlagPieceCount                 = stats.Int64("boostddata/failure_flag_piece_count", "Counter of flag piece failure", stats.UnitDimensionless)
	BoostdDataFailureUnflagPieceCount               = stats.Int64("boostddata/failure_unflag_piece_count", "Counter of unflag piece failure", stats.UnitDimensionless)
	BoostdDataFailureFlaggedPiecesListCount         = stats.Int64("boostddata/failure_flagged_pieces_list_count", "Counter of flagged pieces list failure", stats.UnitDimensionless)
	BoostdDataFailureFlaggedPiecesCountCount        = stats.Int64("boostddata/failure_flagged_pieces_count_count", "Counter of flagged pieces count failure", stats.UnitDimensionless)
)

var (
	InfoView = &view.View{
		Name:        "info",
		Description: "Boost service information",
		Measure:     BoostInfo,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{Version, Commit, NodeType},
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

	//boostd-data
	BoostdDataSuccessAddDealForPieceCountView           = &view.View{Measure: BoostdDataSuccessAddDealForPieceCount, Aggregation: view.Count()}
	BoostdDataSuccessAddIndexCountView                  = &view.View{Measure: BoostdDataSuccessAddIndexCount, Aggregation: view.Count()}
	BoostdDataSuccessIsIndexedCountView                 = &view.View{Measure: BoostdDataSuccessIsIndexedCount, Aggregation: view.Count()}
	BoostdDataSuccessIsCompleteIndexCountView           = &view.View{Measure: BoostdDataSuccessIsCompleteIndexCount, Aggregation: view.Count()}
	BoostdDataSuccessGetIndexCountView                  = &view.View{Measure: BoostdDataSuccessGetIndexCount, Aggregation: view.Count()}
	BoostdDataSuccessGetOffsetSizeCountView             = &view.View{Measure: BoostdDataSuccessGetOffsetSizeCount, Aggregation: view.Count()}
	BoostdDataSuccessListPiecesCountView                = &view.View{Measure: BoostdDataSuccessListPiecesCount, Aggregation: view.Count()}
	BoostdDataSuccessPiecesCountCountView               = &view.View{Measure: BoostdDataSuccessPiecesCountCount, Aggregation: view.Count()}
	BoostdDataSuccessScanProgressCountView              = &view.View{Measure: BoostdDataSuccessScanProgressCount, Aggregation: view.Count()}
	BoostdDataSuccessGetPieceMetadataCountView          = &view.View{Measure: BoostdDataSuccessGetPieceMetadataCount, Aggregation: view.Count()}
	BoostdDataSuccessGetPieceDealsCountView             = &view.View{Measure: BoostdDataSuccessGetPieceDealsCount, Aggregation: view.Count()}
	BoostdDataSuccessIndexedAtCountView                 = &view.View{Measure: BoostdDataSuccessIndexedAtCount, Aggregation: view.Count()}
	BoostdDataSuccessPiecesContainingMultihashCountView = &view.View{Measure: BoostdDataSuccessPiecesContainingMultihashCount, Aggregation: view.Count()}
	BoostdDataSuccessRemoveDealForPieceCountView        = &view.View{Measure: BoostdDataSuccessRemoveDealForPieceCount, Aggregation: view.Count()}
	BoostdDataSuccessRemovePieceMetadataCountView       = &view.View{Measure: BoostdDataSuccessRemovePieceMetadataCount, Aggregation: view.Count()}
	BoostdDataSuccessRemoveIndexesCountView             = &view.View{Measure: BoostdDataSuccessRemoveIndexesCount, Aggregation: view.Count()}
	BoostdDataSuccessNextPiecesToCheckCountView         = &view.View{Measure: BoostdDataSuccessNextPiecesToCheckCount, Aggregation: view.Count()}
	BoostdDataSuccessFlagPieceCountView                 = &view.View{Measure: BoostdDataSuccessFlagPieceCount, Aggregation: view.Count()}
	BoostdDataSuccessUnflagPieceCountView               = &view.View{Measure: BoostdDataSuccessUnflagPieceCount, Aggregation: view.Count()}
	BoostdDataSuccessFlaggedPiecesListCountView         = &view.View{Measure: BoostdDataSuccessFlaggedPiecesListCount, Aggregation: view.Count()}
	BoostdDataSuccessFlaggedPiecesCountCountView        = &view.View{Measure: BoostdDataSuccessFlaggedPiecesCountCount, Aggregation: view.Count()}
	BoostdDataFailureAddDealForPieceCountView           = &view.View{Measure: BoostdDataFailureAddDealForPieceCount, Aggregation: view.Count()}
	BoostdDataFailureAddIndexCountView                  = &view.View{Measure: BoostdDataFailureAddIndexCount, Aggregation: view.Count()}
	BoostdDataFailureIsIndexedCountView                 = &view.View{Measure: BoostdDataFailureIsIndexedCount, Aggregation: view.Count()}
	BoostdDataFailureIsCompleteIndexCountView           = &view.View{Measure: BoostdDataFailureIsCompleteIndexCount, Aggregation: view.Count()}
	BoostdDataFailureGetIndexCountView                  = &view.View{Measure: BoostdDataFailureGetIndexCount, Aggregation: view.Count()}
	BoostdDataFailureGetOffsetSizeCountView             = &view.View{Measure: BoostdDataFailureGetOffsetSizeCount, Aggregation: view.Count()}
	BoostdDataFailureListPiecesCountView                = &view.View{Measure: BoostdDataFailureListPiecesCount, Aggregation: view.Count()}
	BoostdDataFailurePiecesCountCountView               = &view.View{Measure: BoostdDataFailurePiecesCountCount, Aggregation: view.Count()}
	BoostdDataFailureScanProgressCountView              = &view.View{Measure: BoostdDataFailureScanProgressCount, Aggregation: view.Count()}
	BoostdDataFailureGetPieceMetadataCountView          = &view.View{Measure: BoostdDataFailureGetPieceMetadataCount, Aggregation: view.Count()}
	BoostdDataFailureGetPieceDealsCountView             = &view.View{Measure: BoostdDataFailureGetPieceDealsCount, Aggregation: view.Count()}
	BoostdDataFailureIndexedAtCountView                 = &view.View{Measure: BoostdDataFailureIndexedAtCount, Aggregation: view.Count()}
	BoostdDataFailurePiecesContainingMultihashCountView = &view.View{Measure: BoostdDataFailurePiecesContainingMultihashCount, Aggregation: view.Count()}
	BoostdDataFailureRemoveDealForPieceCountView        = &view.View{Measure: BoostdDataFailureRemoveDealForPieceCount, Aggregation: view.Count()}
	BoostdDataFailureRemovePieceMetadataCountView       = &view.View{Measure: BoostdDataFailureRemovePieceMetadataCount, Aggregation: view.Count()}
	BoostdDataFailureRemoveIndexesCountView             = &view.View{Measure: BoostdDataFailureRemoveIndexesCount, Aggregation: view.Count()}
	BoostdDataFailureNextPiecesToCheckCountView         = &view.View{Measure: BoostdDataFailureNextPiecesToCheckCount, Aggregation: view.Count()}
	BoostdDataFailureFlagPieceCountView                 = &view.View{Measure: BoostdDataFailureFlagPieceCount, Aggregation: view.Count()}
	BoostdDataFailureUnflagPieceCountView               = &view.View{Measure: BoostdDataFailureUnflagPieceCount, Aggregation: view.Count()}
	BoostdDataFailureFlaggedPiecesListCountView         = &view.View{Measure: BoostdDataFailureFlaggedPiecesListCount, Aggregation: view.Count()}
	BoostdDataFailureFlaggedPiecesCountCountView        = &view.View{Measure: BoostdDataFailureFlaggedPiecesCountCount, Aggregation: view.Count()}
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
		BoostdDataSuccessAddDealForPieceCountView,
		BoostdDataSuccessAddIndexCountView,
		BoostdDataSuccessIsIndexedCountView,
		BoostdDataSuccessIsCompleteIndexCountView,
		BoostdDataSuccessGetIndexCountView,
		BoostdDataSuccessGetOffsetSizeCountView,
		BoostdDataSuccessListPiecesCountView,
		BoostdDataSuccessPiecesCountCountView,
		BoostdDataSuccessScanProgressCountView,
		BoostdDataSuccessGetPieceMetadataCountView,
		BoostdDataSuccessGetPieceDealsCountView,
		BoostdDataSuccessIndexedAtCountView,
		BoostdDataSuccessPiecesContainingMultihashCountView,
		BoostdDataSuccessRemoveDealForPieceCountView,
		BoostdDataSuccessRemovePieceMetadataCountView,
		BoostdDataSuccessRemoveIndexesCountView,
		BoostdDataSuccessNextPiecesToCheckCountView,
		BoostdDataSuccessFlagPieceCountView,
		BoostdDataSuccessUnflagPieceCountView,
		BoostdDataSuccessFlaggedPiecesListCountView,
		BoostdDataSuccessFlaggedPiecesCountCountView,
		BoostdDataFailureAddDealForPieceCountView,
		BoostdDataFailureAddIndexCountView,
		BoostdDataFailureIsIndexedCountView,
		BoostdDataFailureIsCompleteIndexCountView,
		BoostdDataFailureGetIndexCountView,
		BoostdDataFailureGetOffsetSizeCountView,
		BoostdDataFailureListPiecesCountView,
		BoostdDataFailurePiecesCountCountView,
		BoostdDataFailureScanProgressCountView,
		BoostdDataFailureGetPieceMetadataCountView,
		BoostdDataFailureGetPieceDealsCountView,
		BoostdDataFailureIndexedAtCountView,
		BoostdDataFailurePiecesContainingMultihashCountView,
		BoostdDataFailureRemoveDealForPieceCountView,
		BoostdDataFailureRemovePieceMetadataCountView,
		BoostdDataFailureRemoveIndexesCountView,
		BoostdDataFailureNextPiecesToCheckCountView,
		BoostdDataFailureFlagPieceCountView,
		BoostdDataFailureUnflagPieceCountView,
		BoostdDataFailureFlaggedPiecesListCountView,
		BoostdDataFailureFlaggedPiecesCountCountView,
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
