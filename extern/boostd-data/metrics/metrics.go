package metrics

import (
	"context"
	"time"

	rpcmetrics "github.com/filecoin-project/go-jsonrpc/metrics"
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
	APIInterface, _ = tag.NewKey("api")
)

// Measures
var (
	BoostInfo          = stats.Int64("info", "Arbitrary counter to tag boost info to", stats.UnitDimensionless)
	APIRequestDuration = stats.Float64("api/request_duration_ms", "Duration of API requests", stats.UnitMilliseconds)

	//boostd-data
	SuccessAddDealForPieceCount           = stats.Int64("success_add_deal_for_piece_count", "Counter of add deal success", stats.UnitDimensionless)
	SuccessAddIndexCount                  = stats.Int64("success_add_index_count", "Counter of add index success", stats.UnitDimensionless)
	SuccessIsIndexedCount                 = stats.Int64("success_is_indexed_count", "Counter of is indexed success", stats.UnitDimensionless)
	SuccessIsCompleteIndexCount           = stats.Int64("success_is_complete_index_count", "Counter of is complete index success", stats.UnitDimensionless)
	SuccessGetIndexCount                  = stats.Int64("success_get_index_count", "Counter of get index success", stats.UnitDimensionless)
	SuccessGetOffsetSizeCount             = stats.Int64("success_get_offset_size_count", "Counter of get offset size success", stats.UnitDimensionless)
	SuccessListPiecesCount                = stats.Int64("success_list_pieces_count", "Counter of list pieces success", stats.UnitDimensionless)
	SuccessPiecesCountCount               = stats.Int64("success_piece_count_count", "Counter of piece count success", stats.UnitDimensionless)
	SuccessScanProgressCount              = stats.Int64("success_scan_progress_count", "Counter of scan progress success", stats.UnitDimensionless)
	SuccessGetPieceMetadataCount          = stats.Int64("success_get_piece_metadata_count", "Counter of get piece metadata success", stats.UnitDimensionless)
	SuccessGetPieceDealsCount             = stats.Int64("success_get_piece_deals_count", "Counter of get piece deals success", stats.UnitDimensionless)
	SuccessIndexedAtCount                 = stats.Int64("success_indexed_at_count", "Counter of indexed at success", stats.UnitDimensionless)
	SuccessPiecesContainingMultihashCount = stats.Int64("success_pieces_containing_multihashes_count", "Counter of pieces containing multihashes success", stats.UnitDimensionless)
	SuccessRemoveDealForPieceCount        = stats.Int64("success_remove_deal_for_piece_count", "Counter of remove deal for piece success", stats.UnitDimensionless)
	SuccessRemovePieceMetadataCount       = stats.Int64("success_remove_piece_metadata_count", "Counter of remove piece metadata success", stats.UnitDimensionless)
	SuccessRemoveIndexesCount             = stats.Int64("success_remove_indexes_count", "Counter of remove indexes success", stats.UnitDimensionless)
	SuccessNextPiecesToCheckCount         = stats.Int64("success_next_pieces_to_check_count", "Counter of next pieces to check success", stats.UnitDimensionless)
	SuccessFlagPieceCount                 = stats.Int64("success_flag_piece_count", "Counter of flag piece success", stats.UnitDimensionless)
	SuccessUnflagPieceCount               = stats.Int64("success_unflag_piece_count", "Counter of unflag piece success", stats.UnitDimensionless)
	SuccessFlaggedPiecesListCount         = stats.Int64("success_flagged_pieces_list_count", "Counter of flagged pieces list success", stats.UnitDimensionless)
	SuccessFlaggedPiecesCountCount        = stats.Int64("success_flagged_pieces_count_count", "Counter of flagged pieces count success", stats.UnitDimensionless)
	FailureAddDealForPieceCount           = stats.Int64("failure_add_deal_for_piece_count", "Counter of add deal failure", stats.UnitDimensionless)
	FailureAddIndexCount                  = stats.Int64("failure_add_index_count", "Counter of add index failure", stats.UnitDimensionless)
	FailureIsIndexedCount                 = stats.Int64("failure_is_indexed_count", "Counter of is indexed failure", stats.UnitDimensionless)
	FailureIsCompleteIndexCount           = stats.Int64("failure_is_complete_index_count", "Counter of is complete index failure", stats.UnitDimensionless)
	FailureGetIndexCount                  = stats.Int64("failure_get_index_count", "Counter of get index failure", stats.UnitDimensionless)
	FailureGetOffsetSizeCount             = stats.Int64("failure_get_offset_size_count", "Counter of get offset size failure", stats.UnitDimensionless)
	FailureListPiecesCount                = stats.Int64("failure_list_pieces_count", "Counter of list pieces failure", stats.UnitDimensionless)
	FailurePiecesCountCount               = stats.Int64("failure_piece_count_count", "Counter of piece count failure", stats.UnitDimensionless)
	FailureScanProgressCount              = stats.Int64("failure_scan_progress_count", "Counter of scan progress failure", stats.UnitDimensionless)
	FailureGetPieceMetadataCount          = stats.Int64("failure_get_piece_metadata_count", "Counter of get piece metadata failure", stats.UnitDimensionless)
	FailureGetPieceDealsCount             = stats.Int64("failure_get_piece_deals_count", "Counter of get piece deals failure", stats.UnitDimensionless)
	FailureIndexedAtCount                 = stats.Int64("failure_indexed_at_count", "Counter of indexed at failure", stats.UnitDimensionless)
	FailurePiecesContainingMultihashCount = stats.Int64("failure_pieces_containing_multihashes_count", "Counter of pieces containing multihashes failure", stats.UnitDimensionless)
	FailureRemoveDealForPieceCount        = stats.Int64("failure_remove_deal_for_piece_count", "Counter of remove deal for piece failure", stats.UnitDimensionless)
	FailureRemovePieceMetadataCount       = stats.Int64("failure_remove_piece_metadata_count", "Counter of remove piece metadata failure", stats.UnitDimensionless)
	FailureRemoveIndexesCount             = stats.Int64("failure_remove_indexes_count", "Counter of remove indexes failure", stats.UnitDimensionless)
	FailureNextPiecesToCheckCount         = stats.Int64("failure_next_pieces_to_check_count", "Counter of next pieces to check failure", stats.UnitDimensionless)
	FailureFlagPieceCount                 = stats.Int64("failure_flag_piece_count", "Counter of flag piece failure", stats.UnitDimensionless)
	FailureUnflagPieceCount               = stats.Int64("failure_unflag_piece_count", "Counter of unflag piece failure", stats.UnitDimensionless)
	FailureFlaggedPiecesListCount         = stats.Int64("failure_flagged_pieces_list_count", "Counter of flagged pieces list failure", stats.UnitDimensionless)
	FailureFlaggedPiecesCountCount        = stats.Int64("failure_flagged_pieces_count_count", "Counter of flagged pieces count failure", stats.UnitDimensionless)
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

	//boostd-data
	SuccessAddDealForPieceCountView           = &view.View{Measure: SuccessAddDealForPieceCount, Aggregation: view.Count()}
	SuccessAddIndexCountView                  = &view.View{Measure: SuccessAddIndexCount, Aggregation: view.Count()}
	SuccessIsIndexedCountView                 = &view.View{Measure: SuccessIsIndexedCount, Aggregation: view.Count()}
	SuccessIsCompleteIndexCountView           = &view.View{Measure: SuccessIsCompleteIndexCount, Aggregation: view.Count()}
	SuccessGetIndexCountView                  = &view.View{Measure: SuccessGetIndexCount, Aggregation: view.Count()}
	SuccessGetOffsetSizeCountView             = &view.View{Measure: SuccessGetOffsetSizeCount, Aggregation: view.Count()}
	SuccessListPiecesCountView                = &view.View{Measure: SuccessListPiecesCount, Aggregation: view.Count()}
	SuccessPiecesCountCountView               = &view.View{Measure: SuccessPiecesCountCount, Aggregation: view.Count()}
	SuccessScanProgressCountView              = &view.View{Measure: SuccessScanProgressCount, Aggregation: view.Count()}
	SuccessGetPieceMetadataCountView          = &view.View{Measure: SuccessGetPieceMetadataCount, Aggregation: view.Count()}
	SuccessGetPieceDealsCountView             = &view.View{Measure: SuccessGetPieceDealsCount, Aggregation: view.Count()}
	SuccessIndexedAtCountView                 = &view.View{Measure: SuccessIndexedAtCount, Aggregation: view.Count()}
	SuccessPiecesContainingMultihashCountView = &view.View{Measure: SuccessPiecesContainingMultihashCount, Aggregation: view.Count()}
	SuccessRemoveDealForPieceCountView        = &view.View{Measure: SuccessRemoveDealForPieceCount, Aggregation: view.Count()}
	SuccessRemovePieceMetadataCountView       = &view.View{Measure: SuccessRemovePieceMetadataCount, Aggregation: view.Count()}
	SuccessRemoveIndexesCountView             = &view.View{Measure: SuccessRemoveIndexesCount, Aggregation: view.Count()}
	SuccessNextPiecesToCheckCountView         = &view.View{Measure: SuccessNextPiecesToCheckCount, Aggregation: view.Count()}
	SuccessFlagPieceCountView                 = &view.View{Measure: SuccessFlagPieceCount, Aggregation: view.Count()}
	SuccessUnflagPieceCountView               = &view.View{Measure: SuccessUnflagPieceCount, Aggregation: view.Count()}
	SuccessFlaggedPiecesListCountView         = &view.View{Measure: SuccessFlaggedPiecesListCount, Aggregation: view.Count()}
	SuccessFlaggedPiecesCountCountView        = &view.View{Measure: SuccessFlaggedPiecesCountCount, Aggregation: view.Count()}
	FailureAddDealForPieceCountView           = &view.View{Measure: FailureAddDealForPieceCount, Aggregation: view.Count()}
	FailureAddIndexCountView                  = &view.View{Measure: FailureAddIndexCount, Aggregation: view.Count()}
	FailureIsIndexedCountView                 = &view.View{Measure: FailureIsIndexedCount, Aggregation: view.Count()}
	FailureIsCompleteIndexCountView           = &view.View{Measure: FailureIsCompleteIndexCount, Aggregation: view.Count()}
	FailureGetIndexCountView                  = &view.View{Measure: FailureGetIndexCount, Aggregation: view.Count()}
	FailureGetOffsetSizeCountView             = &view.View{Measure: FailureGetOffsetSizeCount, Aggregation: view.Count()}
	FailureListPiecesCountView                = &view.View{Measure: FailureListPiecesCount, Aggregation: view.Count()}
	FailurePiecesCountCountView               = &view.View{Measure: FailurePiecesCountCount, Aggregation: view.Count()}
	FailureScanProgressCountView              = &view.View{Measure: FailureScanProgressCount, Aggregation: view.Count()}
	FailureGetPieceMetadataCountView          = &view.View{Measure: FailureGetPieceMetadataCount, Aggregation: view.Count()}
	FailureGetPieceDealsCountView             = &view.View{Measure: FailureGetPieceDealsCount, Aggregation: view.Count()}
	FailureIndexedAtCountView                 = &view.View{Measure: FailureIndexedAtCount, Aggregation: view.Count()}
	FailurePiecesContainingMultihashCountView = &view.View{Measure: FailurePiecesContainingMultihashCount, Aggregation: view.Count()}
	FailureRemoveDealForPieceCountView        = &view.View{Measure: FailureRemoveDealForPieceCount, Aggregation: view.Count()}
	FailureRemovePieceMetadataCountView       = &view.View{Measure: FailureRemovePieceMetadataCount, Aggregation: view.Count()}
	FailureRemoveIndexesCountView             = &view.View{Measure: FailureRemoveIndexesCount, Aggregation: view.Count()}
	FailureNextPiecesToCheckCountView         = &view.View{Measure: FailureNextPiecesToCheckCount, Aggregation: view.Count()}
	FailureFlagPieceCountView                 = &view.View{Measure: FailureFlagPieceCount, Aggregation: view.Count()}
	FailureUnflagPieceCountView               = &view.View{Measure: FailureUnflagPieceCount, Aggregation: view.Count()}
	FailureFlaggedPiecesListCountView         = &view.View{Measure: FailureFlaggedPiecesListCount, Aggregation: view.Count()}
	FailureFlaggedPiecesCountCountView        = &view.View{Measure: FailureFlaggedPiecesCountCount, Aggregation: view.Count()}
)

// DefaultViews is an array of OpenCensus views for metric gathering purposes
var DefaultViews = func() []*view.View {
	views := []*view.View{
		InfoView,
		APIRequestDurationView,
		SuccessAddDealForPieceCountView,
		SuccessAddIndexCountView,
		SuccessIsIndexedCountView,
		SuccessIsCompleteIndexCountView,
		SuccessGetIndexCountView,
		SuccessGetOffsetSizeCountView,
		SuccessListPiecesCountView,
		SuccessPiecesCountCountView,
		SuccessScanProgressCountView,
		SuccessGetPieceMetadataCountView,
		SuccessGetPieceDealsCountView,
		SuccessIndexedAtCountView,
		SuccessPiecesContainingMultihashCountView,
		SuccessRemoveDealForPieceCountView,
		SuccessRemovePieceMetadataCountView,
		SuccessRemoveIndexesCountView,
		SuccessNextPiecesToCheckCountView,
		SuccessFlagPieceCountView,
		SuccessUnflagPieceCountView,
		SuccessFlaggedPiecesListCountView,
		SuccessFlaggedPiecesCountCountView,
		FailureAddDealForPieceCountView,
		FailureAddIndexCountView,
		FailureIsIndexedCountView,
		FailureIsCompleteIndexCountView,
		FailureGetIndexCountView,
		FailureGetOffsetSizeCountView,
		FailureListPiecesCountView,
		FailurePiecesCountCountView,
		FailureScanProgressCountView,
		FailureGetPieceMetadataCountView,
		FailureGetPieceDealsCountView,
		FailureIndexedAtCountView,
		FailurePiecesContainingMultihashCountView,
		FailureRemoveDealForPieceCountView,
		FailureRemovePieceMetadataCountView,
		FailureRemoveIndexesCountView,
		FailureNextPiecesToCheckCountView,
		FailureFlagPieceCountView,
		FailureUnflagPieceCountView,
		FailureFlaggedPiecesListCountView,
		FailureFlaggedPiecesCountCountView,
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
