package metrics

import (
	"context"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	rpcmetrics "github.com/filecoin-project/go-jsonrpc/metrics"
	lotusmetrics "github.com/filecoin-project/lotus/metrics"
)

// Distribution
var defaultMillisecondsDistribution = view.Distribution(0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 3000, 4000, 5000, 7500, 10000, 20000, 50000, 100000)
var workMillisecondsDistribution = view.Distribution(
	250, 500, 1000, 2000, 5000, 10_000, 30_000, 60_000, 2*60_000, 5*60_000, 10*60_000, 15*60_000, 30*60_000, // short sealing tasks
	40*60_000, 45*60_000, 50*60_000, 55*60_000, 60*60_000, 65*60_000, 70*60_000, 75*60_000, 80*60_000, 85*60_000, 100*60_000, 120*60_000, // PC2 / C2 range
	130*60_000, 140*60_000, 150*60_000, 160*60_000, 180*60_000, 200*60_000, 220*60_000, 260*60_000, 300*60_000, // PC1 range
	350*60_000, 400*60_000, 600*60_000, 800*60_000, 1000*60_000, 1300*60_000, 1800*60_000, 4000*60_000, 10000*60_000, // intel PC1 range
)

// Global Tags
var (
	// common
	Version, _     = tag.NewKey("version")
	Commit, _      = tag.NewKey("commit")
	NodeType, _    = tag.NewKey("node_type")
	PeerID, _      = tag.NewKey("peer_id")
	MinerID, _     = tag.NewKey("miner_id")
	FailureType, _ = tag.NewKey("failure_type")

	// chain
	Local, _        = tag.NewKey("local")
	MessageFrom, _  = tag.NewKey("message_from")
	MessageTo, _    = tag.NewKey("message_to")
	MessageNonce, _ = tag.NewKey("message_nonce")
	ReceivedFrom, _ = tag.NewKey("received_from")
	MsgValid, _     = tag.NewKey("message_valid")
	Endpoint, _     = tag.NewKey("endpoint")
	APIInterface, _ = tag.NewKey("api") // to distinguish between gateway api and full node api endpoint calls

	// miner
	TaskType, _       = tag.NewKey("task_type")
	WorkerHostname, _ = tag.NewKey("worker_hostname")
	StorageID, _      = tag.NewKey("storage_id")
)

// Measures
var (
	// common
	LotusInfo          = stats.Int64("info", "Arbitrary counter to tag lotus info to", stats.UnitDimensionless)
	PeerCount          = stats.Int64("peer/count", "Current number of FIL peers", stats.UnitDimensionless)
	APIRequestDuration = stats.Float64("api/request_duration_ms", "Duration of API requests", stats.UnitMilliseconds)

	// chain
	ChainNodeHeight                     = stats.Int64("chain/node_height", "Current Height of the node", stats.UnitDimensionless)
	ChainNodeHeightExpected             = stats.Int64("chain/node_height_expected", "Expected Height of the node", stats.UnitDimensionless)
	ChainNodeWorkerHeight               = stats.Int64("chain/node_worker_height", "Current Height of workers on the node", stats.UnitDimensionless)
	MessagePublished                    = stats.Int64("message/published", "Counter for total locally published messages", stats.UnitDimensionless)
	MessageReceived                     = stats.Int64("message/received", "Counter for total received messages", stats.UnitDimensionless)
	MessageValidationFailure            = stats.Int64("message/failure", "Counter for message validation failures", stats.UnitDimensionless)
	MessageValidationSuccess            = stats.Int64("message/success", "Counter for message validation successes", stats.UnitDimensionless)
	MessageValidationDuration           = stats.Float64("message/validation_ms", "Duration of message validation", stats.UnitMilliseconds)
	MpoolGetNonceDuration               = stats.Float64("mpool/getnonce_ms", "Duration of getStateNonce in mpool", stats.UnitMilliseconds)
	MpoolGetBalanceDuration             = stats.Float64("mpool/getbalance_ms", "Duration of getStateBalance in mpool", stats.UnitMilliseconds)
	MpoolAddTsDuration                  = stats.Float64("mpool/addts_ms", "Duration of addTs in mpool", stats.UnitMilliseconds)
	MpoolAddDuration                    = stats.Float64("mpool/add_ms", "Duration of Add in mpool", stats.UnitMilliseconds)
	MpoolPushDuration                   = stats.Float64("mpool/push_ms", "Duration of Push in mpool", stats.UnitMilliseconds)
	BlockPublished                      = stats.Int64("block/published", "Counter for total locally published blocks", stats.UnitDimensionless)
	BlockReceived                       = stats.Int64("block/received", "Counter for total received blocks", stats.UnitDimensionless)
	BlockValidationFailure              = stats.Int64("block/failure", "Counter for block validation failures", stats.UnitDimensionless)
	BlockValidationSuccess              = stats.Int64("block/success", "Counter for block validation successes", stats.UnitDimensionless)
	BlockValidationDurationMilliseconds = stats.Float64("block/validation_ms", "Duration for Block Validation in ms", stats.UnitMilliseconds)
	BlockDelay                          = stats.Int64("block/delay", "Delay of accepted blocks, where delay is >5s", stats.UnitMilliseconds)
	PubsubPublishMessage                = stats.Int64("pubsub/published", "Counter for total published messages", stats.UnitDimensionless)
	PubsubDeliverMessage                = stats.Int64("pubsub/delivered", "Counter for total delivered messages", stats.UnitDimensionless)
	PubsubRejectMessage                 = stats.Int64("pubsub/rejected", "Counter for total rejected messages", stats.UnitDimensionless)
	PubsubDuplicateMessage              = stats.Int64("pubsub/duplicate", "Counter for total duplicate messages", stats.UnitDimensionless)
	PubsubRecvRPC                       = stats.Int64("pubsub/recv_rpc", "Counter for total received RPCs", stats.UnitDimensionless)
	PubsubSendRPC                       = stats.Int64("pubsub/send_rpc", "Counter for total sent RPCs", stats.UnitDimensionless)
	PubsubDropRPC                       = stats.Int64("pubsub/drop_rpc", "Counter for total dropped RPCs", stats.UnitDimensionless)
	VMFlushCopyDuration                 = stats.Float64("vm/flush_copy_ms", "Time spent in VM Flush Copy", stats.UnitMilliseconds)
	VMFlushCopyCount                    = stats.Int64("vm/flush_copy_count", "Number of copied objects", stats.UnitDimensionless)
	VMApplyBlocksTotal                  = stats.Float64("vm/applyblocks_total_ms", "Time spent applying block state", stats.UnitMilliseconds)
	VMApplyMessages                     = stats.Float64("vm/applyblocks_messages", "Time spent applying block messages", stats.UnitMilliseconds)
	VMApplyEarly                        = stats.Float64("vm/applyblocks_early", "Time spent in early apply-blocks (null cron, upgrades)", stats.UnitMilliseconds)
	VMApplyCron                         = stats.Float64("vm/applyblocks_cron", "Time spent in cron", stats.UnitMilliseconds)
	VMApplyFlush                        = stats.Float64("vm/applyblocks_flush", "Time spent flushing vm state", stats.UnitMilliseconds)
	VMSends                             = stats.Int64("vm/sends", "Counter for sends processed by the VM", stats.UnitDimensionless)
	VMApplied                           = stats.Int64("vm/applied", "Counter for messages (including internal messages) processed by the VM", stats.UnitDimensionless)

	// miner
	WorkerCallsStarted           = stats.Int64("sealing/worker_calls_started", "Counter of started worker tasks", stats.UnitDimensionless)
	WorkerCallsReturnedCount     = stats.Int64("sealing/worker_calls_returned_count", "Counter of returned worker tasks", stats.UnitDimensionless)
	WorkerCallsReturnedDuration  = stats.Float64("sealing/worker_calls_returned_ms", "Counter of returned worker tasks", stats.UnitMilliseconds)
	WorkerUntrackedCallsReturned = stats.Int64("sealing/worker_untracked_calls_returned", "Counter of returned untracked worker tasks", stats.UnitDimensionless)

	StorageFSAvailable      = stats.Float64("storage/path_fs_available_frac", "Fraction of filesystem available storage", stats.UnitDimensionless)
	StorageAvailable        = stats.Float64("storage/path_available_frac", "Fraction of available storage", stats.UnitDimensionless)
	StorageReserved         = stats.Float64("storage/path_reserved_frac", "Fraction of reserved storage", stats.UnitDimensionless)
	StorageLimitUsed        = stats.Float64("storage/path_limit_used_frac", "Fraction of used optional storage limit", stats.UnitDimensionless)
	StorageCapacityBytes    = stats.Int64("storage/path_capacity_bytes", "storage path capacity", stats.UnitBytes)
	StorageFSAvailableBytes = stats.Int64("storage/path_fs_available_bytes", "filesystem available storage bytes", stats.UnitBytes)
	StorageAvailableBytes   = stats.Int64("storage/path_available_bytes", "available storage bytes", stats.UnitBytes)
	StorageReservedBytes    = stats.Int64("storage/path_reserved_bytes", "reserved storage bytes", stats.UnitBytes)
	StorageLimitUsedBytes   = stats.Int64("storage/path_limit_used_bytes", "used optional storage limit bytes", stats.UnitBytes)
	StorageLimitMaxBytes    = stats.Int64("storage/path_limit_max_bytes", "optional storage limit", stats.UnitBytes)

	// splitstore
	SplitstoreMiss                  = stats.Int64("splitstore/miss", "Number of misses in hotstre access", stats.UnitDimensionless)
	SplitstoreCompactionTimeSeconds = stats.Float64("splitstore/compaction_time", "Compaction time in seconds", stats.UnitSeconds)
	SplitstoreCompactionHot         = stats.Int64("splitstore/hot", "Number of hot blocks in last compaction", stats.UnitDimensionless)
	SplitstoreCompactionCold        = stats.Int64("splitstore/cold", "Number of cold blocks in last compaction", stats.UnitDimensionless)
	SplitstoreCompactionDead        = stats.Int64("splitstore/dead", "Number of dead blocks in last compaction", stats.UnitDimensionless)

	// http
	HttpPayloadByCidRequestCount     = stats.Int64("http/payload_by_cid_request_count", "Counter of /ipfs/<payload-cid> requests", stats.UnitDimensionless)
	HttpPayloadByCidRequestDuration  = stats.Float64("http/payload_by_cid_request_duration_ms", "Time spent retrieving a payload by cid", stats.UnitMilliseconds)
	HttpPayloadByCid200ResponseCount = stats.Int64("http/payload_by_cid_200_response_count", "Counter of /ipfs/<payload-cid> 200 responses", stats.UnitDimensionless)
	HttpPayloadByCid400ResponseCount = stats.Int64("http/payload_by_cid_400_response_count", "Counter of /ipfs/<payload-cid> 400 responses", stats.UnitDimensionless)
	HttpPayloadByCid404ResponseCount = stats.Int64("http/payload_by_cid_404_response_count", "Counter of /ipfs/<payload-cid> 404 responses", stats.UnitDimensionless)
	HttpPayloadByCid500ResponseCount = stats.Int64("http/payload_by_cid_500_response_count", "Counter of /ipfs/<payload-cid> 500 responses", stats.UnitDimensionless)
	HttpPieceByCidRequestCount       = stats.Int64("http/piece_by_cid_request_count", "Counter of /piece/<piece-cid> requests", stats.UnitDimensionless)
	HttpPieceByCidRequestDuration    = stats.Float64("http/piece_by_cid_request_duration_ms", "Time spent retrieving a piece by cid", stats.UnitMilliseconds)
	HttpPieceByCid200ResponseCount   = stats.Int64("http/piece_by_cid_200_response_count", "Counter of /piece/<piece-cid> 200 responses", stats.UnitDimensionless)
	HttpPieceByCid400ResponseCount   = stats.Int64("http/piece_by_cid_400_response_count", "Counter of /piece/<piece-cid> 400 responses", stats.UnitDimensionless)
	HttpPieceByCid404ResponseCount   = stats.Int64("http/piece_by_cid_404_response_count", "Counter of /piece/<piece-cid> 404 responses", stats.UnitDimensionless)
	HttpPieceByCid500ResponseCount   = stats.Int64("http/piece_by_cid_500_response_count", "Counter of /piece/<piece-cid> 500 responses", stats.UnitDimensionless)

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
	// http
	HttpPayloadByCidRequestCountView = &view.View{
		Measure:     HttpPayloadByCidRequestCount,
		Aggregation: view.Count(),
	}
	HttpPayloadByCidRequestDurationView = &view.View{
		Measure:     HttpPayloadByCidRequestDuration,
		Aggregation: defaultMillisecondsDistribution,
	}
	HttpPayloadByCid200ResponseCountView = &view.View{
		Measure:     HttpPayloadByCid200ResponseCount,
		Aggregation: view.Count(),
	}
	HttpPayloadByCid400ResponseCountView = &view.View{
		Measure:     HttpPayloadByCid400ResponseCount,
		Aggregation: view.Count(),
	}
	HttpPayloadByCid404ResponseCountView = &view.View{
		Measure:     HttpPayloadByCid404ResponseCount,
		Aggregation: view.Count(),
	}
	HttpPayloadByCid500ResponseCountView = &view.View{
		Measure:     HttpPayloadByCid500ResponseCount,
		Aggregation: view.Count(),
	}
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

	InfoView = &view.View{
		Name:        "info",
		Description: "Lotus node information",
		Measure:     LotusInfo,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{Version, Commit},
	}
	ChainNodeHeightView = &view.View{
		Measure:     ChainNodeHeight,
		Aggregation: view.LastValue(),
	}
	ChainNodeHeightExpectedView = &view.View{
		Measure:     ChainNodeHeightExpected,
		Aggregation: view.LastValue(),
	}
	ChainNodeWorkerHeightView = &view.View{
		Measure:     ChainNodeWorkerHeight,
		Aggregation: view.LastValue(),
	}
	BlockReceivedView = &view.View{
		Measure:     BlockReceived,
		Aggregation: view.Count(),
	}
	BlockValidationFailureView = &view.View{
		Measure:     BlockValidationFailure,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{FailureType},
	}
	BlockValidationSuccessView = &view.View{
		Measure:     BlockValidationSuccess,
		Aggregation: view.Count(),
	}
	BlockValidationDurationView = &view.View{
		Measure:     BlockValidationDurationMilliseconds,
		Aggregation: defaultMillisecondsDistribution,
	}
	BlockDelayView = &view.View{
		Measure: BlockDelay,
		TagKeys: []tag.Key{MinerID},
		Aggregation: func() *view.Aggregation {
			var bounds []float64
			for i := 5; i < 29; i++ { // 5-29s, step 1s
				bounds = append(bounds, float64(i*1000))
			}
			for i := 30; i < 60; i += 2 { // 30-58s, step 2s
				bounds = append(bounds, float64(i*1000))
			}
			for i := 60; i <= 300; i += 10 { // 60-300s, step 10s
				bounds = append(bounds, float64(i*1000))
			}
			bounds = append(bounds, 600*1000) // final cutoff at 10m
			return view.Distribution(bounds...)
		}(),
	}
	MessagePublishedView = &view.View{
		Measure:     MessagePublished,
		Aggregation: view.Count(),
	}
	MessageReceivedView = &view.View{
		Measure:     MessageReceived,
		Aggregation: view.Count(),
	}
	MessageValidationFailureView = &view.View{
		Measure:     MessageValidationFailure,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{FailureType, Local},
	}
	MessageValidationSuccessView = &view.View{
		Measure:     MessageValidationSuccess,
		Aggregation: view.Count(),
	}
	MessageValidationDurationView = &view.View{
		Measure:     MessageValidationDuration,
		Aggregation: defaultMillisecondsDistribution,
		TagKeys:     []tag.Key{MsgValid, Local},
	}
	MpoolGetNonceDurationView = &view.View{
		Measure:     MpoolGetNonceDuration,
		Aggregation: defaultMillisecondsDistribution,
	}
	MpoolGetBalanceDurationView = &view.View{
		Measure:     MpoolGetBalanceDuration,
		Aggregation: defaultMillisecondsDistribution,
	}
	MpoolAddTsDurationView = &view.View{
		Measure:     MpoolAddTsDuration,
		Aggregation: defaultMillisecondsDistribution,
	}
	MpoolAddDurationView = &view.View{
		Measure:     MpoolAddDuration,
		Aggregation: defaultMillisecondsDistribution,
	}
	MpoolPushDurationView = &view.View{
		Measure:     MpoolPushDuration,
		Aggregation: defaultMillisecondsDistribution,
	}
	PeerCountView = &view.View{
		Measure:     PeerCount,
		Aggregation: view.LastValue(),
	}
	PubsubPublishMessageView = &view.View{
		Measure:     PubsubPublishMessage,
		Aggregation: view.Count(),
	}
	PubsubDeliverMessageView = &view.View{
		Measure:     PubsubDeliverMessage,
		Aggregation: view.Count(),
	}
	PubsubRejectMessageView = &view.View{
		Measure:     PubsubRejectMessage,
		Aggregation: view.Count(),
	}
	PubsubDuplicateMessageView = &view.View{
		Measure:     PubsubDuplicateMessage,
		Aggregation: view.Count(),
	}
	PubsubRecvRPCView = &view.View{
		Measure:     PubsubRecvRPC,
		Aggregation: view.Count(),
	}
	PubsubSendRPCView = &view.View{
		Measure:     PubsubSendRPC,
		Aggregation: view.Count(),
	}
	PubsubDropRPCView = &view.View{
		Measure:     PubsubDropRPC,
		Aggregation: view.Count(),
	}
	APIRequestDurationView = &view.View{
		Measure:     APIRequestDuration,
		Aggregation: defaultMillisecondsDistribution,
		TagKeys:     []tag.Key{APIInterface, Endpoint},
	}
	VMFlushCopyDurationView = &view.View{
		Measure:     VMFlushCopyDuration,
		Aggregation: view.Sum(),
	}
	VMFlushCopyCountView = &view.View{
		Measure:     VMFlushCopyCount,
		Aggregation: view.Sum(),
	}
	VMApplyBlocksTotalView = &view.View{
		Measure:     VMApplyBlocksTotal,
		Aggregation: defaultMillisecondsDistribution,
	}
	VMApplyMessagesView = &view.View{
		Measure:     VMApplyMessages,
		Aggregation: defaultMillisecondsDistribution,
	}
	VMApplyEarlyView = &view.View{
		Measure:     VMApplyEarly,
		Aggregation: defaultMillisecondsDistribution,
	}
	VMApplyCronView = &view.View{
		Measure:     VMApplyCron,
		Aggregation: defaultMillisecondsDistribution,
	}
	VMApplyFlushView = &view.View{
		Measure:     VMApplyFlush,
		Aggregation: defaultMillisecondsDistribution,
	}
	VMSendsView = &view.View{
		Measure:     VMSends,
		Aggregation: view.LastValue(),
	}
	VMAppliedView = &view.View{
		Measure:     VMApplied,
		Aggregation: view.LastValue(),
	}

	// miner
	WorkerCallsStartedView = &view.View{
		Measure:     WorkerCallsStarted,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{TaskType, WorkerHostname},
	}
	WorkerCallsReturnedCountView = &view.View{
		Measure:     WorkerCallsReturnedCount,
		Aggregation: view.Count(),
		TagKeys:     []tag.Key{TaskType, WorkerHostname},
	}
	WorkerUntrackedCallsReturnedView = &view.View{
		Measure:     WorkerUntrackedCallsReturned,
		Aggregation: view.Count(),
	}
	WorkerCallsReturnedDurationView = &view.View{
		Measure:     WorkerCallsReturnedDuration,
		Aggregation: workMillisecondsDistribution,
		TagKeys:     []tag.Key{TaskType, WorkerHostname},
	}
	StorageFSAvailableView = &view.View{
		Measure:     StorageFSAvailable,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{StorageID},
	}
	StorageAvailableView = &view.View{
		Measure:     StorageAvailable,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{StorageID},
	}
	StorageReservedView = &view.View{
		Measure:     StorageReserved,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{StorageID},
	}
	StorageLimitUsedView = &view.View{
		Measure:     StorageLimitUsed,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{StorageID},
	}
	StorageCapacityBytesView = &view.View{
		Measure:     StorageCapacityBytes,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{StorageID},
	}
	StorageFSAvailableBytesView = &view.View{
		Measure:     StorageFSAvailableBytes,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{StorageID},
	}
	StorageAvailableBytesView = &view.View{
		Measure:     StorageAvailableBytes,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{StorageID},
	}
	StorageReservedBytesView = &view.View{
		Measure:     StorageReservedBytes,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{StorageID},
	}
	StorageLimitUsedBytesView = &view.View{
		Measure:     StorageLimitUsedBytes,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{StorageID},
	}
	StorageLimitMaxBytesView = &view.View{
		Measure:     StorageLimitMaxBytes,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{StorageID},
	}

	// splitstore
	SplitstoreMissView = &view.View{
		Measure:     SplitstoreMiss,
		Aggregation: view.Count(),
	}
	SplitstoreCompactionTimeSecondsView = &view.View{
		Measure:     SplitstoreCompactionTimeSeconds,
		Aggregation: view.LastValue(),
	}
	SplitstoreCompactionHotView = &view.View{
		Measure:     SplitstoreCompactionHot,
		Aggregation: view.LastValue(),
	}
	SplitstoreCompactionColdView = &view.View{
		Measure:     SplitstoreCompactionCold,
		Aggregation: view.Sum(),
	}
	SplitstoreCompactionDeadView = &view.View{
		Measure:     SplitstoreCompactionDead,
		Aggregation: view.Sum(),
	}
)

// DefaultViews is an array of OpenCensus views for metric gathering purposes
var DefaultViews = func() []*view.View {
	views := []*view.View{
		InfoView,
		PeerCountView,
		APIRequestDurationView,
		HttpPayloadByCidRequestCountView,
		HttpPayloadByCidRequestDurationView,
		HttpPayloadByCid200ResponseCountView,
		HttpPayloadByCid400ResponseCountView,
		HttpPayloadByCid404ResponseCountView,
		HttpPayloadByCid500ResponseCountView,
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

var ChainNodeViews = append([]*view.View{
	ChainNodeHeightView,
	ChainNodeHeightExpectedView,
	ChainNodeWorkerHeightView,
	BlockReceivedView,
	BlockValidationFailureView,
	BlockValidationSuccessView,
	BlockValidationDurationView,
	BlockDelayView,
	MessagePublishedView,
	MessageReceivedView,
	MessageValidationFailureView,
	MessageValidationSuccessView,
	MessageValidationDurationView,
	MpoolGetNonceDurationView,
	MpoolGetBalanceDurationView,
	MpoolAddTsDurationView,
	MpoolAddDurationView,
	MpoolPushDurationView,
	PubsubPublishMessageView,
	PubsubDeliverMessageView,
	PubsubRejectMessageView,
	PubsubDuplicateMessageView,
	PubsubRecvRPCView,
	PubsubSendRPCView,
	PubsubDropRPCView,
	VMFlushCopyCountView,
	VMFlushCopyDurationView,
	SplitstoreMissView,
	SplitstoreCompactionTimeSecondsView,
	SplitstoreCompactionHotView,
	SplitstoreCompactionColdView,
	SplitstoreCompactionDeadView,
	VMApplyBlocksTotalView,
	VMApplyMessagesView,
	VMApplyEarlyView,
	VMApplyCronView,
	VMApplyFlushView,
	VMSendsView,
	VMAppliedView,
}, DefaultViews...)

var MinerNodeViews = append([]*view.View{
	WorkerCallsStartedView,
	WorkerCallsReturnedCountView,
	WorkerUntrackedCallsReturnedView,
	WorkerCallsReturnedDurationView,
	StorageFSAvailableView,
	StorageAvailableView,
	StorageReservedView,
	StorageLimitUsedView,
	StorageFSAvailableBytesView,
	StorageAvailableBytesView,
	StorageReservedBytesView,
	StorageLimitUsedBytesView,
}, DefaultViews...)

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
