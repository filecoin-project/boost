package gql

var (
	normalSectors    map[string]struct{}
	snapdealsSectors map[string]struct{}

	normalSectorsList = []string{
		"WaitDeals",
		"AddPiece",
		"Packing",
		"GetTicket",
		"PreCommit1",
		"PreCommit2",
		"PreCommitting",
		"PreCommitWait",
		"SubmitPreCommitBatch",
		"PreCommitBatchWait",
		"WaitSeed",
		"Committing",
		"CommitFinalize",
		"CommitFinalizeFailed",
		"SubmitCommit",
		"CommitWait",
		"SubmitCommitAggregate",
		"CommitAggregateWait",
		"FinalizeSector",
		"Proving",

		// error modes
		"FailedUnrecoverable",
		"AddPieceFailed",
		"SealPreCommit1Failed",
		"SealPreCommit2Failed",
		"PreCommitFailed",
		"ComputeProofFailed",
		"CommitFailed",
		"PackingFailed",
		"FinalizeFailed",
		"DealsExpired",
		"RecoverDealIDs",

		"Faulty",
		"FaultReported",
		"FaultedFinal",
		"Terminating",
		"TerminateWait",
		"TerminateFinality",
		"TerminateFailed",
		"Removing",
		"RemoveFailed",
		"Removed",
	}

	snapdealsSectorsList = []string{
		"Available",
		"SnapDealsWaitDeals",
		"SnapDealsAddPiece",
		"SnapDealsPacking",
		"UpdateReplica",
		"ProveReplicaUpdate",
		"SubmitReplicaUpdate",
		"ReplicaUpdateWait",
		"FinalizeReplicaUpdate",
		"UpdateActivating",
		"ReleaseSectorKey",

		// snap deals error modes
		"SnapDealsAddPieceFailed",
		"SnapDealsDealsExpired",
		"SnapDealsRecoverDealIDs",
		"AbortUpgrade",
		"ReplicaUpdateFailed",
		"ReleaseSectorKeyFailed",
		"FinalizeReplicaUpdateFailed",
	}
)

func init() {
	normalSectors = make(map[string]struct{})
	snapdealsSectors = make(map[string]struct{})

	for _, s := range normalSectorsList {
		normalSectors[s] = struct{}{}
	}
	for _, s := range snapdealsSectorsList {
		snapdealsSectors[s] = struct{}{}
	}
}
