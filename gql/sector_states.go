package gql

var (
	normalSectors         map[string]struct{}
	normalErredSectors    map[string]struct{}
	snapdealsSectors      map[string]struct{}
	snapdealsErredSectors map[string]struct{}

	allSectorStates []string

	// ordered
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
	}

	normalErredSectorsList = []string{
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
	}

	snapdealsErredSectorsList = []string{
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
	allSectorStates = append(allSectorStates, normalSectorsList...)
	allSectorStates = append(allSectorStates, normalErredSectorsList...)
	allSectorStates = append(allSectorStates, snapdealsSectorsList...)
	allSectorStates = append(allSectorStates, snapdealsErredSectorsList...)

	normalSectors = make(map[string]struct{})
	normalErredSectors = make(map[string]struct{})
	snapdealsSectors = make(map[string]struct{})
	snapdealsErredSectors = make(map[string]struct{})

	for _, s := range normalSectorsList {
		normalSectors[s] = struct{}{}
	}
	for _, s := range normalErredSectorsList {
		normalErredSectors[s] = struct{}{}
	}
	for _, s := range snapdealsSectorsList {
		snapdealsSectors[s] = struct{}{}
	}
	for _, s := range snapdealsErredSectorsList {
		snapdealsErredSectors[s] = struct{}{}
	}
}
