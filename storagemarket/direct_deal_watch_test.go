package storagemarket

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	lapi "github.com/filecoin-project/lotus/api"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	minertypes "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/storagemarket/logs"
	mocksealingpipeline "github.com/filecoin-project/boost/storagemarket/sealingpipeline/mock"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
)

// TestSealingStateGateEverySealerState pins the gate for every state lotus's
// sealer can report. The gate is the first rung of the evidence ladder: it is
// what decides whether the sealer alone can end the watch, or whether the chain
// has to be asked. Getting one state wrong either fails a healthy deal (a state
// wrongly read as failed) or hangs one forever (a settled state read as
// pending), so the whole list is enumerated rather than sampled.
//
// The completeness assertion below is the point of the test: a state added to
// sealing.ExistSectorStateList by a future lotus must fail here until someone
// decides which arm it belongs on, instead of silently inheriting "pending".
func TestSealingStateGateEverySealerState(t *testing.T) {
	const (
		pending = "pending"
		settled = "settled"
		failed  = "failed"
	)

	// Grouped by what the state is about, so a reader can see the shape of the
	// answer rather than 63 loose lines.
	// Keyed by the sealer-side type so the 63 literals need no conversions; the
	// API layer uses a distinct named string type for the same values.
	want := map[sealing.SectorState]string{
		// Not started, or being packed. The sealer still owns the sector.
		sealing.Empty:          pending, // deprecated in lotus, but still in the list
		sealing.WaitDeals:      pending,
		sealing.Packing:        pending,
		sealing.AddPiece:       pending,
		sealing.AddPieceFailed: pending,
		sealing.PackingFailed:  pending,

		// Pre-commit. Note that a failure here is NOT terminal for us: the
		// sealer retries a pre-commit, so failing the deal would be premature.
		sealing.GetTicket:            pending,
		sealing.PreCommit1:           pending,
		sealing.PreCommit2:           pending,
		sealing.PreCommitting:        pending, // deprecated
		sealing.PreCommitWait:        pending,
		sealing.SubmitPreCommitBatch: pending,
		sealing.PreCommitBatchWait:   pending,
		sealing.SealPreCommit1Failed: pending,
		sealing.SealPreCommit2Failed: pending,
		sealing.PreCommitFailed:      pending,

		// Commit. Failures are re-tried by the sealer as well -- a commit message
		// that did not land is not an unrecoverable failure.
		sealing.WaitSeed:              pending,
		sealing.Committing:            pending,
		sealing.CommitFinalize:        pending,
		sealing.CommitFinalizeFailed:  pending,
		sealing.SubmitCommit:          pending, // deprecated
		sealing.CommitWait:            pending,
		sealing.SubmitCommitAggregate: pending,
		sealing.CommitAggregateWait:   pending,
		sealing.ComputeProofFailed:    pending,
		sealing.RemoteCommitFailed:    pending,
		sealing.CommitFailed:          pending,
		sealing.FinalizeSector:        pending,
		sealing.FinalizeFailed:        pending,
		sealing.ReceiveSector:         pending, // external import, still sealing

		// On chain and proving. These are the settled states the ordinary path
		// ends in -- the sealer alone says "done", the chain says whether the
		// data landed.
		sealing.Proving:   settled,
		sealing.Available: settled, // proving CC, snap-dealable

		// Deal metadata, not sealing. The sector is proving; the sealer is only
		// re-recording which deals are in it.
		sealing.DealsExpired:   pending,
		sealing.RecoverDealIDs: pending,

		// Faults. A faulty sector is still a sector with our data in it, and a
		// fault can be recovered, so none of these is a verdict from us.
		sealing.Faulty:        pending,
		sealing.FaultReported: pending,
		sealing.FaultedFinal:  pending,

		// Termination. The sector is being (or has been) torn down -- settled, so
		// the chain read decides. This is the group that is arguably wrong today:
		// a Terminating sector will not prove our data for long, yet the watch
		// treats it as "ask the chain", and the chain still carries the weight.
		sealing.Terminating:       settled,
		sealing.TerminateWait:     settled,
		sealing.TerminateFinality: settled,
		sealing.TerminateFailed:   settled,
		sealing.Removing:          settled,
		sealing.Removed:           settled,
		sealing.RemoveFailed:      pending, // in lotus, runs RemoveFailed -> Removing

		// Snap deals, in flight. All of this is still the sealer's business; the
		// sectors already carry their original CC weight and no claim, which is
		// exactly what a rollback would look like, so judging early here would
		// be wrong.
		sealing.SnapDealsWaitDeals:    pending,
		sealing.SnapDealsAddPiece:     pending,
		sealing.SnapDealsPacking:      pending,
		sealing.UpdateReplica:         pending,
		sealing.ProveReplicaUpdate:    pending,
		sealing.SubmitReplicaUpdate:   pending,
		sealing.WaitMutable:           pending,
		sealing.ReplicaUpdateWait:     pending,
		sealing.FinalizeReplicaUpdate: pending,

		// Snap deals, landed. UpdateActivating/ReleaseSectorKey mean the update
		// message is on chain, so the sector carries the new verified weight and
		// the chain can be asked.
		sealing.UpdateActivating: settled,
		sealing.ReleaseSectorKey: settled,

		// Snap deals, failed. Nothing is settled -- lotus aborts the upgrade and
		// returns the sector to Proving with its original pieces, where the watch
		// picks it up again.
		sealing.SnapDealsAddPieceFailed:     pending,
		sealing.SnapDealsDealsExpired:       pending,
		sealing.SnapDealsRecoverDealIDs:     pending,
		sealing.ReplicaUpdateFailed:         pending,
		sealing.ReleaseSectorKeyFailed:      pending,
		sealing.FinalizeReplicaUpdateFailed: pending,
		sealing.AbortUpgrade:                pending,

		// The one state the sealer alone can rule on.
		sealing.FailedUnrecoverable: failed,
	}

	visited := map[sealing.SectorState]bool{}
	for state := range sealing.ExistSectorStateList {
		visited[state] = true

		w, ok := want[state]
		if !ok {
			t.Errorf("sealer state %q is in lotus's list but the gate test does not classify it -- decide which arm it belongs on", state)
			continue
		}

		var got string
		switch sealingStateGateFor(lapi.SectorState(state)) {
		case sealingStatePending:
			got = pending
		case sealingStateSettled:
			got = settled
		case sealingStateFailed:
			got = failed
		}
		require.Equal(t, w, got, "state %q", state)
	}

	// Both directions: nothing classified that lotus does not have (a state
	// renamed in lotus would otherwise linger here forever), and nothing in
	// lotus that this test did not visit.
	for state := range want {
		require.True(t, visited[state], "test classifies state %q, which lotus's ExistSectorStateList does not contain", state)
	}
	require.Len(t, want, len(sealing.ExistSectorStateList),
		"every state in sealing.ExistSectorStateList must be classified exactly once")
}

// TestSealingStateGateUndefined covers the state a sealer reports before it has
// anything to say. UndefinedSectorState is the empty string, and it is not in
// ExistSectorStateList, so the exhaustive test above cannot reach it.
func TestSealingStateGateUndefined(t *testing.T) {
	require.Equal(t, sealingStatePending, sealingStateGateFor(lapi.SectorState(sealing.UndefinedSectorState)))
	require.Equal(t, sealingStatePending, sealingStateGateFor(lapi.SectorState("Garbage")))
}

// chainFixture is a DirectDealsProvider wired to mocks, with a deal already
// packed into a sector.
type chainFixture struct {
	ddp    *DirectDealsProvider
	sps    *mocksealingpipeline.MockAPI
	node   *lotusmocks.MockFullNode
	logsDB *db.LogsDB
}

func newChainFixture(t *testing.T) *chainFixture {
	t.Helper()

	ctrl := gomock.NewController(t)
	sps := mocksealingpipeline.NewMockAPI(ctrl)
	node := lotusmocks.NewMockFullNode(ctrl)

	return &chainFixture{
		ddp: &DirectDealsProvider{
			ctx:         context.Background(),
			sps:         sps,
			fullnodeApi: node,
			config:      DDPConfig{},
		},
		sps:  sps,
		node: node,
	}
}

func (f *chainFixture) deal() *types.DirectDeal {
	return &types.DirectDeal{
		ID:           uuid.New(),
		PieceCID:     ourPieceCID,
		PieceSize:    ourPieceSize,
		AllocationID: verifreg9types.AllocationId(11),
		SectorID:     sealingSectorNum,
		Offset:       0,
	}
}

// expectSealerState stages one SectorsStatus reply.
func (f *chainFixture) expectSealerState(state sealing.SectorState, pieces []lapi.SectorPiece) {
	f.sps.EXPECT().SectorsStatus(gomock.Any(), sealingSectorNum, false).
		Return(lapi.SectorInfo{State: lapi.SectorState(state), Pieces: pieces}, nil)
}

func (f *chainFixture) expectSectorOnChain(si *minertypes.SectorOnChainInfo) {
	f.node.EXPECT().StateSectorGetInfo(gomock.Any(), gomock.Any(), sealingSectorNum, gomock.Any()).
		Return(si, nil)
}

func (f *chainFixture) expectClaim(claim *verifreg9types.Claim) {
	f.node.EXPECT().StateGetClaim(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(claim, nil)
}

// expectNoChainReads asserts that the poll never asks the chain anything, i.e.
// the sealer answered by itself.
func (f *chainFixture) expectNoChainReads() {
	f.node.EXPECT().StateSectorGetInfo(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	f.node.EXPECT().StateGetClaim(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
}

// TestCheckSealingOncePendingStates is the "sealer still working" half of the
// state matrix: for every pending state the poll must return immediately with
// neither a verdict nor a chain read, whatever the chain happens to say. A
// pending poll that reads the chain is how a snap in flight gets mistaken for a
// rollback: mid-upgrade the sector still has its original CC weight and no
// claim.
func TestCheckSealingOncePendingStates(t *testing.T) {
	pendingStates := []sealing.SectorState{
		sealing.WaitDeals,
		sealing.AddPiece,
		sealing.AddPieceFailed,
		sealing.PreCommit1,
		sealing.PreCommit2,
		sealing.SubmitPreCommitBatch,
		sealing.PreCommitBatchWait,
		sealing.WaitSeed,
		sealing.Committing,
		sealing.CommitFinalize,
		sealing.CommitFinalizeFailed,
		sealing.SubmitCommitAggregate,
		sealing.CommitAggregateWait,
		sealing.FinalizeSector,
		sealing.FinalizeFailed,
		sealing.CommitFailed,
		sealing.ComputeProofFailed,
		sealing.DealsExpired,
		sealing.RecoverDealIDs,
		sealing.Faulty,
		sealing.FaultReported,
		sealing.FaultedFinal,
		sealing.RemoveFailed,
		sealing.SnapDealsWaitDeals,
		sealing.SnapDealsAddPiece,
		sealing.SnapDealsPacking,
		sealing.UpdateReplica,
		sealing.ProveReplicaUpdate,
		sealing.SubmitReplicaUpdate,
		sealing.WaitMutable,
		sealing.ReplicaUpdateWait,
		sealing.FinalizeReplicaUpdate,
		sealing.ReplicaUpdateFailed,
		sealing.FinalizeReplicaUpdateFailed,
		sealing.ReleaseSectorKeyFailed,
		sealing.SnapDealsAddPieceFailed,
		sealing.AbortUpgrade,
		sealing.ReceiveSector,
	}

	for _, state := range pendingStates {
		t.Run(string(state), func(t *testing.T) {
			f := newChainFixture(t)
			// Empty piece list: a mid-upgrade sector's list looks exactly like a
			// rolled-back one, so nothing here may be read from it.
			f.expectSealerState(state, nil)
			f.expectNoChainReads()

			step := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())

			require.True(t, step.observed)
			require.Equal(t, string(state), string(step.state))
			require.False(t, step.done, "a pending sealer state must not end the watch")
			require.Nil(t, step.err)
		})
	}
}

// TestCheckSealingOnceUnrecoverable is the one sealer-only verdict.
func TestCheckSealingOnceUnrecoverable(t *testing.T) {
	f := newChainFixture(t)
	f.expectSealerState(sealing.FailedUnrecoverable, nil)
	f.expectNoChainReads()

	step := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())

	require.True(t, step.done)
	require.NotNil(t, step.err)
	require.Equal(t, types.DealRetryFatal, step.err.retry)
	require.ErrorIs(t, step.err.error, ErrSectorSealingFailed)
}

// settledStates is every sealer state the gate hands to the chain, i.e. every
// state the watch can be in when it reads the chain.
var settledStates = []sealing.SectorState{
	sealing.Proving,
	sealing.Available,
	sealing.UpdateActivating,
	sealing.ReleaseSectorKey,
	sealing.Removed,
	sealing.Removing,
	sealing.Terminating,
	sealing.TerminateWait,
	sealing.TerminateFinality,
	sealing.TerminateFailed,
}

// TestCheckSealingOnceSettledStates runs the full chain-read matrix against
// every settled sealer state. The chain shape -- not the sealer state, and not
// an epoch -- is what decides, so the same shape must give the same answer from
// every one of these states. That equivalence is what makes a CC -> snap across
// nv29 and a plain post-nv29 deal indistinguishable, which is intended: they
// are the same on chain.
func TestCheckSealingOnceSettledStates(t *testing.T) {
	pieces := pieceListWith(ourPieceCID, ourPieceSize, 0)

	tests := []struct {
		name        string
		sector      *minertypes.SectorOnChainInfo
		claim       *verifreg9types.Claim
		pieces      []lapi.SectorPiece
		wantDone    bool
		wantFatal   bool
		wantCase    sealingCase
		wantNoError bool
		// curio marks a sealer that does not fill SectorInfo.Pieces, so the
		// empty list below means "not reported", not "the sector is empty".
		curio bool
	}{
		{
			name:        "post-nv29: weight, no claim",
			sector:      sectorWithWeights(zeroWeight, unitWeight),
			pieces:      pieces,
			wantDone:    true,
			wantNoError: true,
			wantCase:    casePostSealedSector,
		},
		{
			name:        "post-nv29: deal weight (unverified piece), no claim",
			sector:      sectorWithWeights(unitWeight, zeroWeight),
			pieces:      pieces,
			wantDone:    true,
			wantNoError: true,
			wantCase:    casePostSealedSector,
		},
		{
			name:        "post-nv29 on a sealer that reports no piece list",
			sector:      sectorWithWeights(zeroWeight, unitWeight),
			pieces:      nil,
			curio:       true,
			wantDone:    true,
			wantNoError: true,
			wantCase:    casePostSealedSector,
		},
		{
			name:        "nv28: claim on this sector, weight still zero",
			sector:      sectorWithWeights(zeroWeight, zeroWeight),
			claim:       claimOn(sealingSectorNum),
			pieces:      pieces,
			wantDone:    true,
			wantNoError: true,
			wantCase:    caseNv28SealedSector,
		},
		{
			name:        "nv28: claim on this sector, weight present",
			sector:      sectorWithWeights(zeroWeight, unitWeight),
			claim:       claimOn(sealingSectorNum),
			pieces:      pieces,
			wantDone:    true,
			wantNoError: true,
			wantCase:    caseNv28SealedSector,
		},
		{
			name:      "rollback: no weight, no claim, our piece gone from the list",
			sector:    sectorWithWeights(zeroWeight, zeroWeight),
			pieces:    pieceListWith(otherPieceCID, ourPieceSize, 0),
			wantDone:  true,
			wantFatal: true,
		},
		{
			name:      "rollback: no weight, no claim, empty list",
			sector:    sectorWithWeights(zeroWeight, zeroWeight),
			pieces:    nil,
			wantDone:  true,
			wantFatal: true,
		},
		{
			name:     "cc still staging: no weight, no claim, our piece in the list",
			sector:   sectorWithWeights(zeroWeight, zeroWeight),
			pieces:   pieces,
			wantDone: false,
		},
		{
			name:     "no weight, no claim, sealer reports no list -- never a rollback",
			sector:   sectorWithWeights(zeroWeight, zeroWeight),
			pieces:   nil,
			curio:    true,
			wantDone: false,
		},
	}

	for _, state := range settledStates {
		for _, tt := range tests {
			t.Run(string(state)+"/"+tt.name, func(t *testing.T) {
				f := newChainFixture(t)
				f.ddp.config.Curio = tt.curio
				f.expectSealerState(state, tt.pieces)
				f.expectSectorOnChain(tt.sector)
				f.expectClaim(tt.claim)

				step := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())

				require.Equal(t, string(state), string(step.state))
				require.Equal(t, tt.wantDone, step.done, "done")
				if tt.wantNoError {
					require.Nil(t, step.err)
					require.Equal(t, tt.wantCase, step.caseID)
				}
				if tt.wantFatal {
					require.NotNil(t, step.err)
					require.Equal(t, types.DealRetryFatal, step.err.retry)
				}
				if !tt.wantDone {
					require.Nil(t, step.err)
				}
			})
		}
	}
}

// TestCheckSealingOnceOnChainFailures covers the failures the chain can throw
// at the watch: the commit/update message never landed, so there is nothing to
// read. None of these is a verdict -- the message may be in a future block --
// and failing the deal here would discard a sector that is still coming.
func TestCheckSealingOnceOnChainFailures(t *testing.T) {
	t.Run("sector state read fails", func(t *testing.T) {
		f := newChainFixture(t)
		f.sps.EXPECT().SectorsStatus(gomock.Any(), sealingSectorNum, false).
			Return(lapi.SectorInfo{}, errors.New("sealer unreachable"))
		f.expectNoChainReads()

		step := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())

		require.False(t, step.observed, "a failed read learned nothing about the state")
		require.False(t, step.done)
		require.Nil(t, step.err)
	})

	t.Run("StateSectorGetInfo fails: the commit message has not landed", func(t *testing.T) {
		f := newChainFixture(t)
		f.expectSealerState(sealing.Proving, nil)
		f.node.EXPECT().StateSectorGetInfo(gomock.Any(), gomock.Any(), sealingSectorNum, gomock.Any()).
			Return(nil, errors.New("actor not found"))
		f.node.EXPECT().StateGetClaim(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

		step := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())

		require.True(t, step.observed)
		require.False(t, step.done)
		require.Nil(t, step.err)
	})

	t.Run("StateGetClaim fails", func(t *testing.T) {
		f := newChainFixture(t)
		f.expectSealerState(sealing.Proving, nil)
		f.expectSectorOnChain(sectorWithWeights(zeroWeight, unitWeight))
		f.node.EXPECT().StateGetClaim(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, errors.New("node lagging"))

		step := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())

		require.False(t, step.done)
		require.Nil(t, step.err)
	})

	t.Run("the sector is not on chain yet, no claim: node lag, keep polling", func(t *testing.T) {
		f := newChainFixture(t)
		f.expectSealerState(sealing.Proving, nil)
		f.expectSectorOnChain(nil)
		f.expectClaim(nil)

		step := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())

		require.False(t, step.done)
		require.Nil(t, step.err)
	})

	t.Run("the sector is not on chain, but the claim points elsewhere: fatal", func(t *testing.T) {
		f := newChainFixture(t)
		f.expectSealerState(sealing.Proving, nil)
		f.expectSectorOnChain(nil)
		f.expectClaim(claimOn(otherSectorNum))

		step := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())

		require.True(t, step.done)
		require.NotNil(t, step.err)
		require.Equal(t, types.DealRetryFatal, step.err.retry)
	})
}

// newWatchFixture is newChainFixture with a real DealLogger, so the watch loop
// itself can be run and its log lines inspected.
func newWatchFixture(t *testing.T) *chainFixture {
	t.Helper()
	f := newChainFixture(t)

	ctx := context.Background()
	tmp := db.CreateTestTmpDB(t)
	require.NoError(t, db.CreateAllBoostTables(ctx, tmp, tmp))
	require.NoError(t, migrations.Migrate(tmp))
	f.logsDB = db.NewLogsDB(tmp)
	f.ddp.dealLogger = logs.NewDealLogger(f.logsDB)
	f.ddp.directDealsDB = db.NewDirectDealsDB(tmp)

	return f
}

// insertDeal stores the deal the watch will re-read each poll.
func (f *chainFixture) insertDeal(t *testing.T, deal *types.DirectDeal) {
	t.Helper()
	require.NoError(t, f.ddp.directDealsDB.Insert(context.Background(), deal))
}

// TestWatchSealingUpdatesStopsOnShutdown runs the real watch loop. Its first
// poll learns nothing (the sealer is still sealing), so the watch has to park on
// the ticker -- and a cancelled context, i.e. boost shutting down, must end the
// watch as an auto-retry rather than as a failure. Deals are resumed on restart,
// so shutting down must never look like a verdict.
func TestWatchSealingUpdatesStopsOnShutdown(t *testing.T) {
	f := newWatchFixture(t)
	f.expectSealerState(sealing.AddPiece, nil)
	f.expectNoChainReads()

	ctx, cancel := context.WithCancel(context.Background())
	f.ddp.ctx = ctx
	go func() {
		// Let the first poll happen, then shut boost down.
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	entry := f.deal()
	f.insertDeal(t, entry)
	derr := f.ddp.watchSealingUpdates(entry)

	require.NotNil(t, derr)
	require.Equal(t, types.DealRetryAuto, derr.retry)
	require.ErrorIs(t, derr.error, context.Canceled)

	// The state was reported to the operator.
	logsForDeal, err := f.logsDB.Logs(context.Background(), entry.ID)
	require.NoError(t, err)
	require.Contains(t, logMessages(logsForDeal), "current sealing state")
	require.NotContains(t, logMessages(logsForDeal), "the deal's sector is committed and live on chain")
}

// TestWatchSealingUpdatesReturnsImmediatelyWhenDone pins the fast path: a sector
// that is already settled when the watch starts must not wait for a tick.
func TestWatchSealingUpdatesReturnsImmediatelyWhenDone(t *testing.T) {
	f := newWatchFixture(t)
	f.expectSealerState(sealing.Proving, pieceListWith(ourPieceCID, ourPieceSize, 0))
	f.expectSectorOnChain(sectorWithWeights(zeroWeight, unitWeight))
	f.expectClaim(nil)

	entry := f.deal()
	f.insertDeal(t, entry)
	// N.B. compare against nil rather than require.NoError: the watch returns a
	// *dealMakingError, and a nil one in an error interface is not nil.
	require.Nil(t, f.ddp.watchSealingUpdates(entry))

	logsForDeal, err := f.logsDB.Logs(context.Background(), entry.ID)
	require.NoError(t, err)
	msgs := logMessages(logsForDeal)
	require.Contains(t, msgs, "current sealing state")
	require.Contains(t, msgs, "the deal's sector is committed and live on chain")
}

// TestWatchSealingUpdatesFailsOnUnrecoverable pins that a terminal sealer
// failure reaches the caller as a fatal, with the deal failed rather than left
// parked.
func TestWatchSealingUpdatesFailsOnUnrecoverable(t *testing.T) {
	f := newWatchFixture(t)
	f.expectSealerState(sealing.FailedUnrecoverable, nil)
	f.expectNoChainReads()

	entry := f.deal()
	f.insertDeal(t, entry)
	derr := f.ddp.watchSealingUpdates(entry)

	require.NotNil(t, derr)
	require.Equal(t, types.DealRetryFatal, derr.retry)
	require.ErrorIs(t, derr.error, ErrSectorSealingFailed)
	require.Contains(t, logMessages(mustLogs(t, f.logsDB, entry.ID)), "current sealing state")
}

func logMessages(l []db.DealLog) []string {
	msgs := make([]string, 0, len(l))
	for _, e := range l {
		msgs = append(msgs, e.LogMsg)
	}
	return msgs
}

func mustLogs(t *testing.T, ldb *db.LogsDB, id uuid.UUID) []db.DealLog {
	t.Helper()
	l, err := ldb.Logs(context.Background(), id)
	require.NoError(t, err)
	return l
}

// TestWatchSealingUpdatesStopsWhenTheDealIsAlreadyComplete pins the database read
// that opens each poll. The watch can run for as long as a sector takes to seal,
// and the deal row is the authority on whether the deal is done -- so if it is
// already complete there is nothing to wait for, and polling a sector for a
// finished deal would hold the thread open for no reason.
func TestWatchSealingUpdatesStopsWhenTheDealIsAlreadyComplete(t *testing.T) {
	f := newWatchFixture(t)

	// The sealer is mid-seal: only the database knows the deal is already done,
	// so a watch that trusted the sealer alone would park on the ticker here.
	entry := f.deal()
	entry.Checkpoint = dealcheckpoints.Complete
	f.insertDeal(t, entry)

	require.Nil(t, f.ddp.watchSealingUpdates(entry))

	// It stopped without asking the sealer or the chain anything.
	logsForDeal, err := f.logsDB.Logs(context.Background(), entry.ID)
	require.NoError(t, err)
	require.Empty(t, logMessages(logsForDeal))
}

// TestWatchSealingUpdatesPollsWhileTheDealIsIncomplete is the other side: a deal
// row that is not complete must not short-circuit the watch.
func TestWatchSealingUpdatesPollsWhileTheDealIsIncomplete(t *testing.T) {
	f := newWatchFixture(t)
	f.expectSealerState(sealing.AddPiece, nil)
	f.expectNoChainReads()

	ctx, cancel := context.WithCancel(context.Background())
	f.ddp.ctx = ctx
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	entry := f.deal()
	entry.Checkpoint = dealcheckpoints.AddedPiece
	f.insertDeal(t, entry)

	derr := f.ddp.watchSealingUpdates(entry)
	require.NotNil(t, derr)
	require.Equal(t, types.DealRetryAuto, derr.retry)
}
