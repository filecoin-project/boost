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
		removed = "removed"
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

		// Termination. Only the removed end is ours to rule on: a sector the
		// sealer has deleted never appears on chain, so waiting for it is waiting
		// for something that cannot happen. The rest of the group is settled --
		// those sectors are still on chain carrying their weight, so the chain
		// decides them, and a removal in progress reaches Removed on its own.
		sealing.Terminating:       settled,
		sealing.TerminateWait:     settled,
		sealing.TerminateFinality: settled,
		sealing.TerminateFailed:   settled,
		sealing.Removing:          settled,
		sealing.Removed:           removed,
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
		case sealingStateRemoved:
			got = removed
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
// state the watch can be in when it reads the chain. The termination group
// (except Removed) is in here on purpose: those sectors are on chain with their
// weight, so the chain is what decides them and the watch keeps polling.
var settledStates = []sealing.SectorState{
	sealing.Proving,
	sealing.Available,
	sealing.UpdateActivating,
	sealing.ReleaseSectorKey,
	sealing.Removing,
	sealing.Terminating,
	sealing.TerminateWait,
	sealing.TerminateFinality,
	sealing.TerminateFailed,
}

// removedStates is every sealer state the gate ends the watch on by itself.
// These are checked separately from settledStates because they never reach the
// chain read, so the shape of the chain must not appear to matter for them.
var removedStates = []sealing.SectorState{
	sealing.Removed,
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
	}{
		{
			// nv28, fresh seal: both epochs pre-nv29, so the claim exists and
			// confirms the deal on its own.
			name:        "nv28 fresh seal: deal weight, claim on this sector, our piece in the list",
			sector:      sectorWithWeights(unitWeight, zeroWeight),
			claim:       claimOn(sealingSectorNum),
			pieces:      pieces,
			wantDone:    true,
			wantNoError: true,
			wantCase:    caseNv28SealedSector,
		},
		{
			// nv28, plain (unverified) deal: no verifreg claim exists in any
			// era, so the weight settles it.
			name:        "nv28 plain deal: deal weight, no claim, our piece in the list",
			sector:      sectorWithWeights(unitWeight, zeroWeight),
			pieces:      pieces,
			wantDone:    true,
			wantNoError: true,
			wantCase:    casePostSealedSector,
		},
		{
			// The success the snap path exists for: a CC sector upgraded before
			// nv29, where the verified arm carries the piece spacetime and the
			// claim the upgrade wrote still points at this sector. Both pieces of
			// evidence agree, and the deal is done.
			name:        "nv28 CC -> snap: verified weight, claim on this sector, our piece in the list",
			sector:      sectorWithWeights(zeroWeight, unitWeight),
			claim:       claimOn(sealingSectorNum),
			pieces:      pieces,
			wantDone:    true,
			wantNoError: true,
			wantCase:    caseNv28SealedSector,
		},
		{
			// The nv28 shape of the aborted snap: the sector carries
			// somebody's data and the list says it is not ours.
			name:      "nv28 aborted snap: deal weight, no claim, our piece missing from the list",
			sector:    sectorWithWeights(unitWeight, zeroWeight),
			pieces:    pieceListWith(otherPieceCID, ourPieceSize, 0),
			wantDone:  true,
			wantFatal: true,
		},
		{
			// The ordering guard: before nv29 the claim is the authority, so an
			// absent piece cannot contradict a claim that hits this sector.
			name:        "nv28: claim on this sector, our piece missing from the list -- the claim still settles it",
			sector:      sectorWithWeights(zeroWeight, unitWeight),
			claim:       claimOn(sealingSectorNum),
			pieces:      pieceListWith(otherPieceCID, ourPieceSize, 0),
			wantDone:    true,
			wantNoError: true,
			wantCase:    caseNv28SealedSector,
		},
		{
			// nv29, fresh seal across the upgrade: the pre-commit landed before
			// nv29 and the prove-commit after it, so the sector is dated nv29
			// and FIP-0118 leaves it no claim. The weight and the piece list are
			// all there is, and they have to be enough.
			name:        "nv29 fresh seal across the upgrade: verified weight, no claim, our piece in the list",
			sector:      sectorWithWeights(zeroWeight, unitWeight),
			pieces:      pieces,
			wantDone:    true,
			wantNoError: true,
			wantCase:    casePostSealedSector,
		},
		{
			// The same deal a second earlier, judged the same: a plain deal's
			// file lands in the unverified arm.
			name:        "nv29 plain deal: deal weight, no claim, our piece in the list",
			sector:      sectorWithWeights(unitWeight, zeroWeight),
			pieces:      pieces,
			wantDone:    true,
			wantNoError: true,
			wantCase:    casePostSealedSector,
		},
		{
			// The nv29 shape of the aborted snap: another piece took the sector
			// after ours fell back.
			name:      "nv29 aborted snap: verified weight, no claim, our piece missing from the list",
			sector:    sectorWithWeights(zeroWeight, unitWeight),
			pieces:    pieceListWith(otherPieceCID, ourPieceSize, 0),
			wantDone:  true,
			wantFatal: true,
		},
		{
			// Our piece is in the sector, but not where this deal's record says
			// it is. The data is there and the offset is wrong, so completing
			// the deal would index the wrong bytes: a contradiction between our
			// record and the sector, and terminal either way.
			name:      "nv29 aborted snap: verified weight, our piece at an offset we did not add it at",
			sector:    sectorWithWeights(zeroWeight, unitWeight),
			pieces:    pieceListWith(ourPieceCID, ourPieceSize, ourPieceSize),
			wantDone:  true,
			wantFatal: true,
		},
		{
			// The piece is the right CID at the right offset, but the sector's
			// copy of it is a different size -- so it is a different piece that
			// happens to hash alike at the front, not ours. All three of
			// (PieceCID, Size, offset) have to line up or the deal is completed
			// against bytes that are not the client's.
			name:      "nv29 aborted snap: our piece's CID at our offset, but a different size",
			sector:    sectorWithWeights(zeroWeight, unitWeight),
			pieces:    []lapi.SectorPiece{pieceEntry(ourPieceCID, ourPieceSize*2)},
			wantDone:  true,
			wantFatal: true,
		},
		{
			// A sealer that does answer with a list is a sealer whose list can
			// be read, non-empty sector and all, and which of the two sealers
			// is behind the API makes no difference to the answer.
			name:      "nv29 aborted snap: a reported list without our piece is terminal",
			sector:    sectorWithWeights(zeroWeight, unitWeight),
			pieces:    pieceListWith(otherPieceCID, ourPieceSize, 0),
			wantDone:  true,
			wantFatal: true,
		},
		{
			// Two identical pieces in one sector -- two clients uploaded the same
			// file -- and this deal's is the first copy. Duplicates must not make
			// the check ambiguous: the deal resolves to the offset boost recorded
			// when the piece was added, and the second copy is simply not it.
			name:   "nv29: two identical pieces in the sector, the deal resolves to its own copy",
			sector: sectorWithWeights(zeroWeight, unitWeight),
			pieces: []lapi.SectorPiece{
				pieceEntry(ourPieceCID, ourPieceSize), // offset 0, this deal's copy
				pieceEntry(otherPieceCID, ourPieceSize),
				pieceEntry(ourPieceCID, ourPieceSize), // the duplicate
			},
			wantDone:    true,
			wantNoError: true,
			wantCase:    casePostSealedSector,
		},
		{
			// The post-nv29 deal on a sealer whose list is not readable at all:
			// the weight is the only evidence there is, and it has to be
			// enough, or every such deal hangs.
			name:        "nv29 on a sealer that reports no piece list: the weight decides",
			sector:      sectorWithWeights(zeroWeight, unitWeight),
			pieces:      nil,
			wantDone:    true,
			wantNoError: true,
			wantCase:    casePostSealedSector,
		},
		{
			name:      "rollback: no weight, no claim, our piece gone from the list",
			sector:    sectorWithWeights(zeroWeight, zeroWeight),
			pieces:    pieceListWith(otherPieceCID, ourPieceSize, 0),
			wantDone:  true,
			wantFatal: true,
		},
		{
			// An empty answer is not a list of zero pieces, so it cannot say
			// "our piece is absent": it says nothing, and the deal polls.
			name:     "no weight, no claim, empty list",
			sector:   sectorWithWeights(zeroWeight, zeroWeight),
			pieces:   nil,
			wantDone: false,
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
			wantDone: false,
		},
	}

	for _, state := range settledStates {
		for _, tt := range tests {
			t.Run(string(state)+"/"+tt.name, func(t *testing.T) {
				f := newChainFixture(t)
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
					require.ErrorIs(t, step.err.error, ErrSectorSealingFailed)
				}
				if !tt.wantDone {
					require.Nil(t, step.err)
				}
			})
		}
	}
}

// TestCheckSealingOnceRemovedState is the one state the sealer alone ends the
// watch on without a chain read: the sector was deleted. For that state the
// watch must fail the deal and must not consult the chain at all.
//
// Both eras are run against it because the state is the whole verdict here. A
// nv28 sector with its claim, a nv29 sector with its verified weight -- whatever
// the chain holds, a sector the provider has deleted is not where this deal's
// data lives, and if it was deleted before activation it has no chain record at
// all, so a read would report nothing and the watch would wait forever.
//
// The rest of the termination group is not here: those sectors are still on
// chain carrying their weight, so the chain decides them (the settled matrix
// covers them).
func TestCheckSealingOnceRemovedState(t *testing.T) {
	era := map[string]struct {
		sector *minertypes.SectorOnChainInfo
		claim  *verifreg9types.Claim
	}{
		// nv28: the claim is the authority and it hits this sector. It must not
		// be enough to complete a deal whose sector has been deleted -- a sector
		// deleted before activation has no chain record at all.
		"nv28": {sectorWithWeights(zeroWeight, unitWeight), claimOn(sealingSectorNum)},
		// nv29: weight in the verified arm, no claim, our piece in the list.
		// The chain would say "done" if it were ever read.
		"nv29": {sectorWithWeights(zeroWeight, unitWeight), nil},
	}

	for _, state := range removedStates {
		for name, e := range era {
			t.Run(string(state)+"/"+name, func(t *testing.T) {
				// The chain shape this era would have offered, spelled out so
				// the test cannot pass by the chain simply being empty: it is a
				// shape the verdict calls done.
				if _, v := verdict(e.sector, e.claim, pieceListWith(ourPieceCID, ourPieceSize, 0)); v != sealingDone {
					t.Fatalf("fixture era %s is not a chain shape the verdict would call done, so this test proves nothing", name)
				}

				f := newChainFixture(t)
				// The list holds our piece at our offset: even a sector that
				// looks perfect on the sealer's own evidence is terminal once
				// the sealer has deleted it.
				f.expectSealerState(state, pieceListWith(ourPieceCID, ourPieceSize, 0))
				f.expectNoChainReads()

				step := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())

				require.True(t, step.observed)
				require.Equal(t, string(state), string(step.state))
				require.True(t, step.done, "a deleted sector must not be watched forever")
				require.NotNil(t, step.err)
				require.Equal(t, types.DealRetryFatal, step.err.retry)
				require.ErrorIs(t, step.err.error, ErrSectorRemovedByProvider)
			})
		}
	}
}

// TestTerminationInProgressIsSettledNotTerminal pins the half of the
// termination group the watch does *not* rule on, so the boundary is visible
// rather than implied by omission.
//
// A sector being terminated is still on chain carrying its weight, so the chain
// read says the deal's data is there and the deal completes. That is a knowing
// trade: the ordering that ends the deal instead would have to be inferred from
// a state that can also mean "the termination message is still in flight", and
// the removed end of the group is what catches a sector that never reaches the
// chain at all. If the ordering is ever treated as terminal here, this test is
// the one that has to change.
func TestTerminationInProgressIsSettledNotTerminal(t *testing.T) {
	for _, state := range []sealing.SectorState{
		sealing.Removing,
		sealing.Terminating,
		sealing.TerminateWait,
		sealing.TerminateFinality,
		sealing.TerminateFailed,
	} {
		t.Run(string(state), func(t *testing.T) {
			require.Equal(t, sealingStateSettled, sealingStateGateFor(lapi.SectorState(state)),
				"the watch asks the chain for a sector that is only on its way out")

			// The deal's data is on chain: verified weight, no claim, our piece
			// at our offset. The verdict calls that done, so completing here is
			// the chain's answer and not an accident of an empty fixture.
			pieces := pieceListWith(ourPieceCID, ourPieceSize, 0)
			if _, v := verdict(sectorWithWeights(zeroWeight, unitWeight), nil, pieces); v != sealingDone {
				t.Fatalf("fixture is not a chain shape the verdict would call done, so this test proves nothing")
			}

			f := newChainFixture(t)
			f.expectSealerState(state, pieces)
			f.expectSectorOnChain(sectorWithWeights(zeroWeight, unitWeight))
			f.expectClaim(nil)

			step := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())
			require.True(t, step.done)
			require.Nil(t, step.err)
		})
	}
}

// TestSectorPiecesKnownIsIdentityFree pins what a piece list is worth. Boost
// reads the miner API's SectorsStatus here and does not ask which sealer is
// behind it: the same field arrives either way, so the answer may not depend on
// the sealer's identity. A sealed sector's list is never empty -- the padding
// that fills the sector out is listed too -- so a non-empty list is a list,
// while an empty one is not "the sector holds no pieces" but "no list was
// reported", and has to keep meaning "unknown".
//
// The cost of getting this wrong in either direction is severe: too trusting
// turns an unreported list into "the piece is absent" (a false failure on a
// landed deal), too suspicious leaves the aborted snap polling forever.
func TestSectorPiecesKnownIsIdentityFree(t *testing.T) {
	listed := pieceListWith(otherPieceCID, ourPieceSize, 0)

	for _, tt := range []struct {
		name string
		si   lapi.SectorInfo
		want bool
	}{
		{"a reported list", lapi.SectorInfo{Pieces: listed}, true},
		{"no list at all", lapi.SectorInfo{}, false},
		{"an explicitly empty list", lapi.SectorInfo{Pieces: []lapi.SectorPiece{}}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, sectorPiecesKnown(tt.si))
		})
	}

	// The same question, one level up: which sealer answers cannot enter the
	// read, because there is no longer anywhere for it to enter from. The
	// provider holds no identity of the sealer at all -- the config flag and the
	// probe that set it are both gone, along with the Curio-only wait on the
	// success path -- so a settled sector whose list lacks our piece is terminal
	// on the content of the list alone.
	f := newChainFixture(t)
	f.expectSealerState(sealing.Proving, listed)
	f.expectSectorOnChain(sectorWithWeights(zeroWeight, unitWeight))
	f.expectClaim(nil)

	step := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())
	require.True(t, step.done)
	require.NotNil(t, step.err, "a reported list without our piece is terminal")
	require.ErrorIs(t, step.err.error, ErrSectorSealingFailed)
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

// TestSealingWatchRecoversAfterATransientFailure is the half the single-poll
// tests cannot reach: every failure above asserts "not done, no error", and a
// pessimistic bug that parked the watch on that first bad answer would satisfy
// all of them. The watch has to actually come back.
//
// A poll is the unit that has to be re-entrant, so the polls are run back to
// back through checkSealingOnce with the chain answering differently each time:
// the second answer is what proves the first one was never turned into a
// verdict. Each case is a distinct way a healthy node produces a first bad
// answer -- an RPC that fails, a node behind the chain, and a sector the sealer
// is still working on -- and all three end the same way, on the same deal, with
// the same `done`.
func TestSealingWatchRecoversAfterATransientFailure(t *testing.T) {
	our := pieceListWith(ourPieceCID, ourPieceSize, 0)
	landed := sectorWithWeights(zeroWeight, unitWeight)

	tests := []struct {
		name string
		// first is the bad answer this node gives while it catches up.
		first func(f *chainFixture)
		// why explains what the first answer means, so the row cannot be
		// mistaken for a verdict that ought to have fired.
		why string
	}{
		{
			name: "the sealer RPC fails",
			first: func(f *chainFixture) {
				f.sps.EXPECT().SectorsStatus(gomock.Any(), sealingSectorNum, false).
					Return(lapi.SectorInfo{}, errors.New("connection refused"))
				f.expectNoChainReads()
			},
			why: "the sealer is unreachable, which says nothing about the sector",
		},
		{
			name: "the node is behind and cannot see the sector",
			first: func(f *chainFixture) {
				f.expectSealerState(sealing.Proving, our)
				f.expectSectorOnChain(nil)
				f.expectClaim(nil)
			},
			why: "the commit message has not landed on this node yet",
		},
		{
			name: "a retryable sealing state",
			first: func(f *chainFixture) {
				f.expectSealerState(sealing.SnapDealsAddPieceFailed, our)
				f.expectNoChainReads()
			},
			why: "the sealer retries this one itself, so it is not an answer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newChainFixture(t)
			tt.first(f)

			first := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())
			require.False(t, first.done, "a poll that learned nothing must not end the watch: %s", tt.why)
			require.Nil(t, first.err, "and must not fail the deal: %s", tt.why)

			// The same deal, one tick later, with the node caught up.
			f.expectSealerState(sealing.Proving, our)
			f.expectSectorOnChain(landed)
			f.expectClaim(nil)

			second := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())
			require.True(t, second.done)
			require.Nil(t, second.err)
			require.Equal(t, casePostSealedSector, second.caseID)
		})
	}
}

// TestSealingWatchRecoversAfterTheSectorAppears walks the lagging-node case the
// whole way: the sector is invisible, then it is on chain and the deal is done.
// It is separated from the table above because the first poll here *does* read
// the chain -- the sealer already reported a settled state -- so the recovery
// crosses both reads rather than just the sealer's answer.
func TestSealingWatchRecoversAfterTheSectorAppears(t *testing.T) {
	our := pieceListWith(ourPieceCID, ourPieceSize, 0)

	f := newChainFixture(t)
	f.expectSealerState(sealing.Proving, our)
	f.expectSectorOnChain(nil)
	f.expectClaim(nil)

	// Poll 1: the sealer is done and says the piece is in the sector, but this
	// node has not seen the sector at all. Lag is not an answer, and the sealer's
	// piece list cannot stand in for the chain.
	lagging := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())
	require.False(t, lagging.done)
	require.Nil(t, lagging.err)
	require.Equal(t, caseIndeterminate, lagging.caseID)

	// Poll 2: the node has caught up. The same sealer answer now settles it.
	f.expectSealerState(sealing.Proving, our)
	f.expectSectorOnChain(sectorWithWeights(zeroWeight, unitWeight))
	f.expectClaim(nil)

	caughtUp := f.ddp.checkSealingOnce(f.ddp.ctx, f.deal())
	require.True(t, caughtUp.done)
	require.Nil(t, caughtUp.err)
	require.Equal(t, casePostSealedSector, caughtUp.caseID)
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

// TestWatchSealingUpdatesResumesAfterARestart is the other half of the shutdown
// case, and the one a direct deal actually hits: boost is stopped while the
// sector is still sealing, comes back up, and the same deal is watched again
// from the beginning. The watch holds no state across the restart -- there is no
// last-sealing-state to inherit and nothing was written to the deal row by the
// shutdown -- so the second run has to reach the same verdict a fresh watch
// would, and it has to be able to do it from any point in the seal.
//
// Nothing is reset between the runs but the context: the same provider, the same
// deal row, the same sector.
func TestWatchSealingUpdatesResumesAfterARestart(t *testing.T) {
	f := newWatchFixture(t)

	// Before the restart: the sealer is mid-seal, so the watch parks.
	f.expectSealerState(sealing.AddPiece, nil)
	f.expectNoChainReads()

	stopping, cancel := context.WithCancel(context.Background())
	f.ddp.ctx = stopping
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	entry := f.deal()
	f.insertDeal(t, entry)

	derr := f.ddp.watchSealingUpdates(entry)
	require.NotNil(t, derr)
	require.Equal(t, types.DealRetryAuto, derr.retry,
		"a shutdown must leave the deal resumable, not failed")

	// The deal row is untouched by the shutdown, so it is still mid-seal.
	resumed, err := f.ddp.directDealsDB.ByID(context.Background(), entry.ID)
	require.NoError(t, err)
	require.Less(t, resumed.Checkpoint, dealcheckpoints.Complete)

	// Boost comes back up on a fresh context, and the sealing finished while it
	// was down. The watch starts over with no memory of the first run.
	f.ddp.ctx = context.Background()
	f.expectSealerState(sealing.Proving, pieceListWith(ourPieceCID, ourPieceSize, 0))
	f.expectSectorOnChain(sectorWithWeights(zeroWeight, unitWeight))
	f.expectClaim(nil)

	require.Nil(t, f.ddp.watchSealingUpdates(resumed))

	msgs := logMessages(mustLogs(t, f.logsDB, entry.ID))
	require.Contains(t, msgs, "the deal's sector is committed and live on chain")
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

// ---------------------------------------------------------------------------
// The deal row: what boost ends up saying about the deal.
//
// Every test above stops at the watch's return value, and nothing in boost reads
// that value. The operator, the deal list, and the resume-on-restart path all
// read the direct_deals row instead, and the process loop is the only thing that
// turns a *dealMakingError into that row: it writes Complete on success, fails
// the deal on a fatal, and parks it for a retry otherwise. A verdict that never
// reached the row would look exactly like a deal that was never watched, so the
// row is the level the behaviour has to be pinned at.

// insertWatchReadyDeal stores a deal that has already been handed to the sealer
// and announced, which is the state a deal is in when the watch starts. execDeal
// gates commp, the client lookup, AddPiece and indexAndAnnounce on the
// checkpoint, so at IndexedAndAnnounced it goes straight to the watch and then
// writes Complete, leaving the mocks to the same three the tests above use.
//
// The row is inserted carrying a stale failure on purpose: process has to clear
// that, and a row that started clean could not tell "cleared" from "never
// written".
func (f *chainFixture) insertWatchReadyDeal(t *testing.T) *types.DirectDeal {
	t.Helper()
	d := f.deal()
	d.Checkpoint = dealcheckpoints.IndexedAndAnnounced
	d.Retry = types.DealRetryFatal
	d.Err = "failure left over from an earlier run"
	f.insertDeal(t, d)
	return d
}

// dealRow re-reads the deal the way an operator -- or the resume path -- would.
func (f *chainFixture) dealRow(t *testing.T, id uuid.UUID) *types.DirectDeal {
	t.Helper()
	row, err := f.ddp.directDealsDB.ByID(context.Background(), id)
	require.NoError(t, err)
	return row
}

// TestProcessRecordsTheVerdictInTheDealRow runs a whole deal thread to its end
// for every way the watch can finish, and asserts the persisted deal each way:
// it completes, it fails fatally, and the failure says why.
//
// Complete is asserted together with the reason rather than on its own, because
// every terminal outcome writes Complete -- failDeal sets it too, deliberately,
// so that a fatally failed deal is not picked up by ListActive and re-run on the
// next restart. Checkpoint alone therefore cannot tell the two apart, and a test
// that stopped there would pass on a deal that quietly failed.
func TestProcessRecordsTheVerdictInTheDealRow(t *testing.T) {
	our := pieceListWith(ourPieceCID, ourPieceSize, 0)

	tests := []struct {
		name string
		// stage answers the watch's two questions in order: the sealer, then the
		// chain.
		stage func(f *chainFixture)
		// wantErr is a substring of what the operator will be shown on the deal.
		// Empty means the deal is expected to succeed.
		wantErr string
		// wantSentinel is the error the row has to wrap, so the failure stays
		// recognisable after it has been through the database as a string. A nil
		// one means this failure is not a sector-sealing failure: a claim that
		// points elsewhere is decided about the allocation, and saying the
		// sector failed to seal would misdescribe it.
		wantSentinel error
	}{
		{
			// The nv29 shape: verifreg is frozen, so there is no claim to find
			// and the sector's own weight is the evidence.
			name: "an nv29 direct deal lands on the sector's verified weight",
			stage: func(f *chainFixture) {
				f.expectSealerState(sealing.Proving, our)
				f.expectSectorOnChain(sectorWithWeights(zeroWeight, unitWeight))
				f.expectClaim(nil)
			},
		},
		{
			// The pre-nv29 shape on the same code path: the claim is still the
			// authority, and the sector may carry no weight at all.
			name: "an nv28 deal lands on its claim",
			stage: func(f *chainFixture) {
				f.expectSealerState(sealing.Proving, our)
				f.expectSectorOnChain(sectorWithWeights(zeroWeight, zeroWeight))
				f.expectClaim(claimOn(sealingSectorNum))
			},
		},
		{
			// The one verdict the sealer alone can give.
			name: "the sealer reports an unrecoverable failure",
			stage: func(f *chainFixture) {
				f.expectSealerState(sealing.FailedUnrecoverable, nil)
				f.expectNoChainReads()
			},
			wantErr:      "reached sealing state",
			wantSentinel: ErrSectorSealingFailed,
		},
		{
			name: "the sector was removed by the sealer",
			stage: func(f *chainFixture) {
				f.expectSealerState(sealing.Removed, nil)
				f.expectNoChainReads()
			},
			wantErr:      "was removed by the sealer",
			wantSentinel: ErrSectorRemovedByProvider,
		},
		{
			// A snap that ran and fell back: no weight, no claim, and a readable
			// piece list that does not contain this deal's piece.
			name: "the snap fell back to the original CC",
			stage: func(f *chainFixture) {
				f.expectSealerState(sealing.Proving, pieceListWith(otherPieceCID, ourPieceSize, 0))
				f.expectSectorOnChain(sectorWithWeights(zeroWeight, zeroWeight))
				f.expectClaim(nil)
			},
			wantErr:      "the snap fell back",
			wantSentinel: ErrSectorSealingFailed,
		},
		{
			// The sector does carry data, but the piece list says it is not this
			// deal's -- the shape a non-zero weight is not allowed to vouch for.
			name: "the sector carries another piece, not this deal's",
			stage: func(f *chainFixture) {
				f.expectSealerState(sealing.Proving, pieceListWith(otherPieceCID, ourPieceSize, 0))
				f.expectSectorOnChain(sectorWithWeights(zeroWeight, unitWeight))
				f.expectClaim(nil)
			},
			wantErr:      "carries another piece",
			wantSentinel: ErrSectorSealingFailed,
		},
		{
			// Decided without ever seeing this sector: the allocation's claim
			// names a different one, so the data went elsewhere.
			name: "the claim points at another sector",
			stage: func(f *chainFixture) {
				f.expectSealerState(sealing.Proving, nil)
				f.expectSectorOnChain(nil)
				f.expectClaim(claimOn(otherSectorNum))
			},
			wantErr: "claimed by a sector other than",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newWatchFixture(t)
			tt.stage(f)

			entry := f.insertWatchReadyDeal(t)
			f.ddp.process(f.ddp.ctx, entry.ID)

			row := f.dealRow(t, entry.ID)
			require.Equal(t, dealcheckpoints.Complete, row.Checkpoint,
				"a decided deal has to be marked complete, whatever the decision was")

			if tt.wantErr == "" {
				require.Equal(t, types.DealRetryAuto, row.Retry,
					"a landed deal is auto-retryable and carries no error")
				require.Empty(t, row.Err,
					"the stale failure from the earlier run has to be cleared")
				return
			}

			require.Equal(t, types.DealRetryFatal, row.Retry,
				"a failed deal must not stay on the retry list")
			require.Contains(t, row.Err, tt.wantErr,
				"the operator has to be told why the deal failed")
			if tt.wantSentinel != nil {
				require.Contains(t, row.Err, tt.wantSentinel.Error(),
					"and the failure has to stay recognisable after it has been through the database")
			}
		})
	}
}

// TestProcessLeavesAWaitingDealResumable is the third outcome the deal row has
// to express, and the one that is easiest to get wrong: the watch has not
// decided anything yet, boost is shut down, and the deal must survive the
// restart rather than be resolved one way or the other.
//
// The distinguishing assertion is Checkpoint < Complete. Every terminal outcome
// above writes Complete, and DirectDealsProvider.Start resumes via ListActive,
// which excludes exactly those rows -- so "still waiting" is not merely an
// unfinished row, it is the only shape that gets picked back up.
func TestProcessLeavesAWaitingDealResumable(t *testing.T) {
	f := newWatchFixture(t)

	// The sealer is mid-seal on the first poll: nothing has been learned, so the
	// watch parks on the ticker and only the shutdown ends it.
	f.expectSealerState(sealing.AddPiece, nil)
	f.expectNoChainReads()

	stopping, cancel := context.WithCancel(context.Background())
	f.ddp.ctx = stopping
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	entry := f.insertWatchReadyDeal(t)
	f.ddp.process(stopping, entry.ID)

	row := f.dealRow(t, entry.ID)
	require.Equal(t, dealcheckpoints.IndexedAndAnnounced, row.Checkpoint,
		"a shutdown must not move the deal past where it got to")
	require.Less(t, row.Checkpoint, dealcheckpoints.Complete,
		"and must not mark it complete, or the resume would skip it")
	require.Equal(t, types.DealRetryAuto, row.Retry,
		"the deal has to be left for the auto-retry the next start performs")
	require.Contains(t, row.Err, context.Canceled.Error(),
		"the operator should see that boost was shut down, not a failure")
}
