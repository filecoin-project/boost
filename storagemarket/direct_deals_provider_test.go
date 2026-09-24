package storagemarket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/types"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
)

const sealedState = lapi.SectorState(sealing.Proving)

type sealedSectorPipeline struct {
	sealingpipeline.API
	state  lapi.SectorState
	pieces []lapi.SectorPiece
}

func (s *sealedSectorPipeline) SectorsStatus(context.Context, abi.SectorNumber, bool) (lapi.SectorInfo, error) {
	return lapi.SectorInfo{State: s.state, Pieces: s.pieces}, nil
}

func holdPiece(piece cid.Cid) []lapi.SectorPiece {
	return []lapi.SectorPiece{{Piece: abi.PieceInfo{Size: abi.PaddedPieceSize(1 << 20), PieceCID: piece}}}
}

type sealingSequencePipeline struct {
	sealingpipeline.API
	states []lapi.SectorState
	pieces []lapi.SectorPiece
	calls  int
}

func (s *sealingSequencePipeline) SectorsStatus(context.Context, abi.SectorNumber, bool) (lapi.SectorInfo, error) {
	i := s.calls
	if i >= len(s.states) {
		i = len(s.states) - 1
	}
	s.calls++
	return lapi.SectorInfo{State: s.states[i], Pieces: s.pieces}, nil
}

func jumpingClock(base time.Time, atCall int) func() time.Time {
	calls := 0
	return func() time.Time {
		calls++
		if calls >= atCall {
			return base.Add(claimWaitLimit + time.Minute)
		}
		return base
	}
}

// TestWatchSealingUpdatesTimesOutInTheLoop drives the poll loop and its wait: a
// deal whose sector never produces an outcome ends on a timeout naming what was waited for.
func TestWatchSealingUpdatesTimesOutInTheLoop(t *testing.T) {
	base := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)

	for name, tc := range map[string]struct {
		node    *claimLookupNode
		pieces  []lapi.SectorPiece
		wantErr error
		notErr  error
	}{
		"a missing claim": {
			// Pre-nv29: the claim is expected, and only late, so the deal waits.
			node:    &claimLookupNode{activation: nv29Height - 1},
			wantErr: ErrNoClaimFound,
			notErr:  ErrPieceUnverifiable,
		},
		"a piece nobody could check": {
			// Post-nv29 with a sealer that lists no pieces, so the piece question was
			// never answered: unverifiable, not a missing claim, a difference the Curio migration reads.
			node:    &claimLookupNode{activation: nv29Height + 1},
			pieces:  nil,
			wantErr: ErrPieceUnverifiable,
			notErr:  ErrNoClaimFound,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ddp, entry := newWatchSealingHarness(t, tc.node)
			ddp.sps = &sealingSequencePipeline{
				states: []lapi.SectorState{lapi.SectorState(sealing.Packing), lapi.SectorState(sealing.Proving)},
				pieces: tc.pieces,
			}
			ddp.sealingPollEvery = time.Millisecond
			ddp.sealingClock = jumpingClock(base, 2)

			derr := ddp.watchSealingUpdates(entry)

			require.NotNil(t, derr)
			require.Equal(t, types.DealRetryFatal, derr.retry)
			require.ErrorIs(t, derr.error, tc.wantErr)
			require.NotErrorIs(t, derr.error, tc.notErr)
		})
	}
}

// claimLookupNode answers the chain queries watchSealingUpdates makes. Its chain
// is always at nv29, so a check reading the current version gets a wrong answer.
type claimLookupNode struct {
	v1api.FullNode
	nv            network.Version
	paramsErr     error
	activation    abi.ChainEpoch
	snappedAt     abi.ChainEpoch
	snapped       bool
	noPieceData   bool
	sectorErr     error
	sectorMissing bool
	claim         *verifreg9types.Claim
}

func (c *claimLookupNode) StateNetworkVersion(context.Context, ltypes.TipSetKey) (network.Version, error) {
	if c.nv == 0 {
		return network.Version29, nil
	}
	return c.nv, nil
}

func (c *claimLookupNode) StateGetNetworkParams(context.Context) (*lapi.NetworkParams, error) {
	if c.paramsErr != nil {
		return nil, c.paramsErr
	}
	return &lapi.NetworkParams{
		ForkUpgradeParams: lapi.ForkUpgradeParams{UpgradeSolsticeHeight: nv29Height},
	}, nil
}

func (c *claimLookupNode) StateGetClaim(context.Context, address.Address, verifreg9types.ClaimId, ltypes.TipSetKey) (*verifreg9types.Claim, error) {
	return c.claim, nil
}

func (c *claimLookupNode) StateSectorGetInfo(_ context.Context, _ address.Address, sector abi.SectorNumber, _ ltypes.TipSetKey) (*miner.SectorOnChainInfo, error) {
	if c.sectorErr != nil {
		return nil, c.sectorErr
	}
	if c.sectorMissing {
		return nil, nil
	}

	si := &miner.SectorOnChainInfo{
		SectorNumber: sector,
		Activation:   c.activation,
		// Zero, not absent: the chain always writes both weights, and an absent one panics.
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
	}
	if !c.noPieceData {
		// Past nv29 every piece's spacetime lands here whether or not it was verified.
		si.VerifiedDealWeight = big.NewInt(1 << 20)
	}
	if c.snapped {
		// A snapped sector keeps its sealed CID and moves the power base to the update epoch, leaving
		// activation at the original prove.
		key := testSectorKeyCid(c.activation)
		si.SectorKeyCID = &key
		si.PowerBaseEpoch = c.snappedAt
	}
	return si, nil
}

func testSectorKeyCid(seed abi.ChainEpoch) cid.Cid {
	mh, err := multihash.Sum([]byte(fmt.Sprintf("sector key %d", seed)), multihash.SHA2_256, -1)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}

const nv29Height = abi.ChainEpoch(1000)

// newTestStores creates the sqlite database a DirectDealsProvider needs, with
// the boost tables in it.
func newTestStores(t *testing.T) (*db.DirectDealsDB, *logs.DealLogger) {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "*.db")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	sqldb, err := db.SqlDB(f.Name())
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqldb.Close() })
	require.NoError(t, db.CreateAllBoostTables(context.Background(), sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))

	return db.NewDirectDealsDB(sqldb), logs.NewDealLogger(db.NewLogsDB(sqldb))
}

func newWatchSealingHarness(t *testing.T, node *claimLookupNode) (*DirectDealsProvider, *types.DirectDeal) {
	t.Helper()
	return newWatchSealingHarnessInState(t, node, lapi.SectorState(sealing.Proving))
}

func newWatchSealingHarnessInState(t *testing.T, node *claimLookupNode, state lapi.SectorState) (*DirectDealsProvider, *types.DirectDeal) {
	t.Helper()

	ddp, entry := newWatchSealingHarnessWithPieces(t, node, state, nil)
	ddp.sps = &sealedSectorPipeline{state: state, pieces: holdPiece(entry.PieceCID)}
	return ddp, entry
}

func newWatchSealingHarnessWithPieces(t *testing.T, node *claimLookupNode, state lapi.SectorState, pieces []lapi.SectorPiece) (*DirectDealsProvider, *types.DirectDeal) {
	t.Helper()

	_, dealLogger := newTestStores(t)

	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)

	ddp := &DirectDealsProvider{
		ctx:         context.Background(),
		Address:     maddr,
		fullnodeApi: node,
		sps:         &sealedSectorPipeline{state: state, pieces: pieces},
		dealLogger:  dealLogger,
	}

	entry := &types.DirectDeal{
		ID:           uuid.New(),
		AllocationID: verifreg9types.AllocationId(1),
		SectorID:     abi.SectorNumber(2),
		PieceCID:     testPieceCid(t),
	}
	return ddp, entry
}

// TestWatchSealingUpdatesSealedAfterNv29 covers a sector activated after the
// upgrade, where the sealed sector is the whole of the outcome and reporting the
// missing claim as a failure marks a deal that is fine as broken.
func TestWatchSealingUpdatesSealedAfterNv29(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{activation: nv29Height + 1})

	require.Nil(t, ddp.watchSealingUpdates(entry),
		"a deal whose sector sealed after nv29 is complete even though no claim was written")
}

// TestWatchSealingUpdatesClaimFound pins the pre-nv29 path: a claim that matches
// the sector still completes the deal, so the nv29 branch has not displaced the
// normal case.
func TestWatchSealingUpdatesClaimFound(t *testing.T) {
	claim := &verifreg9types.Claim{Sector: abi.SectorNumber(2)}
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{activation: nv29Height - 1, claim: claim})

	require.Nil(t, ddp.watchSealingUpdates(entry))
}

// TestWatchSealingUpdatesClaimSectorMismatch checks that a claim on another
// sector stays fatal whatever the sector's activation, so the new branch does not
// swallow a real inconsistency along with the expected absences.
func TestWatchSealingUpdatesClaimSectorMismatch(t *testing.T) {
	for name, node := range map[string]*claimLookupNode{
		"sealed before nv29": {activation: nv29Height - 1, claim: &verifreg9types.Claim{Sector: abi.SectorNumber(3)}},
		"sealed after nv29":  {activation: nv29Height + 1, claim: &verifreg9types.Claim{Sector: abi.SectorNumber(3)}},
	} {
		t.Run(name, func(t *testing.T) {
			ddp, entry := newWatchSealingHarness(t, node)

			derr := ddp.watchSealingUpdates(entry)
			require.NotNil(t, derr)
			require.Equal(t, types.DealRetryFatal, derr.retry)
			require.Contains(t, derr.Error(), "sector mismatch")
		})
	}
}

// TestWatchSealingUpdatesClaimFoundAtNv29 covers a deal that got its claim
// before the upgrade and is re-checked after: it is still on chain, so use it.
func TestWatchSealingUpdatesClaimFoundAtNv29(t *testing.T) {
	claim := &verifreg9types.Claim{Sector: abi.SectorNumber(2)}
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{activation: nv29Height + 1, claim: claim})

	require.Nil(t, ddp.watchSealingUpdates(entry))
}

func TestSettleNoClaimSealedBeforeNv29KeepsWaiting(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{activation: nv29Height - 1})

	outcome, err := ddp.settle(entry, sealedState)
	require.Nil(t, err)
	require.Equal(t, claimPending, outcome, "a pre-nv29 sector's missing claim is a fault, not the expected nv29 absence")
}

func TestSettleSectorNotOnChainKeepsWaiting(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{sectorMissing: true})

	outcome, err := ddp.settle(entry, sealedState)
	require.Nil(t, err)
	require.Equal(t, claimPending, outcome)
}

// TestSettleLookupFailureRetries: a failed chain query is neither answer, so the deal retries.
func TestSettleLookupFailureRetries(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{sectorErr: context.DeadlineExceeded})

	outcome, err := ddp.settle(entry, sealedState)
	require.NotNil(t, err)
	require.Equal(t, claimPending, outcome)
	require.Equal(t, types.DealRetryAuto, err.retry)
}

// TestSettleSnapAfterNv29: the update's epoch dates a snapped piece, not the activation, which
// would wait for a claim FIP-0118 never writes.
func TestSettleSnapAfterNv29(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{
		activation: nv29Height - 5000,
		snapped:    true,
		snappedAt:  nv29Height + 1,
	})

	outcome, err := ddp.settle(entry, sealedState)
	require.Nil(t, err)
	require.Equal(t, claimDone, outcome, "a piece snapped into a sector after nv29 is complete without a claim")
}

func TestSettleSnapBeforeNv29KeepsWaiting(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{
		activation: nv29Height - 5000,
		snapped:    true,
		snappedAt:  nv29Height - 1,
	})

	outcome, err := ddp.settle(entry, sealedState)
	require.Nil(t, err)
	require.Equal(t, claimPending, outcome)
}

// TestSettleFailedSnapAfterNv29KeepsWaiting: with no snap landed, the absent piece spacetime is
// what says the data never reached the sector.
func TestSettleFailedSnapAfterNv29KeepsWaiting(t *testing.T) {
	ddp, entry := newWatchSealingHarness(t, &claimLookupNode{
		activation:  nv29Height + 1,
		noPieceData: true,
	})

	outcome, err := ddp.settle(entry, sealedState)
	require.Nil(t, err)
	require.Equal(t, claimPending, outcome, "an empty sector must not settle a deal whose data never reached it")
}

// TestSettleSnapWithoutThePieceIsFatal: the sector dates as complete, so only the
// sealer's piece list can say this deal's data is not in it, and no wait will put it there.
func TestSettleSnapWithoutThePieceIsFatal(t *testing.T) {
	ddp, entry := newWatchSealingHarnessWithPieces(t,
		&claimLookupNode{activation: nv29Height - 5000, snapped: true, snappedAt: nv29Height + 1},
		lapi.SectorState(sealing.Proving),
		holdPiece(testPieceCidSeed(t, "another deal's piece")))

	outcome, err := ddp.settle(entry, sealedState)
	require.NotNil(t, err)
	require.Equal(t, claimPending, outcome)
	require.Equal(t, types.DealRetryFatal, err.retry)
	require.ErrorIs(t, err.error, ErrPieceNotOnboarded)
	require.NotErrorIs(t, err.error, ErrNoClaimFound)
}

func TestSettlePieceListUnavailableKeepsWaiting(t *testing.T) {
	ddp, entry := newWatchSealingHarnessWithPieces(t,
		&claimLookupNode{activation: nv29Height + 1},
		lapi.SectorState(sealing.Proving),
		nil)

	outcome, err := ddp.settle(entry, sealedState)
	require.Nil(t, err)
	require.Equal(t, claimUnverifiable, outcome, "waiting on a piece list that never came is not the same wait as a claim that never came")
}

// TestTimeoutErrorNamesWhatWasWaitedFor: Curio carries a missing claim over on the text alone, so
// an unverifiable piece must not read as that claim.
func TestTimeoutErrorNamesWhatWasWaitedFor(t *testing.T) {
	missingClaim := timeoutError(claimPending)
	require.ErrorIs(t, missingClaim.error, ErrNoClaimFound)
	require.Equal(t, types.DealRetryFatal, missingClaim.retry)

	unverified := timeoutError(claimUnverifiable)
	require.ErrorIs(t, unverified.error, ErrPieceUnverifiable)
	require.Equal(t, types.DealRetryFatal, unverified.retry)
	require.NotErrorIs(t, unverified.error, ErrNoClaimFound)
	require.NotErrorIs(t, unverified.error, ErrPieceNotOnboarded)

	require.Equal(t, "piece unverifiable", ErrPieceUnverifiable.Error())
}

func TestErrPieceNotOnboardedWording(t *testing.T) {
	require.Equal(t, "piece not onboarded", ErrPieceNotOnboarded.Error())
	require.NotEqual(t, ErrNoClaimFound.Error(), ErrPieceNotOnboarded.Error())
}

// TestWatchSealingUpdatesSealingFailed: past nv29 no claim is written either way, so
// only the sealing state tells a failed sector from one that sealed.
func TestWatchSealingUpdatesSealingFailed(t *testing.T) {
	for _, state := range []sealing.SectorState{
		sealing.FailedUnrecoverable,
		sealing.Removed,
		sealing.Terminating,
	} {
		t.Run(string(state), func(t *testing.T) {
			node := &claimLookupNode{activation: nv29Height + 1, noPieceData: true}
			ddp, entry := newWatchSealingHarnessInState(t, node, lapi.SectorState(state))

			derr := ddp.watchSealingUpdates(entry)
			require.NotNil(t, derr)
			require.Equal(t, types.DealRetryFatal, derr.retry)
			require.ErrorIs(t, derr.error, ErrSectorSealingFailed)
			require.NotErrorIs(t, derr.error, ErrNoClaimFound)
		})
	}
}

// TestWatchSealingUpdatesClaimOutranksSealingState pins the order the two questions
// are asked in: a claim on chain settles the deal before the sealing state gets a say.
func TestWatchSealingUpdatesClaimOutranksSealingState(t *testing.T) {
	for _, state := range []sealing.SectorState{
		sealing.Removed,
		sealing.Terminating,
		sealing.FailedUnrecoverable,
	} {
		t.Run(string(state), func(t *testing.T) {
			claim := &verifreg9types.Claim{Sector: abi.SectorNumber(2)}
			node := &claimLookupNode{activation: nv29Height - 1, claim: claim}
			ddp, entry := newWatchSealingHarnessInState(t, node, lapi.SectorState(state))

			require.Nil(t, ddp.watchSealingUpdates(entry),
				"a deal whose claim is on chain is complete even if the sector was removed afterwards")
		})
	}
}

// TestErrNoClaimFoundWording pins the sentence migrate-curio matches on: a deal
// carries its error as text alone, so changing the wording would strand data.
func TestErrNoClaimFoundWording(t *testing.T) {
	require.Equal(t, "no claim found", ErrNoClaimFound.Error())
	require.True(t, errors.Is(ErrNoClaimFound, ErrNoClaimFound))
}

// TestNoClaimFoundIsRecordedVerbatim: dealMakingError keeps no error chain, so the
// sentinel must reach the deal record undecorated for migrate-curio to match it.
func TestNoClaimFoundIsRecordedVerbatim(t *testing.T) {
	derr := &dealMakingError{retry: types.DealRetryFatal, error: ErrNoClaimFound}

	require.Equal(t, ErrNoClaimFound.Error(), derr.Error())
}

// TestClaimWaitStartsWhenThereIsSomethingToWaitFor pins the clock the wait is
// measured against: a deal is watched from announcement, before its sector seals.
func TestClaimWaitStartsWhenThereIsSomethingToWaitFor(t *testing.T) {
	now := time.Now()

	var wait claimWait
	require.False(t, wait.expired(now.Add(24*time.Hour)),
		"a wait that never began has nothing to run out on")

	wait.start(now)
	require.False(t, wait.expired(now.Add(9*time.Minute)))
	require.True(t, wait.expired(now.Add(11*time.Minute)))

	wait.start(now.Add(time.Hour))
	require.True(t, wait.expired(now.Add(time.Hour)),
		"the first start is the one that counts")
}

// vanishingAllocationNode serves the node queries Import makes, with an
// allocation that is there for Accept's lookup and gone for the one Import runs
// straight after it - the window in which another deal can claim it. Only the
// methods on this path are implemented, so the embedded interface panics if the
// test ever starts depending on more.
type vanishingAllocationNode struct {
	v1api.FullNode
	nv          network.Version
	miner       address.Address
	allocation  *verifreg9types.Allocation
	lookupCalls int
}

func (n *vanishingAllocationNode) ChainHead(context.Context) (*ltypes.TipSet, error) {
	return mockTipset(n.miner, 100)
}

func (n *vanishingAllocationNode) StateNetworkVersion(context.Context, ltypes.TipSetKey) (network.Version, error) {
	return n.nv, nil
}

func (n *vanishingAllocationNode) StateGetAllocation(context.Context, address.Address, verifreg9types.AllocationId, ltypes.TipSetKey) (*verifreg9types.Allocation, error) {
	n.lookupCalls++
	if n.lookupCalls > 1 {
		return nil, nil
	}
	return n.allocation, nil
}

// TestImportRejectsAllocationThatDisappears covers the allocation going away
// between Accept, which checks it is there, and the lookup Import does after it.
// StateGetAllocation reports a missing allocation as (nil, nil), so Import used
// to dereference it and panic on a perm:admin API call.
func TestImportRejectsAllocationThatDisappears(t *testing.T) {
	ctx := context.Background()

	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)
	mid, err := address.IDFromAddress(maddr)
	require.NoError(t, err)

	clientAddr, err := address.NewIDAddress(1001)
	require.NoError(t, err)

	node := &vanishingAllocationNode{
		nv:         network.Version28,
		miner:      maddr,
		allocation: &verifreg9types.Allocation{Provider: abi.ActorID(mid), TermMin: abi.ChainEpoch(2880)},
	}

	directDealsDB, dealLogger := newTestStores(t)
	ddp := &DirectDealsProvider{
		ctx:           ctx,
		Address:       maddr,
		fullnodeApi:   node,
		directDealsDB: directDealsDB,
		dealLogger:    dealLogger,
	}

	res, err := ddp.Import(ctx, types.DirectDealParams{
		DealUUID:     uuid.New(),
		AllocationID: verifreg9types.AllocationId(1),
		PieceCid:     testPieceCid(t),
		ClientAddr:   clientAddr,
		StartEpoch:   200,
		EndEpoch:     300,
	})

	require.NoError(t, err)
	require.NotNil(t, res)
	require.False(t, res.Accepted, "the deal must be turned away, not panic")
	require.Contains(t, res.Reason, "not found")
	require.Equal(t, 2, node.lookupCalls, "Accept should have found the allocation on the first lookup")

	// Nothing should have been queued for a deal that was rejected.
	deals, err := directDealsDB.ListActive(ctx)
	require.NoError(t, err)
	require.Empty(t, deals)
}

// TestAcceptRejectsDirectDealAtNv29 pins the provider's side of the nv29
// rejection to the shared reason. `boostd import-direct` refuses the same deal
// up front with that string, so this is what keeps the two answers identical
// instead of two sentences that drift apart.
//
// The deal is otherwise well formed and has an allocation, so the network
// version is the only thing that can turn it away.
func TestAcceptRejectsDirectDealAtNv29(t *testing.T) {
	ctx := context.Background()

	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)
	mid, err := address.IDFromAddress(maddr)
	require.NoError(t, err)

	clientAddr, err := address.NewIDAddress(1001)
	require.NoError(t, err)

	node := &vanishingAllocationNode{
		nv:         network.Version29,
		miner:      maddr,
		allocation: &verifreg9types.Allocation{Provider: abi.ActorID(mid), TermMin: abi.ChainEpoch(2880)},
	}

	directDealsDB, dealLogger := newTestStores(t)
	ddp := &DirectDealsProvider{
		ctx:           ctx,
		Address:       maddr,
		fullnodeApi:   node,
		directDealsDB: directDealsDB,
		dealLogger:    dealLogger,
	}

	res, err := ddp.Import(ctx, types.DirectDealParams{
		DealUUID:     uuid.New(),
		AllocationID: verifreg9types.AllocationId(1),
		PieceCid:     testPieceCid(t),
		ClientAddr:   clientAddr,
		StartEpoch:   200,
		EndEpoch:     300,
	})

	require.NoError(t, err)
	require.NotNil(t, res)
	require.False(t, res.Accepted)
	require.Equal(t, DirectDealRejectionAtNv29, res.Reason)

	// The version is checked before the allocation is looked up, so a deal that
	// cannot be served at all does not even reach the chain queries.
	require.Zero(t, node.lookupCalls, "the network version should be settled before anything else is asked")
}

func testPieceCid(t *testing.T) cid.Cid {
	t.Helper()
	return testPieceCidSeed(t, "test piece")
}

func testPieceCidSeed(t *testing.T, seed string) cid.Cid {
	t.Helper()

	mh, err := multihash.Sum([]byte(seed), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}

// TestPieceOnboardedAtOrAfterNv29BeforeUpgrade: pre-nv29 the version alone settles
// it, and the sector lookup is rigged to fail if the check goes past that.
func TestPieceOnboardedAtOrAfterNv29BeforeUpgrade(t *testing.T) {
	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)

	node := &claimLookupNode{nv: network.Version28, sectorErr: context.DeadlineExceeded}

	onboarded, err := PieceOnboardedAtOrAfterNv29(context.Background(), node, maddr, abi.SectorNumber(2))
	require.NoError(t, err)
	require.False(t, onboarded)
}

type paramsNode struct {
	v1api.FullNode
	params *lapi.NetworkParams
	err    error
}

func (p *paramsNode) StateGetNetworkParams(context.Context) (*lapi.NetworkParams, error) {
	return p.params, p.err
}

func paramsAt(height abi.ChainEpoch) *paramsNode {
	return &paramsNode{params: &lapi.NetworkParams{
		ForkUpgradeParams: lapi.ForkUpgradeParams{UpgradeSolsticeHeight: height},
	}}
}

// TestNv29UpgradeHeight: the epoch comes from the node, so these cases hold under every build tag.
func TestNv29UpgradeHeight(t *testing.T) {
	tests := map[string]struct {
		node      v1api.FullNode
		want      abi.ChainEpoch
		expectErr bool
	}{
		"calibnet, whose epoch is already set": {
			node: paramsAt(4109133),
			want: 4109133,
		},
		"mainnet, whose upgrade is not scheduled yet": {
			node: paramsAt(999999999999999),
			want: 999999999999999,
		},
		"a devnet with a scheduled upgrade": {
			node: paramsAt(200),
			want: 200,
		},
		"a devnet with nv29 active from genesis": {
			// Lotus spells a genesis-active nv29 as a negative epoch, which needs no normalising.
			node: paramsAt(-24),
			want: -24,
		},
		"a node too old to carry the field": {
			// Decodes to zero, which would put every sector past the upgrade and settle the deal.
			node:      &paramsNode{params: &lapi.NetworkParams{}},
			expectErr: true,
		},
		"the lookup fails": {
			node:      &paramsNode{err: context.DeadlineExceeded},
			expectErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			height, err := nv29UpgradeHeight(context.Background(), tc.node)
			if tc.expectErr {
				require.Error(t, err)
				require.Zero(t, height, "a rejected height must not leak out as a usable epoch")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, height)
		})
	}
}
