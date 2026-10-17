package itests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	verifregst "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/boost/cmd/boost/util"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/storagemarket"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/testutil"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet/key"
	"github.com/filecoin-project/lotus/itests/kit"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	"github.com/filecoin-project/lotus/storage/pipeline/sealiface"
)

func TestDirectDeal(t *testing.T) {
	ctx := context.Background()
	fileSize := 7048576

	kit.QuietMiningLogs()
	framework.SetLogLevel()

	// Setup datacap wallet and initialise a new ensemble with datacap keys
	rootKey, err := key.GenerateKey(types.KTSecp256k1)
	require.NoError(t, err)

	verifier1Key, err := key.GenerateKey(types.KTSecp256k1)
	require.NoError(t, err)

	bal, err := types.ParseFIL("10000fil")
	require.NoError(t, err)

	var eopts []kit.EnsembleOpt
	// The ensemble otherwise starts at buildconstants.TestNetworkVersion, which is
	// nv29: FIP-0118 deprecates datacap there, so verifreg turns away the AddVerifier
	// this test's setup depends on and a direct deal is rejected outright. The whole
	// run stays at nv28, the last version where an allocation still earns a claim.
	eopts = append(eopts, kit.GenesisNetworkVersion(network.Version28))
	eopts = append(eopts, kit.RootVerifier(rootKey, abi.NewTokenAmount(bal.Int64())))
	eopts = append(eopts, kit.Account(verifier1Key, abi.NewTokenAmount(bal.Int64())))
	eopts = append(eopts, kit.RealProofs())
	esemble := kit.NewEnsemble(t, eopts...)

	var opts []framework.FrameworkOpts
	opts = append(opts, framework.WithEnsemble(esemble))
	opts = append(opts, framework.SetProvisionalWalletBalances(int64(9e18)))
	opts = append(opts, framework.WithStartEpochSealingBuffer(30))
	f := framework.NewTestFramework(ctx, t, opts...)
	esemble.Start()
	blockTime := 100 * time.Millisecond
	esemble.BeginMining(blockTime)

	err = f.Start()
	require.NoError(t, err)
	defer f.Stop()

	// Send funds to PSD wallet as it is being used for POST
	info, err := f.FullNode.StateMinerInfo(ctx, f.MinerAddr, types.EmptyTSK)
	require.NoError(t, err)
	addresses := []address.Address{info.Owner, info.Worker}
	addresses = append(addresses, info.ControlAddresses...)
	for i := 0; i < 3; i++ {
		for _, addr := range addresses {
			err = framework.SendFunds(ctx, f.FullNode, addr, abi.NewTokenAmount(int64(9e18)))
			require.NoError(t, err)
			t.Logf("control address: %s", addr)
		}
	}

	// Give the boost client's address enough datacap to make the deal
	err = f.AddClientDataCap(t, ctx, rootKey, verifier1Key)
	require.NoError(t, err)

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, fileSize)
	require.NoError(t, err)
	_, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)
	commp, err := storagemarket.GenerateCommPLocally(carFilepath)
	require.NoError(t, err)
	t.Logf("Piece CID: %s, Piece Size: %d", commp.PieceCID.String(), commp.Size)

	mid, err := address.IDFromAddress(f.MinerAddr)
	require.NoError(t, err)

	var pieceInfos []util.PieceInfos

	pieceInfos = append(pieceInfos, util.PieceInfos{
		Cid:       commp.PieceCID,
		Size:      int64(commp.Size),
		Miner:     abi.ActorID(mid),
		MinerAddr: f.MinerAddr,
		Tmin:      verifregst.MinimumVerifiedAllocationTerm,
		Tmax:      verifregst.MaximumVerifiedAllocationTerm,
		Exp:       verifregst.MaximumVerifiedAllocationExpiration,
	})

	allocateMsg, err := util.CreateAllocationMsg(ctx, f.FullNode, pieceInfos, f.ClientAddr, 10)
	require.NoError(t, err)

	sm, err := f.FullNode.MpoolPushMessage(ctx, allocateMsg[0], nil)
	require.NoError(t, err)

	_, err = f.FullNode.StateWaitMsg(ctx, sm.Cid(), 1, 1e10, true)
	require.NoError(t, err)

	allocations, err := f.FullNode.StateGetAllocations(ctx, f.ClientAddr, types.EmptyTSK)
	require.NoError(t, err)
	require.Len(t, allocations, 1)

	var allocationId uint64
	for id := range allocations {
		allocationId = uint64(id)
	}

	alloc := allocations[verifreg.AllocationId(allocationId)]

	head, err := f.FullNode.ChainHead(ctx)
	require.NoError(t, err)

	startEpoch := head.Height() + 200
	endEpoch := head.Height() + +2880*400

	dealUuid := uuid.New()
	ddParams := smtypes.DirectDealParams{
		DealUUID:           dealUuid,
		AllocationID:       verifreg.AllocationId(allocationId),
		PieceCid:           commp.PieceCID,
		ClientAddr:         f.ClientAddr,
		StartEpoch:         startEpoch,
		EndEpoch:           endEpoch,
		FilePath:           carFilepath,
		DeleteAfterImport:  false,
		RemoveUnsealedCopy: false,
		SkipIPNIAnnounce:   false,
	}

	rej, err := f.Boost.BoostDirectDeal(ctx, ddParams)
	require.NoError(t, err)
	if rej != nil && rej.Reason != "" {
		require.Fail(t, "direct data import rejected: %s", rej.Reason)
	}
	t.Log("Direct data import scheduled for execution")

	// Wait for sector to start sealing
	time.Sleep(2 * time.Second)

	// Wait till sector 2 is Proving
	states := []lapi.SectorState{lapi.SectorState(sealing.Proving)}
	require.Eventuallyf(t, func() bool {
		stateList, err := f.LotusMiner.SectorsListInStates(ctx, states)
		require.NoError(t, err)
		return len(stateList) == 3
	}, 5*time.Minute, 2*time.Second, "sector 2 is still not proving after 5 minutes")

	// Confirm we have 0 allocations left
	allocations, err = f.FullNode.StateGetAllocations(ctx, f.ClientAddr, types.EmptyTSK)
	require.NoError(t, err)
	require.Len(t, allocations, 0)

	// Match claim with different vars
	claims, err := f.FullNode.StateGetClaims(ctx, f.MinerAddr, types.EmptyTSK)
	require.NoError(t, err)

	require.Len(t, claims, 3)
	claim, ok := claims[verifreg.ClaimId(allocationId)]
	require.True(t, ok)

	st, err := f.FullNode.StateSectorGetInfo(ctx, f.MinerAddr, abi.SectorNumber(2), types.EmptyTSK)
	require.NoError(t, err)

	require.Equal(t, alloc.Data, claim.Data)
	require.Equal(t, alloc.Size, claim.Size)
	require.Equal(t, claim.TermStart, st.Activation)
	require.Equal(t, claim.TermMin, alloc.TermMin)
}

// TestDirectDealAcrossNv29 covers a direct deal that is accepted under nv28 but
// whose sector only activates after the chain upgrades to nv29. FIP-0118
// (Solstice) deprecates datacap at nv29, so the sector's prove-commit writes no
// verifreg claim; the sealing watch has to decide the deal from the sector
// itself rather than from a claim that will never appear. TestDirectDeal keeps
// the whole run on nv28 and proves the claim-based path -- it cannot exercise
// the crossing -- so this test schedules the fork and lets the deal straddle it.
func TestDirectDealAcrossNv29(t *testing.T) {
	ctx := context.Background()
	fileSize := 7048576

	kit.QuietMiningLogs()
	framework.SetLogLevel()

	rootKey, err := key.GenerateKey(types.KTSecp256k1)
	require.NoError(t, err)

	verifier1Key, err := key.GenerateKey(types.KTSecp256k1)
	require.NoError(t, err)

	bal, err := types.ParseFIL("10000fil")
	require.NoError(t, err)

	// The commit batch wait holds prove-commit until the test explicitly flushes
	// it after the nv29 fork.
	const upgradeEpoch = abi.ChainEpoch(2500)

	var eopts []kit.EnsembleOpt
	// nv28 at genesis, nv29 at upgradeEpoch, with the same v19 actors migration
	// the solstice itests run. The datacap setup this test depends on is only
	// available before the fork, which is exactly the scenario under test.
	eopts = append(eopts, kit.LatestActorsAt(upgradeEpoch))
	eopts = append(eopts, kit.RootVerifier(rootKey, abi.NewTokenAmount(bal.Int64())))
	eopts = append(eopts, kit.Account(verifier1Key, abi.NewTokenAmount(bal.Int64())))
	eopts = append(eopts, kit.RealProofs())
	esemble := kit.NewEnsemble(t, eopts...)

	var opts []framework.FrameworkOpts
	opts = append(opts, framework.WithEnsemble(esemble))
	opts = append(opts, framework.SetProvisionalWalletBalances(int64(9e18)))
	opts = append(opts, framework.WithStartEpochSealingBuffer(30))
	opts = append(opts, func(cfg *framework.TestFrameworkConfig) {
		cfg.MutateSealing = func(sc *sealiface.Config) {
			sc.PreCommitBatchWait = time.Hour
			sc.CommitBatchWait = time.Hour
		}
	})
	f := framework.NewTestFramework(ctx, t, opts...)
	esemble.Start()
	blockTime := 100 * time.Millisecond
	esemble.BeginMining(blockTime)

	err = f.Start()
	require.NoError(t, err)
	defer f.Stop()

	// Send funds to PSD wallet as it is being used for POST
	info, err := f.FullNode.StateMinerInfo(ctx, f.MinerAddr, types.EmptyTSK)
	require.NoError(t, err)
	addresses := []address.Address{info.Owner, info.Worker}
	addresses = append(addresses, info.ControlAddresses...)
	for i := 0; i < 3; i++ {
		for _, addr := range addresses {
			err = framework.SendFunds(ctx, f.FullNode, addr, abi.NewTokenAmount(int64(9e18)))
			require.NoError(t, err)
		}
	}

	// Give the boost client's address enough datacap to make the deal. Datacap is
	// deprecated at nv29, so this -- like the allocation below -- has to happen
	// while the chain is still on nv28.
	err = f.AddClientDataCap(t, ctx, rootKey, verifier1Key)
	require.NoError(t, err)

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, fileSize)
	require.NoError(t, err)
	_, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)
	commp, err := storagemarket.GenerateCommPLocally(carFilepath)
	require.NoError(t, err)
	t.Logf("Piece CID: %s, Piece Size: %d", commp.PieceCID.String(), commp.Size)

	mid, err := address.IDFromAddress(f.MinerAddr)
	require.NoError(t, err)

	pieceInfos := []util.PieceInfos{{
		Cid:       commp.PieceCID,
		Size:      int64(commp.Size),
		Miner:     abi.ActorID(mid),
		MinerAddr: f.MinerAddr,
		Tmin:      verifregst.MinimumVerifiedAllocationTerm,
		Tmax:      verifregst.MaximumVerifiedAllocationTerm,
		Exp:       verifregst.MaximumVerifiedAllocationExpiration,
	}}

	allocateMsg, err := util.CreateAllocationMsg(ctx, f.FullNode, pieceInfos, f.ClientAddr, 10)
	require.NoError(t, err)

	sm, err := f.FullNode.MpoolPushMessage(ctx, allocateMsg[0], nil)
	require.NoError(t, err)

	_, err = f.FullNode.StateWaitMsg(ctx, sm.Cid(), 1, 1e10, true)
	require.NoError(t, err)

	allocations, err := f.FullNode.StateGetAllocations(ctx, f.ClientAddr, types.EmptyTSK)
	require.NoError(t, err)
	require.Len(t, allocations, 1)

	var allocationId uint64
	for id := range allocations {
		allocationId = uint64(id)
	}

	// Start sealing while nv28 is active. The test holds precommit and
	// prove-commit independently, so proof duration does not determine which
	// side of the fork either message lands on.
	head, err := f.FullNode.ChainHead(ctx)
	require.NoError(t, err)
	require.Less(t, head.Height(), upgradeEpoch,
		"the deal must be accepted before the nv29 fork")
	nv, err := f.FullNode.StateNetworkVersion(ctx, types.EmptyTSK)
	require.NoError(t, err)
	require.Equal(t, network.Version28, nv,
		"the allocation and direct deal must be created while nv28 is active")

	// Keep both batcher cutoffs beyond the fork as well: Lotus flushes at the
	// earlier of its protocol deadline and the deal start.
	startEpoch := upgradeEpoch + 2880
	endEpoch := head.Height() + 2880*400

	dealUuid := uuid.New()
	ddParams := smtypes.DirectDealParams{
		DealUUID:           dealUuid,
		AllocationID:       verifreg.AllocationId(allocationId),
		PieceCid:           commp.PieceCID,
		ClientAddr:         f.ClientAddr,
		StartEpoch:         startEpoch,
		EndEpoch:           endEpoch,
		FilePath:           carFilepath,
		DeleteAfterImport:  false,
		RemoveUnsealedCopy: false,
		SkipIPNIAnnounce:   false,
	}

	rej, err := f.Boost.BoostDirectDeal(ctx, ddParams)
	require.NoError(t, err)
	// A rejection here means Accept read the head at nv29 and turned the deal
	// away -- the submission crossed the fork. Fail now rather than carry on to
	// the wait below, which would only time out ten minutes later.
	if rej != nil && rej.Reason != "" {
		require.FailNow(t, "direct data import rejected: %s", rej.Reason)
	}
	t.Log("Direct data import scheduled for execution")

	var sectorNum abi.SectorNumber
	require.Eventuallyf(t, func() bool {
		dl, err := f.DirectDealsDB.ByID(ctx, dealUuid)
		if err != nil || dl.Checkpoint < dealcheckpoints.AddedPiece || dl.SectorID == 0 {
			return false
		}
		sectorNum = dl.SectorID

		status, err := f.LotusMiner.SectorsStatus(ctx, sectorNum, false)
		if err != nil {
			t.Logf("reading sector %d: %s", sectorNum, err)
			return false
		}
		if status.State != lapi.SectorState(sealing.SubmitPreCommitBatch) {
			return false
		}
		pending, err := f.LotusMiner.SectorPreCommitPending(ctx)
		if err != nil {
			t.Logf("reading pending precommits: %s", err)
			return false
		}
		for _, sid := range pending {
			if sid.Number == sectorNum {
				return true
			}
		}
		return false
	}, 5*time.Minute, time.Second, "sector never entered the pending precommit batch")

	// Release precommit just before the fork. The seed wait and commit proof can
	// then proceed normally, while the commit batch wait holds prove-commit.
	f.FullNode.WaitTillChain(ctx, kit.HeightAtLeast(upgradeEpoch-20))
	head, err = f.FullNode.ChainHead(ctx)
	require.NoError(t, err)
	require.Less(t, head.Height(), upgradeEpoch)
	precommitResults, err := f.LotusMiner.SectorPreCommitFlush(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, precommitResults, "the held precommit must be flushed before nv29")

	var precommitEpoch abi.ChainEpoch
	require.Eventuallyf(t, func() bool {
		precommit, err := f.FullNode.StateSectorPreCommitInfo(ctx, f.MinerAddr, sectorNum, types.EmptyTSK)
		if err != nil || precommit == nil {
			return false
		}
		precommitEpoch = precommit.PreCommitEpoch
		return true
	}, 2*time.Minute, time.Second, "precommit did not land before nv29")
	require.Less(t, precommitEpoch, upgradeEpoch,
		"the sector must precommit before nv29 for this to cover the requested crossing")

	f.FullNode.WaitTillChain(ctx, kit.HeightAtLeast(upgradeEpoch+1))
	nv, err = f.FullNode.StateNetworkVersion(ctx, types.EmptyTSK)
	require.NoError(t, err)
	require.Equal(t, network.Version29, nv)

	require.Eventuallyf(t, func() bool {
		status, err := f.LotusMiner.SectorsStatus(ctx, sectorNum, false)
		if err != nil {
			t.Logf("reading sector %d: %s", sectorNum, err)
			return false
		}
		if status.State != lapi.SectorState(sealing.SubmitCommitAggregate) {
			return false
		}
		pending, err := f.LotusMiner.SectorCommitPending(ctx)
		if err != nil {
			t.Logf("reading pending commits: %s", err)
			return false
		}
		for _, sid := range pending {
			if sid.Number == sectorNum {
				return true
			}
		}
		return false
	}, 5*time.Minute, time.Second, "sector never entered the pending prove-commit batch after nv29")

	heldDeal, err := f.DirectDealsDB.ByID(ctx, dealUuid)
	require.NoError(t, err)
	require.NotEqual(t, dealcheckpoints.Complete, heldDeal.Checkpoint,
		"the deal must remain incomplete while prove-commit is held")
	heldSector, err := f.FullNode.StateSectorGetInfo(ctx, f.MinerAddr, sectorNum, types.EmptyTSK)
	require.NoError(t, err)
	require.Nil(t, heldSector, "the held prove-commit must not have activated the sector")

	// Release the prove-commit explicitly on the far side of nv29.
	commitResults, err := f.LotusMiner.SectorCommitFlush(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, commitResults, "the held prove-commit must be flushed")
	var flushedSector bool
	for _, result := range commitResults {
		for _, sn := range result.Sectors {
			if sn == sectorNum {
				flushedSector = true
			}
		}
	}
	require.True(t, flushedSector, "the held sector's prove-commit must be flushed")

	// The deal reaches Complete once the sector is proven on chain.
	require.Eventuallyf(t, func() bool {
		dl, err := f.DirectDealsDB.ByID(ctx, dealUuid)
		return err == nil && dl.Checkpoint == dealcheckpoints.Complete
	}, 10*time.Minute, 2*time.Second, "direct deal did not reach a terminal checkpoint")

	dl, err := f.DirectDealsDB.ByID(ctx, dealUuid)
	require.NoError(t, err)
	require.Equal(t, dealcheckpoints.Complete, dl.Checkpoint)
	require.Empty(t, dl.Err, "a deal accepted before nv29 must still complete after it")

	// The chain really did cross the fork.
	nv, err = f.FullNode.StateNetworkVersion(ctx, types.EmptyTSK)
	require.NoError(t, err)
	require.Equal(t, network.Version29, nv, "the chain must be on nv29 by the end of the test")

	// No claim was written for the deal: FIP-0118 writes none from nv29 on. This
	// is what makes the completion above a decision taken from the sector itself.
	claim, err := f.FullNode.StateGetClaim(ctx, f.MinerAddr, verifreg.ClaimId(allocationId), types.EmptyTSK)
	require.NoError(t, err)
	require.Nil(t, claim, "a deal activated after the nv29 fork has no claim")

	// The sector activated after the fork, which is what makes this a post-nv29
	// activation rather than an nv28 one that merely finished late.
	st, err := f.FullNode.StateSectorGetInfo(ctx, f.MinerAddr, dl.SectorID, types.EmptyTSK)
	require.NoError(t, err)
	require.NotNil(t, st, "the deal's sector must be on chain")
	require.Greater(t, int64(st.Activation), int64(upgradeEpoch),
		"the sector must activate after the fork for this test to cover the crossing")
	require.True(t, !st.DealWeight.NilOrZero() || !st.VerifiedDealWeight.NilOrZero(),
		"the sector must carry the deal's spacetime")
}

// TestDirectDealSnapAcrossNv29 covers the other way a direct deal's piece reaches
// a sector: snapped into a committed-capacity sector that is already on chain,
// with the snap's own prove-commit landing after the nv29 fork.
//
// It is the harder half of the crossing, and the reason the sector's activation
// cannot be the thing that dates a deal. A fresh seal activates after the fork,
// so an implementation comparing the activation against the fork height would
// pass the test above by accident. Here the sector was activated and posted long
// before the fork carrying nothing, the deal is accepted before it, and only the
// snap after it gives the sector any weight -- so the sector's activation says
// nv28 while the data says nv29, and FIP-0118 writes no claim either way. The
// sealing watch has to decide it from the sector's own weight.
//
// A snap has nothing on chain to hold the way a seal has its pre-commit
// challenge: the sealer packs the piece as soon as it has a sector to pack it
// into, and submits the update as soon as it is proven. So the hold is one step
// earlier, at the handoff of the piece. The sealer upgrades a CC sector only once
// that sector is in its Available state; leaving the sector in Proving (see
// framework.WithSnapDeals) leaves the piece boost hands over sitting in the
// miner's piece queue, and the snap cannot start. After the fork the test marks
// the sector for upgrade and matches the queued piece to it, which releases the
// snap on the far side of the fork.
//
// Everything here is real: the proofs are the kit's real proofs, so the sealer
// packs the piece, produces a replica-update proof for the sector and submits a
// genuine ProveReplicaUpdate. That is the only option -- the kit's mock sealer
// reports every sector as having no unsealed copy and has ReplicaUpdate return
// empty CIDs, so a snap under mock proofs can neither be read back by boost nor
// even be recorded by the sealing state machine.
//
// The sector size is not what this test is about, so it is small. Every proof a
// real sector costs is priced by its size, and a replica update at the
// framework's default 8MiB is minutes of work on every core -- long enough that
// the test then has to fight its own instrument: the kit's block miner gives up
// for good once it falls behind the wall clock by policy.ChainFinality (900)
// rounds, so a slow chain is needed to keep it mining through the proof. At 2KiB
// (framework.WithSectorSize) the same proof takes seconds, the chain runs at the
// usual hundred milliseconds, and the piece only has to fit in the sector.
func TestDirectDealSnapBeforeNv29(t *testing.T) {
	testDirectDealSnap(t, false)
}

func TestDirectDealSnapAcrossNv29(t *testing.T) {
	testDirectDealSnap(t, true)
}

func testDirectDealSnap(t *testing.T, crossesNv29 bool) {
	ctx := context.Background()
	// Small enough that the CAR -- header, CIDs and all -- pads to a 2KiB piece,
	// which is what the sector below takes. See the piece size logged below.
	fileSize := 1000

	kit.QuietMiningLogs()
	framework.SetLogLevel()

	rootKey, err := key.GenerateKey(types.KTSecp256k1)
	require.NoError(t, err)

	verifier1Key, err := key.GenerateKey(types.KTSecp256k1)
	require.NoError(t, err)

	bal, err := types.ParseFIL("10000fil")
	require.NoError(t, err)

	// The fork is further out than in the fresh-seal test: the CC sector below
	// has to be sealed, posted and activated before the deal is submitted.
	const upgradeEpoch = abi.ChainEpoch(900)
	const submitMargin = abi.ChainEpoch(60)

	var eopts []kit.EnsembleOpt
	if crossesNv29 {
		eopts = append(eopts, kit.LatestActorsAt(upgradeEpoch))
	} else {
		// Keep the chain on nv28 for this run, but retain the actor migration
		// schedule the Lotus harness needs for mining with the latest actors.
		eopts = append(eopts, kit.LatestActorsAt(10_000))
	}
	eopts = append(eopts, kit.RootVerifier(rootKey, abi.NewTokenAmount(bal.Int64())))
	eopts = append(eopts, kit.Account(verifier1Key, abi.NewTokenAmount(bal.Int64())))
	eopts = append(eopts, kit.RealProofs())
	esemble := kit.NewEnsemble(t, eopts...)

	var opts []framework.FrameworkOpts
	opts = append(opts, framework.WithEnsemble(esemble))
	opts = append(opts, framework.SetProvisionalWalletBalances(int64(9e18)))
	opts = append(opts, framework.WithStartEpochSealingBuffer(30))
	// Where the piece lands is the sealer's decision, not boost's: this is what
	// asks it to put it into the committed-capacity sector below rather than seal
	// a new one -- and to leave that sector where a piece cannot reach it until
	// this test says otherwise.
	opts = append(opts, framework.WithSnapDeals())
	// A 2KiB sector instead of the framework's 8MiB: the snap below needs a real
	// replica-update proof, and its cost is the sector size. See the note above.
	opts = append(opts, framework.WithSectorSize(2<<10))
	f := framework.NewTestFramework(ctx, t, opts...)
	esemble.Start()
	blockTime := 100 * time.Millisecond
	esemble.BeginMining(blockTime)

	err = f.Start()
	require.NoError(t, err)
	defer f.Stop()

	// Send funds to PSD wallet as it is being used for POST
	info, err := f.FullNode.StateMinerInfo(ctx, f.MinerAddr, types.EmptyTSK)
	require.NoError(t, err)
	addresses := []address.Address{info.Owner, info.Worker}
	addresses = append(addresses, info.ControlAddresses...)
	for i := 0; i < 3; i++ {
		for _, addr := range addresses {
			err = framework.SendFunds(ctx, f.FullNode, addr, abi.NewTokenAmount(int64(9e18)))
			require.NoError(t, err)
		}
	}

	// Seal one committed-capacity sector. It is what the deal is snapped into, so
	// it has to be on chain before the fork and carry nothing: a sector with a
	// piece in it is not a CC sector and cannot be snapped. Kicked off here and
	// waited on below, so the sealing overlaps the setup that follows.
	pledged, err := f.LotusMiner.PledgeSector(ctx)
	require.NoError(t, err)
	ccSector := pledged.Number

	// Give the boost client's address enough datacap to make the deal. Datacap is
	// deprecated at nv29, so this -- like the allocation below -- has to happen
	// while the chain is still on nv28.
	err = f.AddClientDataCap(t, ctx, rootKey, verifier1Key)
	require.NoError(t, err)

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, fileSize)
	require.NoError(t, err)
	_, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)
	commp, err := storagemarket.GenerateCommPLocally(carFilepath)
	require.NoError(t, err)
	t.Logf("Piece CID: %s, Piece Size: %d", commp.PieceCID.String(), commp.Size)

	// The sealer refuses to snap into a sector that has not been posted yet, so
	// the sector has to be active -- not merely sealed -- before the deal arrives.
	f.LotusMiner.WaitSectorsProving(ctx, map[abi.SectorNumber]struct{}{ccSector: {}})
	f.FullNode.WaitForSectorActive(ctx, t, ccSector, f.MinerAddr)

	ccInfo, err := f.FullNode.StateSectorGetInfo(ctx, f.MinerAddr, ccSector, types.EmptyTSK)
	require.NoError(t, err)
	require.NotNil(t, ccInfo, "the CC sector must be on chain")
	if crossesNv29 {
		require.Less(t, int64(ccInfo.Activation), int64(upgradeEpoch),
			"the CC sector must activate before the fork, or this is not a snap across it")
	}
	require.True(t, ccInfo.DealWeight.NilOrZero() && ccInfo.VerifiedDealWeight.NilOrZero(),
		"the sector the piece is snapped into has to carry nothing of its own")

	mid, err := address.IDFromAddress(f.MinerAddr)
	require.NoError(t, err)

	pieceInfos := []util.PieceInfos{{
		Cid:       commp.PieceCID,
		Size:      int64(commp.Size),
		Miner:     abi.ActorID(mid),
		MinerAddr: f.MinerAddr,
		Tmin:      verifregst.MinimumVerifiedAllocationTerm,
		Tmax:      verifregst.MaximumVerifiedAllocationTerm,
		Exp:       verifregst.MaximumVerifiedAllocationExpiration,
	}}

	allocateMsg, err := util.CreateAllocationMsg(ctx, f.FullNode, pieceInfos, f.ClientAddr, 10)
	require.NoError(t, err)

	sm, err := f.FullNode.MpoolPushMessage(ctx, allocateMsg[0], nil)
	require.NoError(t, err)

	_, err = f.FullNode.StateWaitMsg(ctx, sm.Cid(), 1, 1e10, true)
	require.NoError(t, err)

	allocations, err := f.FullNode.StateGetAllocations(ctx, f.ClientAddr, types.EmptyTSK)
	require.NoError(t, err)
	require.Len(t, allocations, 1)

	var allocationId uint64
	for id := range allocations {
		allocationId = uint64(id)
	}

	// For the crossing case, submit just before the fork; otherwise keep the
	// whole snap on nv28.
	if crossesNv29 {
		f.FullNode.WaitTillChain(ctx, kit.HeightAtLeast(upgradeEpoch-submitMargin))
	}
	head, err := f.FullNode.ChainHead(ctx)
	require.NoError(t, err)
	if crossesNv29 {
		require.Less(t, head.Height(), upgradeEpoch,
			"setup overran the fork epoch; raise upgradeEpoch so the deal is still accepted on nv28")
	}

	// The piece's schedule has to outlast the snap, and the snap is minutes of
	// proving away rather than seconds.
	startEpoch := head.Height() + 2880
	endEpoch := head.Height() + 2880*400

	dealUuid := uuid.New()
	ddParams := smtypes.DirectDealParams{
		DealUUID:           dealUuid,
		AllocationID:       verifreg.AllocationId(allocationId),
		PieceCid:           commp.PieceCID,
		ClientAddr:         f.ClientAddr,
		StartEpoch:         startEpoch,
		EndEpoch:           endEpoch,
		FilePath:           carFilepath,
		DeleteAfterImport:  false,
		RemoveUnsealedCopy: false,
		SkipIPNIAnnounce:   false,
	}

	rej, err := f.Boost.BoostDirectDeal(ctx, ddParams)
	require.NoError(t, err)
	if rej != nil && rej.Reason != "" {
		require.FailNow(t, "direct data import rejected: %s", rej.Reason)
	}
	t.Log("Direct data import scheduled for execution")

	if crossesNv29 {
		// Keep the piece queued until the fork so the snap's prove-commit must
		// land on nv29.
		f.FullNode.WaitTillChain(ctx, kit.HeightAtLeast(upgradeEpoch))
		held, err := f.FullNode.ChainHead(ctx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, int64(held.Height()), int64(upgradeEpoch),
			"the fork has to be behind us before the piece is released")

		heldSector, err := f.LotusMiner.SectorsStatus(ctx, ccSector, false)
		require.NoError(t, err)
		require.Equal(t, "Proving", string(heldSector.State),
			"the CC sector must not be in a state the sealer can put a piece into")
		var heldPieces []string
		for _, p := range heldSector.Pieces {
			heldPieces = append(heldPieces, p.Piece.PieceCID.String())
		}
		require.NotContains(t, heldPieces, commp.PieceCID.String(),
			"the piece must not have reached the sector before the fork; a CC sector still reports its filler piece")

		heldDeal, err := f.DirectDealsDB.ByID(ctx, dealUuid)
		require.NoError(t, err)
		require.NotEqual(t, dealcheckpoints.Complete, heldDeal.Checkpoint,
			"the deal cannot have completed while its piece is held")
	}

	// Marking the sector for upgrade moves it to Available, where the sealer
	// accepts the piece. In the crossing case this release happens after nv29.
	err = f.LotusMiner.SectorMarkForUpgrade(ctx, ccSector, true)
	require.NoError(t, err)

	require.Eventuallyf(t, func() bool {
		st, err := f.LotusMiner.SectorsStatus(ctx, ccSector, false)
		if err != nil {
			t.Logf("reading the sector: %s", err)
			return false
		}
		return st.State == "Available"
	}, 2*time.Minute, time.Second, "the sector never became available for the queued piece")

	err = f.LotusMiner.SectorMatchPendingPiecesToOpenSectors(ctx)
	require.NoError(t, err)

	// The wait below covers the whole snap: the replica-update proof, the
	// ProveReplicaUpdate message and the watch noticing the sector's weight.
	require.Eventuallyf(t, func() bool {
		dl, err := f.DirectDealsDB.ByID(ctx, dealUuid)
		if err != nil || dl.Checkpoint != dealcheckpoints.Complete {
			st, serr := f.LotusMiner.SectorsStatus(ctx, ccSector, false)
			if err != nil {
				t.Logf("reading the deal: %s", err)
			} else {
				t.Logf("deal at %s err %q, sector %d in %s (err %s)",
					dl.Checkpoint, dl.Err, dl.SectorID, st.State, serr)
			}
			return false
		}
		return true
	}, 5*time.Minute, 5*time.Second, "direct deal did not reach a terminal checkpoint")

	dl, err := f.DirectDealsDB.ByID(ctx, dealUuid)
	require.NoError(t, err)
	require.Equal(t, dealcheckpoints.Complete, dl.Checkpoint)
	require.Empty(t, dl.Err, "a deal snapped in after nv29 must still complete")

	nv, err := f.FullNode.StateNetworkVersion(ctx, types.EmptyTSK)
	require.NoError(t, err)
	wantNV := network.Version28
	if crossesNv29 {
		wantNV = network.Version29
	}
	require.Equal(t, wantNV, nv, "the chain must finish on the expected network version")

	// The piece went into the CC sector the test pledged, and not into a new one:
	// without this the run could pass as a fresh seal and cover nothing.
	require.Equal(t, ccSector, dl.SectorID,
		"the deal's piece has to be snapped into the pledged CC sector")

	// A post-upgrade snap has no claim; a pre-upgrade snap must retain the
	// verifreg claim that proves the deal.
	claim, err := f.FullNode.StateGetClaim(ctx, f.MinerAddr, verifreg.ClaimId(allocationId), types.EmptyTSK)
	require.NoError(t, err)
	if crossesNv29 {
		require.Nil(t, claim, "a snap proven after the nv29 fork has no claim")
	} else {
		require.NotNil(t, claim, "a snap proven before nv29 must have a claim")
		require.Equal(t, ccSector, claim.Sector)
	}

	// The sector is still the pre-fork CC one -- same activation -- and the deal
	// is visible only as the spacetime the snap added to it.
	st, err := f.FullNode.StateSectorGetInfo(ctx, f.MinerAddr, ccSector, types.EmptyTSK)
	require.NoError(t, err)
	require.NotNil(t, st, "the deal's sector must be on chain")
	require.Equal(t, ccInfo.Activation, st.Activation,
		"a snap must not re-activate the sector, so the sector's activation cannot date the deal")
	require.True(t, !st.DealWeight.NilOrZero() || !st.VerifiedDealWeight.NilOrZero(),
		"the snap must have left the deal's spacetime on the sector")

	// The sealer's own piece list is the other half of the evidence the watch
	// weighs, so check the piece really is where boost recorded it.
	status, err := f.LotusMiner.SectorsStatus(ctx, ccSector, false)
	require.NoError(t, err)
	var sectorPieces []string
	for _, p := range status.Pieces {
		sectorPieces = append(sectorPieces, p.Piece.PieceCID.String())
	}
	require.Contains(t, sectorPieces, commp.PieceCID.String(),
		"the snapped sector must report the deal's piece at offset %d", dl.Offset)
}
