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

	// The fork height, and how close to it the deal is submitted. Everything the
	// deal needs to exist on nv28 -- the datacap and the allocation -- is created
	// during setup at a much lower height; only the deal itself waits. The margin
	// is shorter than the test network's PreCommitChallengeDelay (150), so a piece
	// accepted this close to the fork cannot prove-commit before it: the sector's
	// activation, and therefore its network version, is nv29.
	const upgradeEpoch = abi.ChainEpoch(700)
	const submitMargin = abi.ChainEpoch(50)

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

	// Wait until just before the fork, then submit: the deal is accepted on nv28
	// and its sector activates on nv29, which is the crossing under test. The
	// margin is shorter than PreCommitChallengeDelay, so the sector cannot finish
	// before the fork. The height check below is a guard on setup overrunning --
	// it is read just before the submission, so it cannot see the exact instant
	// Accept re-reads the head; a rejection is caught below rather than here.
	f.FullNode.WaitTillChain(ctx, kit.HeightAtLeast(upgradeEpoch-submitMargin))
	head, err := f.FullNode.ChainHead(ctx)
	require.NoError(t, err)
	require.Less(t, head.Height(), upgradeEpoch,
		"setup overran the fork epoch; raise upgradeEpoch so the deal is still accepted on nv28")

	startEpoch := head.Height() + 200
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

	// The deal reaches Complete once the sector is proven on chain, which is on
	// the far side of the fork.
	require.Eventuallyf(t, func() bool {
		dl, err := f.DirectDealsDB.ByID(ctx, dealUuid)
		return err == nil && dl.Checkpoint == dealcheckpoints.Complete
	}, 10*time.Minute, 2*time.Second, "direct deal did not reach a terminal checkpoint")

	dl, err := f.DirectDealsDB.ByID(ctx, dealUuid)
	require.NoError(t, err)
	require.Equal(t, dealcheckpoints.Complete, dl.Checkpoint)
	require.Empty(t, dl.Err, "a deal accepted before nv29 must still complete after it")

	// The chain really did cross the fork.
	nv, err := f.FullNode.StateNetworkVersion(ctx, types.EmptyTSK)
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
