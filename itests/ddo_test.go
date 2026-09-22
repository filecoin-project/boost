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
	"github.com/filecoin-project/boost/testutil"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet/key"
	"github.com/filecoin-project/lotus/itests/kit"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
)

// upgradeHeight is where the ensemble crosses to nv29. It has to be late enough
// that the datacap setup and the deal itself are done at nv28 - verifreg rejects
// AddVerifier from nv29, so none of that flow can run afterwards - and early
// enough that the sector is still sealing, which is what makes the sector prove
// under the new rules. The test asserts that ordering rather than trusting it,
// so a miss shows up as a clear failure instead of a test that quietly stops
// covering the crossing.
const upgradeHeight = abi.ChainEpoch(200)

// TestDirectDealSealingAcrossNv29 runs a direct deal across the upgrade: it is
// accepted at nv28 and its sector finishes sealing at nv29.
//
// FIP-0118 removes the verified registry from sector activation, and the miner
// actor no longer talks to it at all, so the sector proves normally and no claim
// is ever written. Boost used to read that missing claim as the deal having left
// the chain, which is the root of the sealing, piece doctor, migration and UI
// bugs this branch fixes. Testing either side of the upgrade on its own leaves
// this crossing uncovered, which is how those bugs got in.
func TestDirectDealSealingAcrossNv29(t *testing.T) {
	runDirectDealTest(t)
}

// runDirectDealTest drives a direct deal from allocation to a proving sector,
// with the chain upgrading to nv29 while that sector is sealing.
func runDirectDealTest(t *testing.T) {
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
	// The ensemble otherwise defaults to buildconstants.TestNetworkVersion
	// (nv29), where verifreg rejects AddVerifier and none of this flow can run.
	eopts = append(eopts, kit.LatestActorsAt(upgradeHeight))
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

	// The crossing only gets covered if the deal really was accepted before the
	// upgrade. Check it rather than assume it, so that setup drifting past the
	// upgrade height fails loudly instead of silently turning this into a plain
	// nv29 test that can never pass.
	nvAtAccept, err := f.FullNode.StateNetworkVersion(ctx, types.EmptyTSK)
	require.NoError(t, err)
	require.Less(t, nvAtAccept, network.Version29,
		"the deal has to be accepted before nv29; raise upgradeHeight")

	// Wait for sector to start sealing
	time.Sleep(2 * time.Second)

	// Wait till sector 2 is Proving
	states := []lapi.SectorState{lapi.SectorState(sealing.Proving)}
	require.Eventuallyf(t, func() bool {
		stateList, err := f.LotusMiner.SectorsListInStates(ctx, states)
		require.NoError(t, err)
		return len(stateList) == 3
	}, 5*time.Minute, 2*time.Second, "sector 2 is still not proving after 5 minutes")

	assertSealedWithoutClaim(t, ctx, f, allocationId)
}

// assertSealedWithoutClaim is the end state across the upgrade. The sector
// proved at nv29, where the miner actor no longer talks to the verified
// registry, so there is no claim for it - and that is the expected outcome, not
// a fault. What has to hold is that the data really is sealed and stays
// reachable, which is what the rest of Boost has to key off now that the claim
// it used to look for is gone.
func assertSealedWithoutClaim(t *testing.T, ctx context.Context, f *framework.TestFramework, allocationID uint64) {
	nv, err := f.FullNode.StateNetworkVersion(ctx, types.EmptyTSK)
	require.NoError(t, err)
	require.GreaterOrEqual(t, nv, network.Version29, "the chain should have upgraded while the sector was sealing")

	// The sector is on chain and proving, so the data did seal.
	st, err := f.FullNode.StateSectorGetInfo(ctx, f.MinerAddr, abi.SectorNumber(2), types.EmptyTSK)
	require.NoError(t, err)
	require.NotNil(t, st, "the sector should be on chain even though no claim was made for it")

	// No claim was written for the allocation, and none ever will be.
	claims, err := f.FullNode.StateGetClaims(ctx, f.MinerAddr, types.EmptyTSK)
	require.NoError(t, err)
	_, ok := claims[verifreg.ClaimId(allocationID)]
	require.False(t, ok, "FIP-0118 removes verifreg from sector activation, so no claim should exist")

	// The sector really carries the piece, so the data is on disk and not just a
	// sector number that happened to reach Proving.
	si, err := f.LotusMiner.SectorsStatus(ctx, abi.SectorNumber(2), false)
	require.NoError(t, err)
	require.NotEmpty(t, si.Pieces, "the sealed sector should still hold its piece")
}

// TestDirectDealRejectedAtNv29 is the other half of the pair. The crossing test
// above gets a deal in before the upgrade; this one starts at nv29 and checks
// that no deal gets in at all.
//
// FIP-0118 deprecates datacap at nv29, so a direct deal must be turned away at
// Accept() with a reason that says why. Rejecting up front matters: allowing
// the deal through would import data for an allocation that can never be
// claimed.
func TestDirectDealRejectedAtNv29(t *testing.T) {
	ctx := context.Background()

	kit.QuietMiningLogs()
	framework.SetLogLevel()

	// No GenesisNetworkVersion here: the ensemble defaults to
	// buildconstants.TestNetworkVersion, which is nv29, which is where this test
	// needs to run. The proof mode still has to be explicit.
	esemble := kit.NewEnsemble(t, kit.RealProofs())

	var opts []framework.FrameworkOpts
	opts = append(opts, framework.WithEnsemble(esemble))
	opts = append(opts, framework.SetProvisionalWalletBalances(int64(9e18)))
	f := framework.NewTestFramework(ctx, t, opts...)
	esemble.Start()
	esemble.BeginMining(100 * time.Millisecond)

	err := f.Start()
	require.NoError(t, err)
	defer f.Stop()

	nv, err := f.FullNode.StateNetworkVersion(ctx, types.EmptyTSK)
	require.NoError(t, err)
	require.GreaterOrEqual(t, nv, network.Version29, "this test only means anything at nv29 or later")

	// Build a CAR file so the request is well formed in every other respect and
	// the rejection can only be about the network version.
	tempdir := t.TempDir()
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, 7048576)
	require.NoError(t, err)
	_, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)
	commp, err := storagemarket.GenerateCommPLocally(carFilepath)
	require.NoError(t, err)

	head, err := f.FullNode.ChainHead(ctx)
	require.NoError(t, err)

	ddParams := smtypes.DirectDealParams{
		DealUUID:     uuid.New(),
		AllocationID: verifreg.AllocationId(1),
		PieceCid:     commp.PieceCID,
		ClientAddr:   f.ClientAddr,
		StartEpoch:   head.Height() + 200,
		EndEpoch:     head.Height() + 2880*400,
		FilePath:     carFilepath,
	}

	rej, err := f.Boost.BoostDirectDeal(ctx, ddParams)
	require.NoError(t, err, "the deal should be rejected cleanly, not fail with an error")
	require.NotNil(t, rej, "expected a rejection at nv29")
	require.False(t, rej.Accepted, "a direct deal must not be accepted at nv29")
	require.Contains(t, rej.Reason, "network version 29",
		"the rejection should say the network version is why, so an operator is not left guessing")
	require.Contains(t, rej.Reason, "FIP-0118",
		"naming the FIP gives the operator something to look up")
}
