package itests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/filecoin-project/boost/cmd/boost/util"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/storagemarket"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	verifregst "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet/key"
	"github.com/filecoin-project/lotus/itests/kit"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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
	eopts = append(eopts, kit.RootVerifier(rootKey, abi.NewTokenAmount(bal.Int64())))
	eopts = append(eopts, kit.Account(verifier1Key, abi.NewTokenAmount(bal.Int64())))
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

	// Send funs to PSD wallet as it is being used for POST
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

	pieceInfos := []string{fmt.Sprintf("%s=%d", commp.PieceCID, commp.Size)}
	minerIds := []string{"f01000"}
	allocateMsg, err := util.CreateAllocationMsg(ctx, f.FullNode, pieceInfos, minerIds, f.ClientAddr,
		verifregst.MinimumVerifiedAllocationTerm,
		verifregst.MaximumVerifiedAllocationTerm,
		verifregst.MaximumVerifiedAllocationExpiration)
	require.NoError(t, err)

	sm, err := f.FullNode.MpoolPushMessage(ctx, allocateMsg, nil)
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

	for {
		secNums, err := f.LotusMiner.SectorsList(ctx)
		require.NoError(t, err)
		if len(secNums) > 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	states := []lapi.SectorState{lapi.SectorState(sealing.Proving)}

	// Exit if sector 2 is now proving
	for {
		stateList, err := f.LotusMiner.SectorsListInStates(ctx, states)
		require.NoError(t, err)
		if len(stateList) > 2 {
			break
		}
		time.Sleep(2 * time.Second)
	}

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
