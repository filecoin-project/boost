package itests

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/cmd/boost/util"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/storagemarket"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifregst "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	verifregtypes "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet/key"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDirectDeal(t *testing.T) {
	ctx := context.Background()

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	_ = logging.SetLogLevel("fxlog", "WARN")

	rootKey, err := key.GenerateKey(types.KTSecp256k1)
	require.NoError(t, err)

	verifierKey, err := key.GenerateKey(types.KTSecp256k1)
	require.NoError(t, err)

	bal, err := types.ParseFIL("100fil")
	require.NoError(t, err)

	var opts []framework.FrameworkOpts
	opts = append(opts, framework.EnableLegacyDeals(true))
	opts = append(opts, framework.EnsembleOpts(
		kit.RootVerifier(rootKey, abi.NewTokenAmount(bal.Int64())),
		// assign some balance to the verifier so they can send an AddClient message.
		kit.Account(verifierKey, abi.NewTokenAmount(bal.Int64())),
	))
	f := framework.NewTestFramework(ctx, t, opts...)
	err = f.Start()
	require.NoError(t, err)
	defer f.Stop()

	vrh, err := f.FullNode.StateVerifiedRegistryRootKey(ctx, types.TipSetKey{})
	require.NoError(t, err)
	t.Log("Verified Registry Root Key", vrh.String())

	// import the root key.
	rootAddr, err := f.FullNode.WalletImport(ctx, &rootKey.KeyInfo)
	require.NoError(t, err)

	// import the verifier's key.
	verifierAddr, err := f.FullNode.WalletImport(ctx, &verifierKey.KeyInfo)
	require.NoError(t, err)

	params, err := actors.SerializeParams(&verifregtypes.AddVerifierParams{Address: verifierAddr, Allowance: big.NewInt(100000000000)})
	require.NoError(t, err)

	msg := &types.Message{
		From:   rootAddr,
		To:     verifreg.Address,
		Method: verifreg.Methods.AddVerifier,
		Params: params,
		Value:  big.Zero(),
	}

	sm, err := f.FullNode.MpoolPushMessage(ctx, msg, nil)
	require.NoError(t, err, "AddVerifier failed")
	res, err := f.FullNode.StateWaitMsg(ctx, sm.Cid(), 1, lapi.LookbackNoLimit, true)
	require.NoError(t, err)
	require.EqualValues(t, 0, res.Receipt.ExitCode)

	// assign datacap to client
	fileSize := 2000000
	pieceSize := int64(8388608)
	datacap := big.NewInt(pieceSize)

	params, err = actors.SerializeParams(&verifregtypes.AddVerifiedClientParams{Address: f.ClientAddr, Allowance: datacap})
	require.NoError(t, err)

	msg = &types.Message{
		From:   verifierAddr,
		To:     verifreg.Address,
		Method: verifreg.Methods.AddVerifiedClient,
		Params: params,
		Value:  big.Zero(),
	}

	sm, err = f.FullNode.MpoolPushMessage(ctx, msg, nil)
	require.NoError(t, err)

	res, err = f.FullNode.StateWaitMsg(ctx, sm.Cid(), 1, lapi.LookbackNoLimit, true)
	require.NoError(t, err)
	require.EqualValues(t, 0, res.Receipt.ExitCode)

	// check datacap balance
	dcap, err := f.FullNode.StateVerifiedClientStatus(ctx, f.ClientAddr, types.EmptyTSK)
	require.NoError(t, err)
	require.Equal(t, *dcap, datacap)

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

	pieceInfos := []string{fmt.Sprintf("%s=%d", commp.PieceCID, pieceSize)}
	minerIds := []string{f.MinerAddr.String()}
	allocateMsg, err := util.CreateAllocationMsg(ctx, f.FullNode, pieceInfos, minerIds, f.ClientAddr,
		verifregst.MinimumVerifiedAllocationTerm,
		verifregst.MaximumVerifiedAllocationTerm,
		verifregst.MaximumVerifiedAllocationExpiration)
	require.NoError(t, err)

	sm, err = f.FullNode.MpoolPushMessage(ctx, allocateMsg, nil)
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

	dealUuid := uuid.New()
	ddParams := smtypes.DirectDealParams{
		DealUUID:           dealUuid,
		AllocationID:       verifreg.AllocationId(allocationId),
		PieceCid:           commp.PieceCID,
		ClientAddr:         f.ClientAddr,
		StartEpoch:         35000,
		EndEpoch:           518400,
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

	// TODO: uncomment once lotus has updated actors bundle and sealing works
	//err = f.WaitForDealAddedToSector(dealUuid)
	//require.NoError(t, err)
}
