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
	verifregst "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/lotus/chain/actors/builtin/verifreg"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDirectDeal(t *testing.T) {
	ctx := context.Background()
	fileSize := 2000000
	pieceSize := 8388608

	kit.QuietMiningLogs()
	framework.SetLogLevel()

	// Build parameters needed to set up datacap
	dataCapParams, err := framework.BuildDatacapParams()
	require.NoError(t, err)

	var opts []framework.FrameworkOpts
	opts = append(opts, framework.EnableLegacyDeals(true))
	opts = append(opts, framework.EnsembleOpts(dataCapParams.Opts...))
	f := framework.NewTestFramework(ctx, t, opts...)
	err = f.Start()
	require.NoError(t, err)
	defer f.Stop()

	// Give the boost client's address enough datacap to make the deal
	err = f.AddClientDataCap(ctx, dataCapParams.RootKey, dataCapParams.VerifierKey, pieceSize)
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

	pieceInfos := []string{fmt.Sprintf("%s=%d", commp.PieceCID, pieceSize)}
	minerIds := []string{f.MinerAddr.String()}
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
