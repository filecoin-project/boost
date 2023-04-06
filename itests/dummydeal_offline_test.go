package itests

import (
	"context"
	"testing"

	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDummydealOffline(t *testing.T) {
	ctx := context.Background()

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	f := framework.NewTestFramework(ctx, t)
	err := f.Start()
	require.NoError(t, err)
	defer f.Stop()

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, 2000000)
	require.NoError(t, err)
	rootCid, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)

	// make an offline deal
	offlineDealUuid := uuid.New()
	dealRes, err := f.MakeDummyDeal(offlineDealUuid, carFilepath, rootCid, "", true)
	res := dealRes.Result
	require.NoError(t, err)
	require.True(t, res.Accepted)
	res, err = f.Boost.BoostOfflineDealWithData(context.Background(), offlineDealUuid, carFilepath, false)
	require.NoError(t, err)
	require.True(t, res.Accepted)
	err = f.WaitForDealAddedToSector(offlineDealUuid)
	require.NoError(t, err)
}
