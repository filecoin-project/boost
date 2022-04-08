package itests

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDummydeal(t *testing.T) {
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	framework.SetPreCommitChallengeDelay(t, 5)
	f := framework.NewTestFramework(ctx, t)
	err := f.Start()
	require.NoError(t, err)
	defer f.Stop()

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, 2000000)
	require.NoError(t, err)

	failingFilepath, err := testutil.CreateRandomFile(tempdir, 5, 2000000)
	require.NoError(t, err)

	rootCid, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)

	failingRootCid, failingCarFilepath, err := testutil.CreateDenseCARv2(tempdir, failingFilepath)
	require.NoError(t, err)

	// Start a web server to serve the car files
	server, err := testutil.HttpTestFileServer(t, tempdir)
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	dealUuid := uuid.New()

	// Make a deal
	res, err := f.MakeDummyDeal(dealUuid, carFilepath, rootCid, server.URL+"/"+filepath.Base(carFilepath), false)
	require.NoError(t, err)
	require.True(t, res.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))

	time.Sleep(2 * time.Second)

	// Make a second deal - it should fail because the first deal took up all
	// available space
	failingDealUuid := uuid.New()
	res2, err2 := f.MakeDummyDeal(failingDealUuid, failingCarFilepath, failingRootCid, server.URL+"/"+filepath.Base(failingCarFilepath), false)
	require.NoError(t, err2)
	require.Equal(t, "cannot accept piece of size 2254421, on top of already allocated 2254421 bytes, because it would exceed max staging area size 4000000", res2.Reason)
	log.Debugw("got response from MarketDummyDeal for failing deal", "res2", spew.Sdump(res2))

	// Wait for the first deal to be added to a sector and cleaned up so space is made
	err = f.WaitForDealAddedToSector(dealUuid)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Make a third deal - it should succeed because the first deal has been cleaned up
	passingDealUuid := uuid.New()
	res2, err2 = f.MakeDummyDeal(passingDealUuid, failingCarFilepath, failingRootCid, server.URL+"/"+filepath.Base(failingCarFilepath), false)
	require.NoError(t, err2)
	require.True(t, res2.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res2", spew.Sdump(res2))

	// Wait for the deal to be added to a sector
	err = f.WaitForDealAddedToSector(passingDealUuid)
	require.NoError(t, err)
}
