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
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/stretchr/testify/require"
)

func TestDummydealOnline(t *testing.T) {
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	var opts []framework.FrameworkOpts
	//opts = append(opts, framework.SetMaxStagingBytes(3000000))
	f := framework.NewTestFramework(ctx, t, opts...)
	err := f.Start()
	require.NoError(t, err)
	defer f.Stop()

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	fileSize := 2000000
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, fileSize)
	require.NoError(t, err)

	failingFilepath, err := testutil.CreateRandomFile(tempdir, 6, fileSize)
	require.NoError(t, err)

	// NOTE: these calls to CreateDenseCARv2 have the identity CID builder enabled so will
	// produce a root identity CID for this case. So we're testing deal-making and retrieval
	// where a DAG has an identity CID root
	rootCid, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)

	failingRootCid, failingCarFilepath, err := testutil.CreateDenseCARv2(tempdir, failingFilepath)
	require.NoError(t, err)

	// Start a web server to serve the car files
	log.Debug("starting webserver")
	server, err := testutil.HttpTestFileServer(t, tempdir)
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	log.Debug("creating dummy deal")
	dealUuid := uuid.New()

	// Make a deal
	res, err := f.MakeDummyDeal(dealUuid, carFilepath, rootCid, server.URL+"/"+filepath.Base(carFilepath), false)
	require.NoError(t, err)
	require.True(t, res.Result.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))

	time.Sleep(100 * time.Millisecond)

	// Make a second deal - it should fail because the first deal took up all
	// available space
	failingDealUuid := uuid.New()
	res2, err2 := f.MakeDummyDeal(failingDealUuid, failingCarFilepath, failingRootCid, server.URL+"/"+filepath.Base(failingCarFilepath), false)
	require.NoError(t, err2)
	require.Contains(t, res2.Result.Reason, "no space left", res2.Result.Reason)
	log.Debugw("got response from MarketDummyDeal for failing deal", "res2", spew.Sdump(res2))

	// Wait for the first deal to be added to a sector and cleaned up so space is made
	err = f.WaitForDealAddedToSector(dealUuid)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Make a third deal - it should succeed because the first deal has been cleaned up
	passingDealUuid := uuid.New()
	res2, err2 = f.MakeDummyDeal(passingDealUuid, failingCarFilepath, failingRootCid, server.URL+"/"+filepath.Base(failingCarFilepath), false)
	require.NoError(t, err2)
	require.True(t, res2.Result.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res2", spew.Sdump(res2))

	// Wait for the deal to be added to a sector
	err = f.WaitForDealAddedToSector(passingDealUuid)
	require.NoError(t, err)

	// rootCid is an identity CID
	outFile := f.Retrieve(
		ctx,
		t,
		trustlessutils.Request{Root: rootCid, Scope: trustlessutils.DagScopeAll},
		true,
	)
	kit.AssertFilesEqual(t, randomFilepath, outFile)
}
