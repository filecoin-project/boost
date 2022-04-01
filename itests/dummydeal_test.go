package itests

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/testutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestDummydeal(t *testing.T) {
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
	res, err := f.makeDummyDeal(dealUuid, carFilepath, rootCid, server.URL+"/"+filepath.Base(carFilepath), false, abi.NewTokenAmount(2000000))
	require.NoError(t, err)
	require.True(t, res.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))

	time.Sleep(2 * time.Second)

	failingDealUuid := uuid.New()
	res2, err2 := f.makeDummyDeal(failingDealUuid, failingCarFilepath, failingRootCid, server.URL+"/"+filepath.Base(failingCarFilepath), false, abi.NewTokenAmount(2000000))
	require.NoError(t, err2)
	require.Equal(t, "cannot accept piece of size 2254421, on top of already allocated 2254421 bytes, because it would exceed max staging area size 4000000", res2.Reason)
	log.Debugw("got response from MarketDummyDeal for failing deal", "res2", spew.Sdump(res2))

	// Wait for the deal to be added to a sector and be cleanedup so space is made
	err = f.waitForDealAddedToSector(dealUuid)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	passingDealUuid := uuid.New()
	res2, err2 = f.makeDummyDeal(passingDealUuid, failingCarFilepath, failingRootCid, server.URL+"/"+filepath.Base(failingCarFilepath), false, abi.NewTokenAmount(2000000))
	require.NoError(t, err2)
	require.True(t, res2.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res2", spew.Sdump(res2))

	// Wait for the deal to be added to a sector
	err = f.waitForDealAddedToSector(passingDealUuid)
	require.NoError(t, err)

	// make an offline deal
	offlineDealUuid := uuid.New()
	res, err = f.makeDummyDeal(offlineDealUuid, carFilepath, rootCid, server.URL+"/"+filepath.Base(carFilepath), true, abi.NewTokenAmount(2000000))
	require.NoError(t, err)
	require.True(t, res.Accepted)
	res, err = f.boost.BoostOfflineDealWithData(context.Background(), offlineDealUuid, carFilepath)
	require.NoError(t, err)
	require.True(t, res.Accepted)
	err = f.waitForDealAddedToSector(offlineDealUuid)
	require.NoError(t, err)

	// Deal fails because client price less than min ask
	res, err = f.makeDummyDeal(dealUuid, carFilepath, rootCid, server.URL+"/"+filepath.Base(carFilepath), false, abi.NewTokenAmount(1000))
	require.NoError(t, err)
	require.False(t, res.Accepted)
	require.Contains(t, res.Reason, "storage price per epoch less than asking price")
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))
}
