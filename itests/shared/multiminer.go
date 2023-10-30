package shared

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

type RetrievalTest struct {
	BoostAndMiners  *framework.TestFramework
	SampleFilePaths []string
	CarFilepaths    []string
	RootCids        []cid.Cid
}

func RunMultiminerRetrievalTest(t *testing.T, rt func(ctx context.Context, t *testing.T, rt *RetrievalTest)) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kit.QuietMiningLogs()
	framework.SetLogLevel()

	// Set up two miners, each with a separate boost instance connected to it
	ensemble := kit.NewEnsemble(t)
	var opts []framework.FrameworkOpts
	opts = append(opts, framework.EnableLegacyDeals(true), framework.WithEnsemble(ensemble), framework.WithMultipleMiners(2))
	boostAndMiners := framework.NewTestFramework(ctx, t, opts...)
	ensemble.Start()

	blockTime := 100 * time.Millisecond
	ensemble.BeginMining(blockTime)

	err := boostAndMiners.Start()
	require.NoError(t, err)
	defer boostAndMiners.Stop()

	err = boostAndMiners.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	t.Logf("using tempdir %s", tempdir)

	// Start a web server to serve the car files
	t.Logf("starting webserver")
	server, err := testutil.HttpTestFileServer(t, tempdir)
	require.NoError(t, err)
	defer server.Close()

	// make two deals, one on miner1 and one on miner2 to make sure that both are retrievable
	createDealFunc := func(minerAddr address.Address) (cid.Cid, string, string) {
		// Create a new dummy deal
		t.Logf("creating dummy deal")
		dealUuid := uuid.New()

		fileSize := 200000
		randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, fileSize)
		require.NoError(t, err)

		// create a dense carv2 for deal making
		rootCid, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
		require.NoError(t, err)

		// Make a storage deal on the first boost, which will store the index to
		// LID and store the data on the first miner
		res, err := boostAndMiners.MakeDummyDeal(dealUuid, carFilepath, rootCid, server.URL+"/"+filepath.Base(carFilepath), false, minerAddr)
		require.NoError(t, err)
		require.True(t, res.Result.Accepted)
		t.Logf("created MarketDummyDeal %s", spew.Sdump(res))

		// Wait for the deal to be added to a sector
		err = boostAndMiners.WaitForDealAddedToSector(dealUuid)
		require.NoError(t, err)

		return rootCid, randomFilepath, carFilepath
	}

	rootCid1, filePath1, carFilePath1 := createDealFunc(boostAndMiners.MinerAddrs[0])
	rootCid2, filePath2, carFilePath2 := createDealFunc(boostAndMiners.MinerAddrs[1])

	rt(ctx, t, &RetrievalTest{
		BoostAndMiners:  boostAndMiners,
		SampleFilePaths: []string{filePath1, filePath2},
		CarFilepaths:    []string{carFilePath1, carFilePath2},
		RootCids:        []cid.Cid{rootCid1, rootCid2},
	})
}
