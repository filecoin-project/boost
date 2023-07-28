package itests

import (
	"context"
	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestMultiMinerRetrieval(t *testing.T) {
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()

	// Set up two miners, each with a separate boost instance connected to it
	ensemble := kit.NewEnsemble(t)
	var opts []framework.FrameworkOpts
	opts = append(opts, framework.EnableLegacyDeals(true), framework.WithEnsemble(ensemble))
	boostAndMiner1 := framework.NewTestFramework(ctx, t, opts...)
	boostAndMiner2 := framework.NewTestFramework(ctx, t, opts...)
	ensemble.Start()

	blockTime := 100 * time.Millisecond
	ensemble.BeginMining(blockTime)

	err := boostAndMiner1.Start()
	require.NoError(t, err)
	defer boostAndMiner1.Stop()

	// Get the listen address of the first miner
	miner1ApiInfo, err := boostAndMiner1.LotusMinerApiInfo()
	require.NoError(t, err)

	err = boostAndMiner2.Start(func(cfg *config.Boost) {
		// Set up the second boost instance so that it points at the LID
		// service provided by the first boost instance
		cfg.LocalIndexDirectory.EmbeddedServicePort = 0
		cfg.LocalIndexDirectory.ServiceApiInfo = "ws://localhost:8042"

		// Set up the second boost instance so that it can read sector data
		// not only from the second miner, but also from the first miner
		cfg.StorageAccessApiInfo = []string{cfg.SectorIndexApiInfo, miner1ApiInfo}

		// Set up some other ports so they don't clash
		cfg.Graphql.Port = 8081
		cfg.API.ListenAddress = "/ip4/127.0.0.1/tcp/1289/http"
		cfg.API.RemoteListenAddress = "127.0.0.1:1289"
	})
	require.NoError(t, err)
	defer boostAndMiner2.Stop()

	err = boostAndMiner1.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	fileSize := 200000
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, fileSize)
	require.NoError(t, err)

	// create a dense carv2 for deal making
	rootCid, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)

	// Start a web server to serve the car files
	log.Debug("starting webserver")
	server, err := testutil.HttpTestFileServer(t, tempdir)
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	log.Debug("creating dummy deal")
	dealUuid := uuid.New()

	// Make a storage deal on the first boost, which will store the index to
	// LID and store the data on the first miner
	res, err := boostAndMiner1.MakeDummyDeal(dealUuid, carFilepath, rootCid, server.URL+"/"+filepath.Base(carFilepath), false)
	require.NoError(t, err)
	require.True(t, res.Result.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))

	// Wait for the deal to be added to a sector
	err = boostAndMiner1.WaitForDealAddedToSector(dealUuid)
	require.NoError(t, err)

	// Retrieve the deal from the second boost. It should
	// - get the index of the piece's block offsets from LID
	// - get the deal info from LID
	// - recognize that the deal is for a sector on the first miner
	// - read the data for the deal from the first miner
	log.Debugw("deal is added to piece, starting retrieval", "root", rootCid)
	outPath := boostAndMiner2.RetrieveDirect(ctx, t, rootCid, nil, true)

	log.Debugw("retrieval is done, compare in- and out- files", "in", randomFilepath, "out", outPath)
	kit.AssertFilesEqual(t, randomFilepath, outPath)
}
