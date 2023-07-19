package itests

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestDataSegmentIndexRetrieval(t *testing.T) {
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	var opts []framework.FrameworkOpts
	opts = append(opts, framework.EnableLegacyDeals(true))
	opts = append(opts, framework.SetMaxStagingBytes(10000000)) // 10 MB
	f := framework.NewTestFramework(ctx, t, opts...)
	err := f.Start()
	require.NoError(t, err)
	defer f.Stop()

	//err = f.Boost.LogSetLevel(ctx, "piecedirectory", "debug")
	//require.NoError(t, err)

	err = f.LotusMiner.LogSetLevel(ctx, "stores", "info")
	require.NoError(t, err)

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	//// Create a CAR file
	//tempdir := t.TempDir()
	//log.Debugw("using tempdir", "dir", tempdir)
	//
	//// Select the number of car segments to use in test
	//seg := 2
	//
	//// Generate car file containing multiple car files
	//segmentDetails, err := framework.GenerateDataSegmentFiles(t, tempdir, seg)
	//require.NoError(t, err)
	//
	//p := segmentDetails.Piece.PieceCID.String()
	//
	//log.Info(p)

	// Start a web server to serve the car files
	log.Debug("starting webserver")
	server, err := testutil.HttpTestFileServer(t, "fixtures")
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	log.Debug("creating dummy deal")
	dealUuid := uuid.New()

	pieceCid, err := cid.Parse(string("baga6ea4seaqly4jqbnjbw5dz4gpcu5uuu3o3t7ohzjpjx7x6z3v53tkfutogwga"))
	require.NoError(t, err)

	// Make a deal
	//res, err := f.MakeDummyDeal(dealUuid, segmentDetails.CarPath, segmentDetails.Piece.PieceCID, server.URL+"/"+filepath.Base(segmentDetails.CarPath), false)
	res, err := f.MakeDummyDeal(dealUuid, "fixtures/final.car", pieceCid, server.URL+"/"+filepath.Base("final.car"), false)
	require.NoError(t, err)
	require.True(t, res.Result.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))

	// Wait for the deal to be added to a sector
	err = f.WaitForDealAddedToSector(dealUuid)
	require.NoError(t, err)

	// Retrieve and compare the all car files within the deal
	//for i := 0; i < seg; i++ {
	//	for _, r := range segmentDetails.Segments[i].Root {
	//		outFile := f.RetrieveDirect(ctx, t, r, &res.DealParams.ClientDealProposal.Proposal.PieceCID, true)
	//		kit.AssertFilesEqual(t, segmentDetails.Segments[i].FilePath, outFile)
	//	}
	//}
}
