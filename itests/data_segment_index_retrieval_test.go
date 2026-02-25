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
	carv2 "github.com/ipld/go-car/v2"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/stretchr/testify/require"
)

func TestDataSegmentIndexRetrieval(t *testing.T) {
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	var opts []framework.FrameworkOpts
	opts = append(opts, framework.SetMaxStagingBytes(10000000)) // 10 MB
	f := framework.NewTestFramework(ctx, t, opts...)
	err := f.Start()
	require.NoError(t, err)
	defer f.Stop()

	err = f.Boost.LogSetLevel(ctx, "piecedirectory", "debug")
	require.NoError(t, err)

	err = f.LotusMiner.LogSetLevel(ctx, "stores", "info")
	require.NoError(t, err)

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Start a web server to serve the car files
	log.Debug("starting webserver")
	server, err := testutil.HttpTestFileServer(t, "fixtures")
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	log.Debug("creating dummy deal")
	dealUuid := uuid.New()

	pieceCid, err := cid.Parse("baga6ea4seaqly4jqbnjbw5dz4gpcu5uuu3o3t7ohzjpjx7x6z3v53tkfutogwga")
	require.NoError(t, err)

	// Make a deal
	//res, err := f.MakeDummyDeal(dealUuid, segmentDetails.CarPath, segmentDetails.Piece.PieceCID, server.URL+"/"+filepath.Base(segmentDetails.CarPath), false)
	res, err := f.MakeDummyDeal(dealUuid, "fixtures/final.car", pieceCid, server.URL+"/"+filepath.Base("final.car"), true)
	require.NoError(t, err)
	require.True(t, res.Result.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))
	res1, err := f.Boost.BoostOfflineDealWithData(context.Background(), dealUuid, "fixtures/final.car", false)
	require.NoError(t, err)
	require.True(t, res1.Accepted)

	// Wait for the deal to be added to a sector
	err = f.WaitForDealAddedToSector(dealUuid)
	require.NoError(t, err)

	r1, err := cid.Parse("bafykbzaceaqliwrg6y2bxrhhbbiz3nknhz43yj2bqog4rulu5km5qhkckffuw")
	require.NoError(t, err)
	r2, err := cid.Parse("bafykbzaceccq64xf6yadlbmqpfindtf5x3cssel2fozkhvdyrrtnjnutr5j52")
	require.NoError(t, err)

	outF1 := f.Retrieve(ctx, t, trustlessutils.Request{Root: r1, Scope: trustlessutils.DagScopeAll}, false)
	r, err := carv2.OpenReader(outF1)
	require.NoError(t, err)
	rs, err := r.Roots()
	require.NoError(t, err)
	require.Equal(t, r1, rs[0])
	_ = r.Close()
	outf2 := f.Retrieve(ctx, t, trustlessutils.Request{Root: r2, Scope: trustlessutils.DagScopeAll}, false)
	r, err = carv2.OpenReader(outf2)
	require.NoError(t, err)
	rs, err = r.Roots()
	require.NoError(t, err)
	require.Equal(t, r2, rs[0])
	_ = r.Close()
}
