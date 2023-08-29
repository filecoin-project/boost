package itests

import (
	"context"
	"testing"

	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/stretchr/testify/require"
)

func TestMarketsV1Deal(t *testing.T) {
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	var opts []framework.FrameworkOpts
	opts = append(opts, framework.EnableLegacyDeals(true))
	f := framework.NewTestFramework(ctx, t, opts...)
	err := f.Start()
	require.NoError(t, err)
	defer f.Stop()

	// Create a CAR file
	log.Debugw("using tempdir", "dir", f.HomeDir)
	rseed := 0
	size := 7 << 20 // 7MiB file

	inPath, err := testutil.CreateRandomFile(f.HomeDir, rseed, size)
	require.NoError(t, err)
	res, err := f.FullNode.ClientImport(ctx, lapi.FileRef{Path: inPath})
	require.NoError(t, err)

	// Create a new markets v1 deal
	dp := f.DefaultMarketsV1DealParams()
	dp.Data.Root = res.Root

	log.Debugw("starting deal", "root", res.Root)
	dealProposalCid, err := f.FullNode.ClientStartDeal(ctx, &dp)
	require.NoError(t, err)

	log.Debugw("got deal proposal cid", "cid", dealProposalCid)

	err = f.WaitDealSealed(ctx, dealProposalCid)
	require.NoError(t, err)

	log.Debugw("deal is sealed, starting retrieval", "cid", dealProposalCid, "root", res.Root)
	outPath := f.Retrieve(ctx, t, dealProposalCid, res.Root, true, nil)

	log.Debugw("retrieval is done, compare in- and out- files", "in", inPath, "out", outPath)
	kit.AssertFilesEqual(t, inPath, outPath)
}
