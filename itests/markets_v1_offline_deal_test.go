package itests

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/filecoin-project/boost-gfm/storagemarket"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/stretchr/testify/require"
)

func TestMarketsV1OfflineDeal(t *testing.T) {
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

	rseed := 1
	size := 7 << 20 // 7MiB file
	inPath, err := testutil.CreateRandomFile(f.HomeDir, rseed, size)
	require.NoError(t, err)
	res, err := f.FullNode.ClientImport(ctx, lapi.FileRef{Path: inPath})
	require.NoError(t, err)

	// Get the piece size and commP
	rootCid := res.Root
	pieceInfo, err := f.FullNode.ClientDealPieceCID(ctx, rootCid)
	require.NoError(t, err)

	// Create a new markets v1 deal
	dp := f.DefaultMarketsV1DealParams()
	dp.Data.Root = res.Root
	// Replace with params for manual storage deal (offline deal)
	dp.Data.TransferType = storagemarket.TTManual
	dp.Data.PieceCid = &pieceInfo.PieceCID
	dp.Data.PieceSize = pieceInfo.PieceSize.Unpadded()

	log.Debugw("starting offline deal", "root", res.Root)
	dealProposalCid, err := f.FullNode.ClientStartDeal(ctx, &dp)
	require.NoError(t, err)
	log.Debugw("got deal proposal cid", "cid", dealProposalCid)

	// Wait for the deal to reach StorageDealCheckForAcceptance on the client
	cd, err := f.FullNode.ClientGetDealInfo(ctx, *dealProposalCid)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		cd, _ := f.FullNode.ClientGetDealInfo(ctx, *dealProposalCid)
		fmt.Println(storagemarket.DealStates[cd.State])
		return cd.State == storagemarket.StorageDealCheckForAcceptance
	}, 60*time.Second, 500*time.Millisecond, "actual deal status is %s", storagemarket.DealStates[cd.State])

	// Create a CAR file from the raw file
	log.Debugw("generate out.car for miner")
	carFilePath := filepath.Join(f.HomeDir, "out.car")
	err = f.FullNode.ClientGenCar(ctx, lapi.FileRef{Path: inPath}, carFilePath)
	require.NoError(t, err)

	// Import the CAR file on the miner - this is the equivalent to
	// transferring the file across the wire in a normal (non-offline) deal
	log.Debugw("import out.car in boost")
	err = f.Boost.MarketImportDealData(ctx, *dealProposalCid, carFilePath)
	require.NoError(t, err)

	log.Debugw("wait until offline deal is sealed")
	err = f.WaitDealSealed(ctx, dealProposalCid)
	require.NoError(t, err)

	log.Debugw("offline deal is sealed, starting retrieval", "cid", dealProposalCid, "root", res.Root)
	outPath := f.Retrieve(ctx, t, dealProposalCid, res.Root, true, nil)

	log.Debugw("retrieval of offline deal is done, compare in- and out- files", "in", inPath, "out", outPath)
	kit.AssertFilesEqual(t, inPath, outPath)
}
