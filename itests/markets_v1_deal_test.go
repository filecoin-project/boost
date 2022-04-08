package itests

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/lotus/api"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	dstest "github.com/ipfs/go-merkledag/test"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipld/go-car"
	"github.com/stretchr/testify/require"
)

func TestMarketsV1Deal(t *testing.T) {
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	framework.SetPreCommitChallengeDelay(t, 5)
	f := framework.NewTestFramework(ctx, t, false)
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
	outPath := retrieve(t, f, ctx, dealProposalCid, res.Root, true)

	log.Debugw("retrieval is done, compare in- and out- files", "in", inPath, "out", outPath)
	kit.AssertFilesEqual(t, inPath, outPath)
}

func TestMarketsV1OfflineDeal(t *testing.T) {
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	framework.SetPreCommitChallengeDelay(t, 5)
	f := framework.NewTestFramework(ctx, t, false)
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
	err = f.FullNode.ClientGenCar(ctx, api.FileRef{Path: inPath}, carFilePath)
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
	outPath := retrieve(t, nil, ctx, dealProposalCid, res.Root, true)

	log.Debugw("retrieval of offline deal is done, compare in- and out- files", "in", inPath, "out", outPath)
	kit.AssertFilesEqual(t, inPath, outPath)
}

func retrieve(t *testing.T, f *framework.TestFramework, ctx context.Context, deal *cid.Cid, root cid.Cid, carExport bool) (path string) {
	// perform retrieval.
	info, err := f.FullNode.ClientGetDealInfo(ctx, *deal)
	require.NoError(t, err)

	offers, err := f.FullNode.ClientFindData(ctx, root, &info.PieceCID)
	require.NoError(t, err)
	require.NotEmpty(t, offers, "no offers")

	carFile, err := ioutil.TempFile(f.HomeDir, "ret-car")
	require.NoError(t, err)

	defer carFile.Close() //nolint:errcheck

	caddr, err := f.FullNode.WalletDefaultAddress(ctx)
	require.NoError(t, err)

	updatesCtx, cancel := context.WithCancel(ctx)
	updates, err := f.FullNode.ClientGetRetrievalUpdates(updatesCtx)
	require.NoError(t, err)

	retrievalRes, err := f.FullNode.ClientRetrieve(ctx, offers[0].Order(caddr))
	require.NoError(t, err)
consumeEvents:
	for {
		var evt api.RetrievalInfo
		select {
		case <-updatesCtx.Done():
			t.Fatal("Retrieval Timed Out")
		case evt = <-updates:
			if evt.ID != retrievalRes.DealID {
				continue
			}
		}
		switch evt.Status {
		case retrievalmarket.DealStatusCompleted:
			break consumeEvents
		case retrievalmarket.DealStatusRejected:
			t.Fatalf("Retrieval Proposal Rejected: %s", evt.Message)
		case
			retrievalmarket.DealStatusDealNotFound,
			retrievalmarket.DealStatusErrored:
			t.Fatalf("Retrieval Error: %s", evt.Message)
		}
	}
	cancel()

	require.NoError(t, f.FullNode.ClientExport(ctx,
		api.ExportRef{
			Root:   root,
			DealID: retrievalRes.DealID,
		},
		api.FileRef{
			Path:  carFile.Name(),
			IsCAR: carExport,
		}))

	ret := carFile.Name()
	if carExport {
		actualFile := extractFileFromCAR(t, f, ctx, carFile)
		ret = actualFile.Name()
		_ = actualFile.Close() //nolint:errcheck
	}

	return ret
}

func extractFileFromCAR(t *testing.T, f *framework.TestFramework, ctx context.Context, file *os.File) (out *os.File) {
	bserv := dstest.Bserv()
	ch, err := car.LoadCar(ctx, bserv.Blockstore(), file)
	require.NoError(t, err)

	b, err := bserv.GetBlock(ctx, ch.Roots[0])
	require.NoError(t, err)

	nd, err := ipld.Decode(b)
	require.NoError(t, err)

	dserv := dag.NewDAGService(bserv)
	fil, err := unixfile.NewUnixfsFile(ctx, dserv, nd)
	require.NoError(t, err)

	tmpfile, err := ioutil.TempFile(f.HomeDir, "file-in-car")
	require.NoError(t, err)

	defer tmpfile.Close() //nolint:errcheck

	err = files.WriteTo(fil, tmpfile.Name())
	require.NoError(t, err)

	return tmpfile
}
