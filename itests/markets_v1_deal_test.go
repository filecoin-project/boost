package itests

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	dstest "github.com/ipfs/go-merkledag/test"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipld/go-car"
	"github.com/minio/blake2b-simd"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/lotus/api"
	lapi "github.com/filecoin-project/lotus/api"

	"github.com/filecoin-project/boost/testutil"
)

func TestMarketsV1Deal(t *testing.T) {
	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	rseed := 0
	size := 7 << 20 // 7MiB file

	inPath, err := testutil.CreateRandomFile(t.TempDir(), rseed, size)
	require.NoError(t, err)
	res, err := f.fullNode.ClientImport(f.ctx, lapi.FileRef{Path: inPath})
	require.NoError(t, err)

	// Create a new markets v1 deal
	dp := f.DefaultMarketsV1DealParams()
	dp.Data.Root = res.Root

	log.Debugw("starting deal", "root", res.Root)
	dealProposalCid, err := f.fullNode.ClientStartDeal(f.ctx, &dp)
	require.NoError(t, err)

	log.Debugw("got deal proposal cid", "cid", dealProposalCid)

	err = f.WaitDealSealed(f.ctx, dealProposalCid)
	require.NoError(t, err)

	log.Debugw("deal is sealed, starting retrieval", "cid", dealProposalCid, "root", res.Root)
	outPath := retrieve(t, f.ctx, dealProposalCid, res.Root, true)

	log.Debugw("retrieval is done, compare in- and out- files", "in", inPath, "out", outPath)
	assertFilesEqual(t, inPath, outPath)
}

func TestMarketsV1OfflineDeal(t *testing.T) {
	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	rseed := 0
	size := 7 << 20 // 7MiB file

	inPath, err := testutil.CreateRandomFile(tempdir, rseed, size)
	require.NoError(t, err)
	res, err := f.fullNode.ClientImport(f.ctx, lapi.FileRef{Path: inPath})
	require.NoError(t, err)

	// Get the piece size and commP
	rootCid := res.Root
	pieceInfo, err := f.fullNode.ClientDealPieceCID(f.ctx, rootCid)
	require.NoError(t, err)

	// Create a new markets v1 deal
	dp := f.DefaultMarketsV1DealParams()
	dp.Data.Root = res.Root
	// Replace with params for manual storage deal (offline deal)
	dp.Data.TransferType = storagemarket.TTManual
	dp.Data.PieceCid = &pieceInfo.PieceCID
	dp.Data.PieceSize = pieceInfo.PieceSize.Unpadded()

	log.Debugw("starting offline deal", "root", res.Root)
	dealProposalCid, err := f.fullNode.ClientStartDeal(f.ctx, &dp)
	require.NoError(t, err)
	log.Debugw("got deal proposal cid", "cid", dealProposalCid)

	// Wait for the deal to reach StorageDealCheckForAcceptance on the client
	cd, err := f.fullNode.ClientGetDealInfo(f.ctx, *dealProposalCid)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		cd, _ := f.fullNode.ClientGetDealInfo(f.ctx, *dealProposalCid)
		return cd.State == storagemarket.StorageDealCheckForAcceptance
	}, 30*time.Second, 1*time.Second, "actual deal status is %s", storagemarket.DealStates[cd.State])

	// Create a CAR file from the raw file
	log.Debugw("generate out.car for miner")
	carFilePath := filepath.Join(tempdir, "out.car")
	err = f.fullNode.ClientGenCar(f.ctx, api.FileRef{Path: inPath}, carFilePath)
	require.NoError(t, err)

	// Import the CAR file on the miner - this is the equivalent to
	// transferring the file across the wire in a normal (non-offline) deal
	log.Debugw("import out.car in boost")
	err = f.boost.MarketImportDealData(f.ctx, *dealProposalCid, carFilePath)
	require.NoError(t, err)

	log.Debugw("wait until offline deal is sealed")
	err = f.WaitDealSealed(f.ctx, dealProposalCid)
	require.NoError(t, err)

	log.Debugw("offline deal is sealed, starting retrieval", "cid", dealProposalCid, "root", res.Root)
	outPath := retrieve(t, f.ctx, dealProposalCid, res.Root, true)

	log.Debugw("retrieval of offline deal is done, compare in- and out- files", "in", inPath, "out", outPath)
	assertFilesEqual(t, inPath, outPath)
}

// assertFilesEqual compares two files by blake2b hash equality and
// fails the test if unequal.
func assertFilesEqual(t *testing.T, left, right string) {
	// initialize hashes.
	leftH, rightH := blake2b.New256(), blake2b.New256()

	// open files.
	leftF, err := os.Open(left)
	require.NoError(t, err)

	rightF, err := os.Open(right)
	require.NoError(t, err)

	// feed hash functions.
	_, err = io.Copy(leftH, leftF)
	require.NoError(t, err)

	_, err = io.Copy(rightH, rightF)
	require.NoError(t, err)

	// compute digests.
	leftD, rightD := leftH.Sum(nil), rightH.Sum(nil)

	require.True(t, bytes.Equal(leftD, rightD))
}

func retrieve(t *testing.T, ctx context.Context, deal *cid.Cid, root cid.Cid, carExport bool) (path string) {
	// perform retrieval.
	info, err := f.fullNode.ClientGetDealInfo(ctx, *deal)
	require.NoError(t, err)

	offers, err := f.fullNode.ClientFindData(ctx, root, &info.PieceCID)
	require.NoError(t, err)
	require.NotEmpty(t, offers, "no offers")

	carFile, err := ioutil.TempFile(t.TempDir(), "ret-car")
	require.NoError(t, err)

	defer carFile.Close() //nolint:errcheck

	caddr, err := f.fullNode.WalletDefaultAddress(ctx)
	require.NoError(t, err)

	updatesCtx, cancel := context.WithCancel(ctx)
	updates, err := f.fullNode.ClientGetRetrievalUpdates(updatesCtx)
	require.NoError(t, err)

	retrievalRes, err := f.fullNode.ClientRetrieve(ctx, offers[0].Order(caddr))
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

	require.NoError(t, f.fullNode.ClientExport(ctx,
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
		actualFile := extractFileFromCAR(t, ctx, carFile)
		ret = actualFile.Name()
		_ = actualFile.Close() //nolint:errcheck
	}

	return ret
}

func extractFileFromCAR(t *testing.T, ctx context.Context, file *os.File) (out *os.File) {
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

	tmpfile, err := ioutil.TempFile(t.TempDir(), "file-in-car")
	require.NoError(t, err)

	defer tmpfile.Close() //nolint:errcheck

	err = files.WriteTo(fil, tmpfile.Name())
	require.NoError(t, err)

	return tmpfile
}
