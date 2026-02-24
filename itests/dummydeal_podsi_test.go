package itests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-data-segment/datasegment"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestDummyPodsiDealOnline(t *testing.T) {
	randomFileSize := int(1e6)

	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	var opts []framework.FrameworkOpts
	opts = append(opts, framework.SetMaxStagingBytes(10e9), framework.SetProvisionalWalletBalances(9e18))
	f := framework.NewTestFramework(ctx, t, opts...)
	err := f.Start()
	require.NoError(t, err)
	defer f.Stop()

	err = f.AddClientProviderBalance(abi.NewTokenAmount(5e18))
	require.NoError(t, err)

	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	// create a random file
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, randomFileSize)
	require.NoError(t, err)

	carFile := filepath.Join(tempdir, "test.car")
	dataSegmentFile := filepath.Join(tempdir, "datasegment.dat")

	// pack it into the car
	rootCid, err := createCar(t, carFile, []string{randomFilepath})
	require.NoError(t, err)

	// pack the car into data segement piece twice so that we have two segments
	makeDataSegmentPiece(t, dataSegmentFile, []string{carFile, carFile})

	// Start a web server to serve the car files
	log.Debug("starting webserver")
	server, err := testutil.HttpTestFileServer(t, tempdir)
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	log.Debug("creating dummy deal")
	dealUuid := uuid.New()

	// Make a deal
	res, err := f.MakeDummyDeal(dealUuid, dataSegmentFile, rootCid, server.URL+"/"+filepath.Base(dataSegmentFile), true)
	require.NoError(t, err)
	require.True(t, res.Result.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))
	res1, err := f.Boost.BoostOfflineDealWithData(context.Background(), dealUuid, dataSegmentFile, false)
	require.NoError(t, err)
	require.True(t, res1.Accepted)

	// Wait for the first deal to be added to a sector and cleaned up so space is made
	err = f.WaitForDealAddedToSector(dealUuid)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

}

func makeDataSegmentPiece(t *testing.T, dataSegmentFile string, subPieces []string) {
	readers := make([]io.Reader, 0)
	deals := make([]abi.PieceInfo, 0)
	for _, sp := range subPieces {
		arg, err := os.Open(sp)
		require.NoError(t, err)

		readers = append(readers, arg)
		cp := new(commp.Calc)
		_, _ = io.Copy(cp, arg)
		rawCommP, size, err := cp.Digest()
		require.NoError(t, err)

		_, _ = arg.Seek(0, io.SeekStart)
		c, _ := commcid.DataCommitmentV1ToCID(rawCommP)
		subdeal := abi.PieceInfo{
			Size:     abi.PaddedPieceSize(size),
			PieceCID: c,
		}
		deals = append(deals, subdeal)
	}
	require.NotEqual(t, 0, len(deals))

	_, size, err := datasegment.ComputeDealPlacement(deals)
	require.NoError(t, err)

	overallSize := abi.PaddedPieceSize(size)
	// we need to make this the 'next' power of 2 in order to have space for the index
	next := 1 << (64 - bits.LeadingZeros64(uint64(overallSize+256)))

	a, err := datasegment.NewAggregate(abi.PaddedPieceSize(next), deals)
	require.NoError(t, err)
	out, err := a.AggregateObjectReader(readers)
	require.NoError(t, err)

	// open output file
	fo, err := os.Create(dataSegmentFile)
	require.NoError(t, err)
	defer func() {
		_ = fo.Close()
	}()

	written, err := io.Copy(fo, out)
	require.NoError(t, err)
	require.NotZero(t, written)
}

func createCar(t *testing.T, carFile string, files []string) (cid.Cid, error) {
	// make a cid with the right length that we eventually will patch with the root.
	hasher, err := multihash.GetHasher(multihash.SHA2_256)
	if err != nil {
		return cid.Undef, err
	}
	digest := hasher.Sum([]byte{})
	hash, err := multihash.Encode(digest, multihash.SHA2_256)
	if err != nil {
		return cid.Undef, err
	}
	proxyRoot := cid.NewCidV1(uint64(multicodec.DagPb), hash)

	options := []car.Option{}

	cdest, err := blockstore.OpenReadWrite(carFile, []cid.Cid{proxyRoot}, options...)

	if err != nil {
		return cid.Undef, err
	}

	// Write the unixfs blocks into the store.
	root, err := writeFiles(context.Background(), false, cdest, files...)
	if err != nil {
		return cid.Undef, err
	}

	if err := cdest.Finalize(); err != nil {
		return cid.Undef, err
	}
	// re-open/finalize with the final root.
	return root, car.ReplaceRootsInFile(carFile, []cid.Cid{root})
}

func writeFiles(ctx context.Context, noWrap bool, bs *blockstore.ReadWrite, paths ...string) (cid.Cid, error) {
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		cl, ok := l.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("not a cidlink")
		}
		blk, err := bs.Get(ctx, cl.Cid)
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(blk.RawData()), nil
	}
	ls.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l ipld.Link) error {
			cl, ok := l.(cidlink.Link)
			if !ok {
				return fmt.Errorf("not a cidlink")
			}
			blk, err := blocks.NewBlockWithCid(buf.Bytes(), cl.Cid)
			if err != nil {
				return err
			}
			_ = bs.Put(ctx, blk)
			return nil
		}, nil
	}

	topLevel := make([]dagpb.PBLink, 0, len(paths))
	for _, p := range paths {
		l, size, err := builder.BuildUnixFSRecursive(p, &ls)
		if err != nil {
			return cid.Undef, err
		}
		if noWrap {
			rcl, ok := l.(cidlink.Link)
			if !ok {
				return cid.Undef, fmt.Errorf("could not interpret %s", l)
			}
			return rcl.Cid, nil
		}
		name := path.Base(p)
		entry, err := builder.BuildUnixFSDirectoryEntry(name, int64(size), l)
		if err != nil {
			return cid.Undef, err
		}
		topLevel = append(topLevel, entry)
	}

	// make a directory for the file(s).

	root, _, err := builder.BuildUnixFSDirectory(topLevel, &ls)
	if err != nil {
		return cid.Undef, nil
	}
	rcl, ok := root.(cidlink.Link)
	if !ok {
		return cid.Undef, fmt.Errorf("could not interpret %s", root)
	}

	return rcl.Cid, nil
}
