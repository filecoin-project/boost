package testutil

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"

	"github.com/ipfs/boxo/blockservice"
	bstore "github.com/ipfs/boxo/blockstore"
	chunk "github.com/ipfs/boxo/chunker"
	offline "github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	ihelper "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipldformat "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multihash"
)

const (
	defaultHashFunction = uint64(multihash.BLAKE2B_MIN + 31)
	unixfsChunkSize     = uint64(1 << 10)
	unixfsLinksPerLevel = 1024
)

type CarRes struct {
	CarFile    string
	OrigFile   string
	Root       cid.Cid
	Blockstore bstore.Blockstore
	CarSize    uint64
}

// CreateRandomFile
func CreateRandomFile(dir string, rseed, size int) (string, error) {
	source := io.LimitReader(rand.New(rand.NewSource(int64(rseed))), int64(size))

	file, err := os.CreateTemp(dir, "sourcefile.dat")
	if err != nil {
		return "", err
	}

	_, err = io.Copy(file, source)
	if err != nil {
		return "", err
	}

	//
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return "", err
	}

	return file.Name(), nil
}

func CreateDenseCARv2(dir, src string) (cid.Cid, string, error) {
	cs := int64(unixfsChunkSize)
	maxlinks := unixfsLinksPerLevel
	// Use carv2
	caropts := []car.Option{
		blockstore.UseWholeCIDs(true),
	}
	return CreateDenseCARWith(dir, src, cs, maxlinks, caropts)
}

// CreateDenseCARv2 generates a "dense" UnixFS CARv2 from the supplied ordinary file.
// A dense UnixFS CARv2 is one storing leaf data. Contrast to CreateRefCARv2.
func CreateDenseCARWith(dir, src string, chunksize int64, maxlinks int, caropts []car.Option) (cid.Cid, string, error) {
	bs := bstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	dagSvc := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

	root, err := WriteUnixfsDAGTo(src, dagSvc, chunksize, maxlinks)
	if err != nil {
		return cid.Undef, "", err
	}

	// Create a UnixFS DAG again AND generate a CARv2 file using a CARv2
	// read-write blockstore now that we have the root.
	out, err := os.CreateTemp(dir, "rand")
	if err != nil {
		return cid.Undef, "", err
	}
	err = out.Close()
	if err != nil {
		return cid.Undef, "", err
	}

	rw, err := blockstore.OpenReadWrite(out.Name(), []cid.Cid{root}, caropts...)
	if err != nil {
		return cid.Undef, "", err
	}

	dagSvc = merkledag.NewDAGService(blockservice.New(rw, offline.Exchange(rw)))

	root2, err := WriteUnixfsDAGTo(src, dagSvc, chunksize, maxlinks)
	if err != nil {
		return cid.Undef, "", err
	}

	err = rw.Finalize()
	if err != nil {
		return cid.Undef, "", err
	}

	if root != root2 {
		return cid.Undef, "", fmt.Errorf("DAG root cid mismatch")
	}

	return root, out.Name(), nil
}

func WriteUnixfsDAGTo(path string, into ipldformat.DAGService, chunksize int64, maxlinks int) (cid.Cid, error) {
	file, err := os.Open(path)
	if err != nil {
		return cid.Undef, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return cid.Undef, err
	}

	// get a IPLD reader path file
	// required to write the Unixfs DAG blocks to a filestore
	rpf, err := files.NewReaderPathFile(file.Name(), file, stat)
	if err != nil {
		return cid.Undef, err
	}

	// generate the dag and get the root
	// import to UnixFS
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return cid.Undef, err
	}

	prefix.MhType = defaultHashFunction

	bufferedDS := ipldformat.NewBufferedDAG(context.Background(), into)
	params := ihelper.DagBuilderParams{
		Maxlinks:  maxlinks,
		RawLeaves: true,
		// NOTE: InlineBuilder not recommended, we are using this to test identity CIDs
		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   126,
		},
		Dagserv: bufferedDS,
		NoCopy:  true,
	}

	db, err := params.New(chunk.NewSizeSplitter(rpf, chunksize))
	if err != nil {
		return cid.Undef, err
	}

	nd, err := balanced.Layout(db)
	if err != nil {
		return cid.Undef, err
	}

	err = bufferedDS.Commit()
	if err != nil {
		return cid.Undef, err
	}

	err = rpf.Close()
	if err != nil {
		return cid.Undef, err
	}

	return nd.Cid(), nil
}
