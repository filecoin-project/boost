package testutil

import (
	"bufio"
	"context"
	"io"
	"math/rand"
	"os"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	chunk "github.com/ipfs/go-ipfs-chunker"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	files "github.com/ipfs/go-ipfs-files"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs/importer/balanced"
	"github.com/ipfs/go-unixfs/importer/helpers"
	"github.com/ipld/go-car"
	"github.com/multiformats/go-multihash"
)

const unixfsChunkSize uint64 = 1 << 10

var defaultHashFunction = uint64(multihash.BLAKE2B_MIN + 31)

type CarRes struct {
	CarFile    string
	OrigFile   string
	Root       cid.Cid
	Blockstore bstore.Blockstore
	CarSize    uint64
}

// CreateRandomFile
func CreateRandomFile(rseed, size int) (string, error) {
	source := io.LimitReader(rand.New(rand.NewSource(int64(rseed))), int64(size))

	file, err := os.CreateTemp("", "sourcefile.dat")
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

// CreateRandomCARv1 creates a  normal file with the provided seed and the
// provided size and then transforms it to a CARv1 file and returns it.
func CreateRandomCARv1(rseed, size int) (*CarRes, error) {
	ctx := context.Background()

	source := io.LimitReader(rand.New(rand.NewSource(int64(rseed))), int64(size))

	file, err := os.CreateTemp("", "sourcefile.dat")
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(file, source)
	if err != nil {
		return nil, err
	}

	//
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	bs := bstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	dagSvc := merkledag.NewDAGService(blockservice.New(bs, offline.Exchange(bs)))

	root, err := writeUnixfsDAG(ctx, file, dagSvc)
	if err != nil {
		return nil, err
	}

	// create a CARv1 file from the DAG
	tmp, err := os.CreateTemp("", "randcarv1")
	if err != nil {
		return nil, err
	}

	err = car.WriteCar(ctx, dagSvc, []cid.Cid{root}, tmp)
	if err != nil {
		return nil, err
	}

	_, err = tmp.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	hd, _, err := car.ReadHeader(bufio.NewReader(tmp))
	if err != nil {
		return nil, err
	}

	err = tmp.Close()
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(tmp.Name())
	if err != nil {
		return nil, err
	}

	return &CarRes{
		CarFile:    tmp.Name(),
		CarSize:    uint64(stat.Size()),
		OrigFile:   file.Name(),
		Root:       hd.Roots[0],
		Blockstore: bs,
	}, nil
}

func writeUnixfsDAG(ctx context.Context, rd io.Reader, dag format.DAGService) (cid.Cid, error) {
	rpf := files.NewReaderFile(rd)

	// generate the dag and get the root
	// import to UnixFS
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return cid.Undef, err
	}

	prefix.MhType = defaultHashFunction

	bufferedDS := format.NewBufferedDAG(ctx, dag)
	params := helpers.DagBuilderParams{
		Maxlinks:  1024,
		RawLeaves: true,
		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   126,
		},
		Dagserv: bufferedDS,
	}

	db, err := params.New(chunk.NewSizeSplitter(rpf, int64(unixfsChunkSize)))
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
