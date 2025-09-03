package car

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"testing"

	"github.com/ipfs/boxo/blockservice"
	bstore "github.com/ipfs/boxo/blockstore"
	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/merkledag"
	merkledagpb "github.com/ipfs/boxo/ipld/merkledag/pb"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cidutil"
	"github.com/ipfs/go-datastore"
	dss "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestCarOffsetWriterDagOrder(t *testing.T) {
	ctx := context.Background()
	ds := dss.MutexWrap(datastore.NewMapDatastore())
	bs := bstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, nil)

	// make a 3 block DAG, the links in the root aren't in DAG-PB spec sort order,
	// they should be byte-order sorted by `Name` field, but we switch them here
	// so we can test that traversal happens based on the links as they appear in
	// the encoded bytes, which is what we get with go-codec-dagpb + go-ipld-prime
	aaaa := "aaaa"
	aaaaBlk := merkledag.NewRawNode([]byte(aaaa))
	require.NoError(t, bserv.AddBlock(ctx, aaaaBlk))
	bbbb := "bbbb"
	bbbbBlk := merkledag.NewRawNode([]byte(bbbb))
	require.NoError(t, bserv.AddBlock(ctx, bbbbBlk))
	pbn := &merkledagpb.PBNode{
		Links: []*merkledagpb.PBLink{
			{Hash: bbbbBlk.Cid().Bytes(), Name: &bbbb},
			{Hash: aaaaBlk.Cid().Bytes(), Name: &aaaa},
		},
	}
	rootByts, err := proto.Marshal(pbn)
	require.NoError(t, err)
	rootBlk := blocks.NewBlock(rootByts)
	require.NoError(t, bserv.AddBlock(ctx, rootBlk))

	// execute
	fullCarCow := NewCarOffsetWriter(rootBlk.Cid(), bs, NewBlockInfoCache())
	var fullBuff bytes.Buffer
	require.NoError(t, fullCarCow.Write(context.Background(), &fullBuff, 0))

	// decode result
	reader, err := car.NewCarReader(bytes.NewReader(fullBuff.Bytes()))
	require.NoError(t, err)

	// header
	require.Equal(t, 1, len(reader.Header.Roots))
	require.Equal(t, rootBlk.Cid(), reader.Header.Roots[0])
	blk, err := reader.Next()
	require.NoError(t, err)

	// 3 blocks, in the correct, unsorted order - root, bbbb, aaaa
	require.Equal(t, blk.Cid(), rootBlk.Cid())
	blk, err = reader.Next()
	require.NoError(t, err)
	require.Equal(t, blk.Cid(), bbbbBlk.Cid())
	blk, err = reader.Next()
	require.NoError(t, err)
	require.Equal(t, blk.Cid(), aaaaBlk.Cid())
	_, err = reader.Next()
	require.Error(t, err)
}

func TestCarOffsetWriter(t *testing.T) {
	ds := dss.MutexWrap(datastore.NewMapDatastore())
	bs := bstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, nil)
	dserv := merkledag.NewDAGService(bserv)

	rseed := 5
	size := 2 * 1024 * 1024
	source := io.LimitReader(rand.New(rand.NewSource(int64(rseed))), int64(size))
	nd, err := DAGImport(dserv, source)
	require.NoError(t, err)

	// Write the CAR to a buffer from offset 0 so the buffer can be used for
	// comparison
	payloadCid := nd.Cid()
	fullCarCow := NewCarOffsetWriter(payloadCid, bs, NewBlockInfoCache())
	var fullBuff bytes.Buffer
	err = fullCarCow.Write(context.Background(), &fullBuff, 0)
	require.NoError(t, err)

	fullCar := fullBuff.Bytes()
	header := carHeader(nd.Cid())
	headerSize, err := car.HeaderSize(&header)

	testCases := []struct {
		name   string
		offset uint64
	}{{
		name:   "1 byte offset",
		offset: 1,
	}, {
		name:   "offset < header size",
		offset: headerSize - 1,
	}, {
		name:   "offset == header size",
		offset: headerSize,
	}, {
		name:   "offset > header size",
		offset: headerSize + 1,
	}, {
		name:   "offset > header + one block size",
		offset: headerSize + 1024*1024 + 512*1024,
	}}

	runTestCases := func(name string, runTCWithCow func() *CarOffsetWriter) {
		for _, tc := range testCases {
			t.Run(name+" - "+tc.name, func(t *testing.T) {
				cow := runTCWithCow()
				var buff bytes.Buffer
				err = cow.Write(context.Background(), &buff, tc.offset)
				require.NoError(t, err)
				require.Equal(t, len(fullCar)-int(tc.offset), len(buff.Bytes()))
				require.Equal(t, fullCar[tc.offset:], buff.Bytes())
			})
		}
	}

	// Run tests with a new CarOffsetWriter
	runTestCases("new car offset writer", func() *CarOffsetWriter {
		return NewCarOffsetWriter(payloadCid, bs, NewBlockInfoCache())
	})

	// Run tests with a CarOffsetWriter that has already been used to write
	// a CAR starting at offset 0
	runTestCases("fully written car offset writer", func() *CarOffsetWriter {
		fullCarCow := NewCarOffsetWriter(payloadCid, bs, NewBlockInfoCache())
		var buff bytes.Buffer
		err = fullCarCow.Write(context.Background(), &buff, 0)
		require.NoError(t, err)
		return fullCarCow
	})

	// Run tests with a CarOffsetWriter that has already been used to write
	// a CAR starting at offset 1
	runTestCases("car offset writer written from offset 1", func() *CarOffsetWriter {
		fullCarCow := NewCarOffsetWriter(payloadCid, bs, NewBlockInfoCache())
		var buff bytes.Buffer
		err = fullCarCow.Write(context.Background(), &buff, 1)
		require.NoError(t, err)
		return fullCarCow
	})

	// Run tests with a CarOffsetWriter that has already been used to write
	// a CAR starting part way through the second block
	runTestCases("car offset writer written from offset 1.5 blocks", func() *CarOffsetWriter {
		fullCarCow := NewCarOffsetWriter(payloadCid, bs, NewBlockInfoCache())
		var buff bytes.Buffer
		err = fullCarCow.Write(context.Background(), &buff, 1024*1024+512*1024)
		require.NoError(t, err)
		return fullCarCow
	})

	// Run tests with a CarOffsetWriter that has already been used to write
	// a CAR repeatedly
	runTestCases("car offset writer written from offset repeatedly", func() *CarOffsetWriter {
		fullCarCow := NewCarOffsetWriter(payloadCid, bs, NewBlockInfoCache())
		var buff bytes.Buffer
		err = fullCarCow.Write(context.Background(), &buff, 1024)
		require.NoError(t, err)
		var buff2 bytes.Buffer
		err = fullCarCow.Write(context.Background(), &buff2, 10)
		require.NoError(t, err)
		var buff3 bytes.Buffer
		err = fullCarCow.Write(context.Background(), &buff3, 1024*1024+512*1024)
		require.NoError(t, err)
		return fullCarCow
	})
}

func TestSkipWriter(t *testing.T) {
	testCases := []struct {
		name     string
		size     int
		skip     int
		expected int
	}{{
		name:     "no skip",
		size:     1024,
		skip:     0,
		expected: 1024,
	}, {
		name:     "skip 1",
		size:     1024,
		skip:     1,
		expected: 1023,
	}, {
		name:     "skip all",
		size:     1024,
		skip:     1024,
		expected: 0,
	}, {
		name:     "skip overflow",
		size:     1024,
		skip:     1025,
		expected: 0,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buff bytes.Buffer
			write := func(sw io.Writer) (int, error) {
				bz := make([]byte, tc.size)
				return sw.Write(bz)
			}
			count, err := skipWrite(&buff, uint64(tc.skip), write)
			require.NoError(t, err)
			require.Equal(t, tc.expected, count)
			require.Equal(t, tc.expected, len(buff.Bytes()))
		})
	}
}

var DefaultHashFunction = uint64(mh.SHA2_256)

func DAGImport(dserv format.DAGService, fi io.Reader) (format.Node, error) {
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return nil, err
	}
	prefix.MhType = DefaultHashFunction

	spl := chunk.NewSizeSplitter(fi, 1024*1024)
	dbp := helpers.DagBuilderParams{
		Maxlinks:  1024,
		RawLeaves: true,

		// NOTE: InlineBuilder not recommended, we are using this to test identity CIDs
		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   32,
		},

		Dagserv: dserv,
	}

	db, err := dbp.New(spl)
	if err != nil {
		return nil, err
	}

	return balanced.Layout(db)
}
