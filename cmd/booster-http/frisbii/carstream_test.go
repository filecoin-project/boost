package frisbii_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/filecoin-project/boost/cmd/booster-http/frisbii"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	gstestutil "github.com/ipfs/go-graphsync/testutil"
	"github.com/ipfs/go-unixfsnode"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesstestutil "github.com/ipld/go-trustless-utils/testutil"
	"github.com/stretchr/testify/require"
)

func TestStreamCar(t *testing.T) {
	ctx := context.Background()

	chainLsys := makeLsys()
	tbc := gstestutil.SetupBlockChain(ctx, t, chainLsys, 1000, 100)
	allChainBlocks := tbc.AllBlocks()

	fileLsys := makeLsys()
	fileEnt := unixfs.GenerateFile(t, &fileLsys, rand.Reader, 1<<20)

	dirLsys := makeLsys()
	var dirEnt unixfs.DirEntry
	for {
		dirEnt = GenerateNoDupes(func() unixfs.DirEntry { return unixfs.GenerateDirectory(t, &dirLsys, rand.Reader, 4<<20, false) })
		if len(dirEnt.Children) > 2 { // we want at least 3 children to test the path subset selector
			break
		}
	}

	shardedDirLsys := makeLsys()
	var shardedDirEnt unixfs.DirEntry
	for {
		shardedDirEnt = GenerateNoDupes(func() unixfs.DirEntry { return unixfs.GenerateDirectory(t, &shardedDirLsys, rand.Reader, 4<<20, true) })
		if len(dirEnt.Children) > 2 { // we want at least 3 children to test the path subset selector
			break
		}
	}

	testCases := []struct {
		name        string
		path        datamodel.Path
		scope       trustlessutils.DagScope
		root        cid.Cid
		lsys        linking.LinkSystem
		validate    func(t *testing.T, r io.Reader)
		expectedErr string
	}{
		{
			name:  "chain: all blocks",
			scope: trustlessutils.DagScopeAll,
			root:  tbc.TipLink.(cidlink.Link).Cid,
			lsys:  chainLsys,
			validate: func(t *testing.T, r io.Reader) {
				root, blks := carToBlocks(t, r)
				require.Equal(t, tbc.TipLink.(cidlink.Link).Cid, root)
				require.Equal(t, allChainBlocks, blks)
			},
		},
		{
			name:  "chain: just root",
			scope: trustlessutils.DagScopeBlock,
			root:  tbc.TipLink.(cidlink.Link).Cid,
			lsys:  chainLsys,
			validate: func(t *testing.T, r io.Reader) {
				root, blks := carToBlocks(t, r)
				require.Equal(t, tbc.TipLink.(cidlink.Link).Cid, root)
				require.Equal(t, []blocks.Block{allChainBlocks[0]}, blks)
			},
		},
		{
			name:  "unixfs file",
			scope: trustlessutils.DagScopeAll,
			root:  fileEnt.Root,
			lsys:  fileLsys,
			validate: func(t *testing.T, r io.Reader) {
				root, blks := carToBlocks(t, r)
				require.Equal(t, fileEnt.Root, root)
				require.ElementsMatch(t, fileEnt.SelfCids, blkCids(blks))
			},
		},
		{
			name:  "unixfs directory",
			scope: trustlessutils.DagScopeAll,
			root:  dirEnt.Root,
			lsys:  dirLsys,
			validate: func(t *testing.T, r io.Reader) {
				root, blks := carToBlocks(t, r)
				require.Equal(t, dirEnt.Root, root)
				require.ElementsMatch(t, entCids(dirEnt), blkCids(blks))
			},
		},
		{
			name:  "unixfs sharded directory",
			scope: trustlessutils.DagScopeAll,
			root:  shardedDirEnt.Root,
			lsys:  shardedDirLsys,
			validate: func(t *testing.T, r io.Reader) {
				root, blks := carToBlocks(t, r)
				require.Equal(t, shardedDirEnt.Root, root)
				require.ElementsMatch(t, entCids(shardedDirEnt), blkCids(blks))
			},
		},
		{
			// path that (probably) doesn't exist, shouldn't error but shouldn't
			// return much (this ought to be tested better with a fixture)
			name:  "unixfs sharded directory, no such path",
			scope: trustlessutils.DagScopeAll,
			path:  datamodel.ParsePath(shardedDirEnt.Children[0].Path + "/nope"),
			root:  shardedDirEnt.Root,
			lsys:  shardedDirLsys,
			validate: func(t *testing.T, r io.Reader) {
				root, blks := carToBlocks(t, r)
				require.Equal(t, shardedDirEnt.Root, root)
				cids := blkCids(blks)
				require.Contains(t, cids, shardedDirEnt.Root)
				require.NotEqual(t, len(entCids(shardedDirEnt)), cids) // shouldn't contain the full thing!
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)
			var buf bytes.Buffer
			err := frisbii.StreamCar(ctx, tc.lsys, &buf, trustlessutils.Request{Root: tc.root, Path: tc.path.String(), Scope: tc.scope, Duplicates: false})
			if tc.expectedErr != "" {
				req.EqualError(err, tc.expectedErr)
			} else {
				req.NoError(err)
				tc.validate(t, &buf)
			}
		})
	}
}

func carToBlocks(t *testing.T, r io.Reader) (cid.Cid, []blocks.Block) {
	rdr, err := car.NewBlockReader(r)
	require.NoError(t, err)
	require.Len(t, rdr.Roots, 1)
	blks := make([]blocks.Block, 0)
	for {
		blk, err := rdr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		blks = append(blks, blk)
	}
	return rdr.Roots[0], blks
}

func blkCids(blks []blocks.Block) []cid.Cid {
	cids := make([]cid.Cid, len(blks))
	for i, blk := range blks {
		cids[i] = blk.Cid()
	}
	return cids
}

func entCids(ent unixfs.DirEntry) []cid.Cid {
	cids := make([]cid.Cid, 0)
	var _entCids func(ent unixfs.DirEntry)
	_entCids = func(ent unixfs.DirEntry) {
		cids = append(cids, ent.SelfCids...)
		for _, c := range ent.Children {
			_entCids(c)
		}
	}
	_entCids(ent)
	return cids
}

func makeLsys() linking.LinkSystem {
	store := &trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{
		Bag: make(map[string][]byte),
	}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
	return lsys
}

// TODO: this should probably be an option in unixfsnode/testutil, for
// generators to strictly not return a DAG with duplicates

func GenerateNoDupes(gen func() unixfs.DirEntry) unixfs.DirEntry {
	var check func(unixfs.DirEntry) bool
	var seen map[cid.Cid]struct{}
	check = func(e unixfs.DirEntry) bool {
		for _, c := range e.SelfCids {
			if _, ok := seen[c]; ok {
				return false
			}
			seen[c] = struct{}{}
		}
		for _, c := range e.Children {
			if !check(c) {
				return false
			}
		}
		return true
	}
	for {
		seen = make(map[cid.Cid]struct{})
		gend := gen()
		if check(gend) {
			return gend
		}
	}
}
