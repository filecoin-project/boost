package itests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	gstestutil "github.com/filecoin-project/boost-graphsync/testutil"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestDealAndRetrievalWithIdentityCID(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	var opts []framework.FrameworkOpts
	opts = append(opts, framework.WithMaxStagingDealsBytes(10000000))
	f := framework.NewTestFramework(ctx, t, opts...)
	err := f.Start()
	req.NoError(err)
	defer f.Stop()

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)
	carPath := tempdir + "/testfile.car"
	log.Debugf("using test car %s", carPath)
	carFile, err := os.Create(carPath)
	req.NoError(err)

	carStore, err := storage.NewReadableWritable(carFile, []cid.Cid{cid.MustParse("bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}, car.WriteAsCarV1(true))
	req.NoError(err)

	lsys := cidlink.DefaultLinkSystem()
	lsys.SetWriteStorage(carStore)
	lsys.SetReadStorage(carStore)

	// Setup our DAG with an identity CID in the middle
	testDag := gstestutil.SetupIdentityDAG(ctx, t, lsys)
	expectedCids := make([]cid.Cid, 0)

	// Fill out the deal with a bunch of random data so we're not trying to do a
	// tiny storage deal.
	rseed := 1
	rnd := rand.New(rand.NewSource(int64(rseed)))
	n, err := qp.BuildList(basicnode.Prototype.List, 8, func(la ipld.ListAssembler) {
		for ii := 0; ii < 7; ii++ {
			rndBytes := make([]byte, 1<<20)
			_, err := rnd.Read(rndBytes)
			req.NoError(err)
			l, err := lsys.Store(ipld.LinkContext{}, rawlp, basicnode.NewBytes(rndBytes))
			req.NoError(err)
			qp.ListEntry(la, qp.Link(l))
			expectedCids = append(expectedCids, l.(cidlink.Link).Cid)
		}
		qp.ListEntry(la, qp.Link(testDag.RootLink))
	})
	req.NoError(err)
	expectedCids = append(expectedCids, toCids(testDag.AllLinksNoIdent)...) // all blocks BUT the identity block
	rootLink, err := lsys.Store(ipld.LinkContext{}, dclp, n)
	req.NoError(err)
	expectedCids = append([]cid.Cid{rootLink.(cidlink.Link).Cid}, expectedCids...)

	req.NoError(carFile.Close())
	req.NoError(car.ReplaceRootsInFile(carPath, []cid.Cid{rootLink.(cidlink.Link).Cid}))
	log.Debugw("filled car, replaced root with correct root", "root", rootLink.String())

	// Start a web server to serve the car files
	log.Debug("starting webserver")
	server, err := testutil.HttpTestFileServer(t, tempdir)
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	log.Debug("creating dummy deal")
	dealUuid := uuid.New()
	root := rootLink.(cidlink.Link).Cid

	// Make a deal
	res, err := f.MakeDummyDeal(dealUuid, carPath, root, server.URL+"/"+filepath.Base(carPath), true)
	require.NoError(t, err)
	require.True(t, res.Result.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))
	dealCid, err := res.DealParams.ClientDealProposal.Proposal.Cid()
	require.NoError(t, err)
	log.Infof("deal ID is : %s", dealCid.String())
	res1, err := f.Boost.BoostOfflineDealWithData(context.Background(), dealUuid, carPath, false)
	require.NoError(t, err)
	require.True(t, res1.Accepted)
	// Wait for the first deal to be added to a sector and cleaned up so space is made
	err = f.WaitForDealAddedToSector(dealUuid)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)

	// Deal is stored and sealed, attempt different retrieval forms

	log.Debugw("deal is sealed, starting retrieval", "cid", dealCid.String(), "root", root.String())
	outPath := f.Retrieve(ctx, t, trustlessutils.Request{Root: root, Scope: trustlessutils.DagScopeAll}, false)

	// Inspect what we got
	gotCids, err := testutil.CidsInCar(outPath)
	req.NoError(err)

	toStr := func(c []cid.Cid) []string {
		// for nicer debugging given that we know the CIDs we expect
		out := make([]string, len(c))
		for i, v := range c {
			out[i] = v.String()
		}
		return out
	}

	req.Equal(toStr(expectedCids), toStr(gotCids))
	log.Debugw("successfully retrieved %d blocks, not including identity block", len(expectedCids))
}

func toCids(links []datamodel.Link) []cid.Cid {
	var cids []cid.Cid
	for _, link := range links {
		cids = append(cids, link.(cidlink.Link).Cid)
		fmt.Println("link.(cidlink.Link).Cid", link.(cidlink.Link).Cid)
	}
	return cids
}

var rawlp = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}

var dclp = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.DagCBOR,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}
