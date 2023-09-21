package itests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	gstestutil "github.com/filecoin-project/boost-graphsync/testutil"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	multihash "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestMarketsV1DealAndRetrievalWithIdentityCID(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	var opts []framework.FrameworkOpts
	opts = append(opts, framework.EnableLegacyDeals(true))
	f := framework.NewTestFramework(ctx, t, opts...)
	err := f.Start()
	req.NoError(err)
	defer f.Stop()

	// Create a CAR file
	carPath := f.HomeDir + "/testfile.car"
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

	// Import and make a deal to store

	res, err := f.FullNode.ClientImport(ctx, lapi.FileRef{Path: carPath, IsCAR: true})
	req.NoError(err)

	log.Debugw("imported data for deal")

	dp := f.DefaultMarketsV1DealParams()
	dp.Data.Root = res.Root

	log.Debugw("starting deal", "root", res.Root)
	dealProposalCid, err := f.FullNode.ClientStartDeal(ctx, &dp)
	req.NoError(err)

	log.Debugw("got deal proposal cid", "cid", dealProposalCid)

	err = f.WaitDealSealed(ctx, dealProposalCid)
	req.NoError(err)

	// Deal is stored and sealed, attempt different retrieval forms

	log.Debugw("deal is sealed, starting retrieval", "cid", dealProposalCid, "root", res.Root)
	outPath := f.Retrieve(ctx, t, dealProposalCid, rootLink.(cidlink.Link).Cid, false, selectorparse.CommonSelector_ExploreAllRecursively)

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
