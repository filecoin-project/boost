package itests

import (
	"context"
	"math"
	"testing"

	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
)

func TestMarketsV1DealRetrieval(t *testing.T) {
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
	rseed := 0
	size := 7 << 20 // 7MiB file

	inPath, dirEnt := testutil.CreateRandomUnixfsFileInCar(t, f.HomeDir, rseed, size)
	root := dirEnt.Root
	leaves := dirEnt.SelfCids[:len(dirEnt.SelfCids)-1]
	/*

		For a 7MiB file with seed 0, we expect to have this structure which we
		can perform range selections on. The root is dirEnt.Root, the raw leaf
		blocks are dirEnt.SelfCid, in order, minus the last one which is also
		the root.

		bafybeiet25r42mzboo5osbxmze5gq45zb4peqksscavjb4ixycwjkbwioa | File      | /[0:7340031] (7340032 B)
		bafkreifrdfke6yoyrjhhqbwkddacapg42vuzcbvwyau6ol24fbjxgfrsha | RawLeaf   | ↳ /0[0:256143] (256144 B)
		bafkreigmdnzqhuaaqrkzhwo2cqa7heshji5neoygkqku2vrubuvzz4twti | RawLeaf   |   /0[256144:512287] (256144 B)
		bafkreidoafwiruc3tqk4fsvql6jfjhsi36pvmr334qwvfjzupkjnws2c7a | RawLeaf   |   /0[512288:768431] (256144 B)
		bafkreibcmiojncftk5kbjo4xdv4onlxacg6dfi3d3objfl4y7dqhld4a3e | RawLeaf   |   /0[768432:1024575] (256144 B)
		bafkreidpvdovcyfj4ypodcw47xgodpkwlq7yokmofzpxto5v3fjcwtaerm | RawLeaf   |   /0[1024576:1280719] (256144 B)
		bafkreicm6tt6ahmimqwuoxuc7m2jdlmlexiofxmsigfsvi4nsuda45kfpu | RawLeaf   |   /0[1280720:1536863] (256144 B)
		bafkreibnq24344buagftasgfpeomzjf6bbnhku4sw4vv3lnisx7wmvv7de | RawLeaf   |   /0[1536864:1793007] (256144 B)
		bafkreieeqrvyz7z3ezshaqlp24zd5jfnmlzb2gnsuie3rltyhxlyb3f46m | RawLeaf   |   /0[1793008:2049151] (256144 B)
		bafkreiaee3232qc3f3dsv5hezroy27va3cmcf35evzkw426liggu4qqguq | RawLeaf   |   /0[2049152:2305295] (256144 B)
		bafkreiccfzitpdvr3d2tyo2ly6e3qjqcc6vmnr4jfir3ys3zxsvaviaqmy | RawLeaf   |   /0[2305296:2561439] (256144 B)
		bafkreiek37qlpud6koj4jybkrwudic7bvdkraion37xk2x7hxmwyrqxzdi | RawLeaf   |   /0[2561440:2817583] (256144 B)
		bafkreifdib756bpqzucggrqp3tbv3c2mvf3kl6ir5erscbsmxt35lv3qf4 | RawLeaf   |   /0[2817584:3073727] (256144 B)
		bafkreiaflhrfx65ukovmuacvktlopj3j5d6iacvmxgi5vyyelqe77laxda | RawLeaf   |   /0[3073728:3329871] (256144 B)
		bafkreia43b3mtjeo5ycrur5dmj72wpktnt5r2tt6u26djl3syuwt2wirmq | RawLeaf   |   /0[3329872:3586015] (256144 B)
		bafkreic5vfpokxgadhzrx5s2yjoqtif6px6hlkqndvgscd3kgxf22omogy | RawLeaf   |   /0[3586016:3842159] (256144 B)
		bafkreibejeisigfddgxjq3gqirbsjcaikpm5ukp6hyelcdarim3mw4gusm | RawLeaf   |   /0[3842160:4098303] (256144 B)
		bafkreibua7gdtdndlwdpntzdzoleomfy4cuawbou2ibqcwdiqcd5c2cktq | RawLeaf   |   /0[4098304:4354447] (256144 B)
		bafkreihce3ojxvomz2afkl4sr67qlje3vvxqcnuugb2mekjocuf2m3axh4 | RawLeaf   |   /0[4354448:4610591] (256144 B)
		bafkreih6gxdebrtqukp7dgpz6uq4mcljkk42aont4f2fmu7yw3qvst5s44 | RawLeaf   |   /0[4610592:4866735] (256144 B)
		bafkreihsyox3ebuboypzrzdnja7mypcq7lugd7yajnyxswaphoqrfj3mvi | RawLeaf   |   /0[4866736:5122879] (256144 B)
		bafkreihqf3wqz5tv3vqlk7s2no7hchw2327ptwowfak773uvddarif2lzi | RawLeaf   |   /0[5122880:5379023] (256144 B)
		bafkreig6hrykcrosz7jwljalgb2bhdh3r2e43gatvy2q3zn7nbck77dnou | RawLeaf   |   /0[5379024:5635167] (256144 B)
		bafkreihblmdcjlmwp5p4esqn6e2h6ko5oytxilpb3msosjjdw5eaarakye | RawLeaf   |   /0[5635168:5891311] (256144 B)
		bafkreihfmufz2fss7oqxqg2gsz4ow5l7i3byhzuqmxev73bu5fi4vdklc4 | RawLeaf   |   /0[5891312:6147455] (256144 B)
		bafkreid3smar6sdcmi7io6e3ewdv3b3gdcyq7yrdznounhsrkswjdmptoa | RawLeaf   |   /0[6147456:6403599] (256144 B)
		bafkreif54hyudjfodmtdphrhass4rxrrle36xvzj5osfcuue52ck4i7pr4 | RawLeaf   |   /0[6403600:6659743] (256144 B)
		bafkreideyfbj2ljnows5gxnsibvpziyhr26azpae6ilesnhhaissbx5ori | RawLeaf   |   /0[6659744:6915887] (256144 B)
		bafkreighy3cp6pvp3pluq4ragvgrycom4s5upargqkawcyu3rw4flturky | RawLeaf   |   /0[6915888:7172031] (256144 B)
		bafkreifokzy5zcluf3hj23nkrvr7tx6sivpshkd4be5tpfibk6vm2mzlxy | RawLeaf   |   /0[7172032:7340031] (168000 B)
	*/

	// Import and make a deal to store

	res, err := f.FullNode.ClientImport(ctx, lapi.FileRef{Path: inPath, IsCAR: true})
	require.NoError(t, err)

	dp := f.DefaultMarketsV1DealParams(f.MinerAddrs[0])
	dp.Data.Root = res.Root

	log.Debugw("starting deal", "root", res.Root)
	dealProposalCid, err := f.FullNode.ClientStartDeal(ctx, &dp)
	require.NoError(t, err)

	log.Debugw("got deal proposal cid", "cid", dealProposalCid)

	err = f.WaitDealSealed(ctx, dealProposalCid)
	require.NoError(t, err)

	// Deal is stored and sealed, attempt different retrieval forms

	retrievalCases := []struct {
		name                   string
		selector               datamodel.Node
		matcherFrom, matcherTo int64
		expectCids             []cid.Cid
	}{
		{
			name:       "full file, explore-all",
			selector:   unixfsnode.UnixFSPathSelectorBuilder("", unixfsnode.ExploreAllRecursivelySelector, false),
			expectCids: append([]cid.Cid{root}, leaves...),
		},
		{
			name:        "slice: 0 to 7MiB",
			matcherFrom: 0,
			matcherTo:   7 << 20,
			expectCids:  append([]cid.Cid{root}, leaves...),
		},
		{
			name:        "slice: 1MiB to 2MiB",
			matcherFrom: 1 << 20,
			matcherTo:   2 << 20,
			expectCids:  append([]cid.Cid{root}, leaves[4:9]...),
		},
		{
			name:        "slice: first byte",
			matcherFrom: 0,
			matcherTo:   1,
			expectCids:  append([]cid.Cid{root}, leaves[0]),
		},
		{
			name:        "slice: last byte",
			matcherFrom: 7340031,
			matcherTo:   7340032,
			expectCids:  append([]cid.Cid{root}, leaves[len(leaves)-1]),
		},
		{
			name:        "slice: last two blocks, negative range, boundary",
			matcherFrom: -168000 - 1,
			matcherTo:   math.MaxInt64,
			expectCids:  append([]cid.Cid{root}, leaves[len(leaves)-2:]...),
		},
		{
			name:        "slice: last block, negative range, boundary",
			matcherFrom: -168000,
			matcherTo:   math.MaxInt64,
			expectCids:  append([]cid.Cid{root}, leaves[len(leaves)-1]),
		},
		{
			// In this case we are attempting to traverse beyond the file to a
			// path that doesn't exist; we expect to only match the root and
			// return that. This is not strictly an error case, it's up to the
			// consumer of this data to verify the path doesn't resolve in the
			// data they get back.
			name:       "path beyond file",
			selector:   unixfsnode.UnixFSPathSelectorBuilder("not/a/path", unixfsnode.ExploreAllRecursivelySelector, false),
			expectCids: []cid.Cid{root},
		},
	}

	for _, tc := range retrievalCases {
		t.Run(tc.name, func(t *testing.T) {
			selNode := tc.selector
			if selNode == nil {
				// build a selector from the specified slice matcher range
				ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
				ss := ssb.ExploreInterpretAs("unixfs", ssb.MatcherSubset(tc.matcherFrom, tc.matcherTo))
				selNode = ss.Node()
			}

			log.Debugw("deal is sealed, starting retrieval", "cid", dealProposalCid, "root", res.Root)
			outPath := f.Retrieve(ctx, t, dealProposalCid, res.Root, false, selNode)

			// Inspect what we got
			gotCids, err := testutil.CidsInCar(outPath)
			require.NoError(t, err)

			toStr := func(c []cid.Cid) []string {
				// for nicer debugging given that we know the CIDs we expect
				out := make([]string, len(c))
				for i, v := range c {
					out[i] = v.String()
				}
				return out
			}

			require.Equal(t, toStr(tc.expectCids), toStr(gotCids))
		})
	}
}
