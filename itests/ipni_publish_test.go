package itests

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

type ClientStore struct {
	datastore.Batching
	ipld.LinkSystem
}

func newClientStore() *ClientStore {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := store.Get(lctx.Ctx, datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return store.Put(lctx.Ctx, datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return &ClientStore{
		Batching:   store,
		LinkSystem: lsys,
	}
}

func getAdvertisement(ctx context.Context, t *testing.T, sub *dagsync.Subscriber, store *ClientStore, publisher peer.AddrInfo, adCid cid.Cid) (cid.Cid, schema.Advertisement) {
	adCid, err := sub.SyncAdChain(ctx, publisher, dagsync.WithHeadAdCid(adCid), dagsync.ScopedDepthLimit(1))
	require.NoError(t, err)
	val, err := store.Batching.Get(ctx, datastore.NewKey(adCid.String()))
	require.NoError(t, err)
	ad, err := schema.BytesToAdvertisement(adCid, val)
	require.NoError(t, err)
	_, err = ad.VerifySignature()
	require.NoError(t, err)
	return adCid, ad
}

func TestIPNIPublish(t *testing.T) {
	ctx := context.Background()
	log := framework.Log

	kit.QuietMiningLogs()
	framework.SetLogLevel()
	var opts []framework.FrameworkOpts
	f := framework.NewTestFramework(ctx, t, opts...)
	err := f.Start()
	require.NoError(t, err)
	defer f.Stop()

	// Get the listen address
	addrs, err := f.Boost.NetAddrsListen(ctx)
	require.NoError(t, err)

	// Create new ipni subscriber.
	p2pHost, err := libp2p.New()
	require.NoError(t, err)
	defer p2pHost.Close()
	p2pHost.Peerstore().AddAddrs(addrs.ID, addrs.Addrs, time.Hour)
	store := newClientStore()
	sub, err := dagsync.NewSubscriber(p2pHost, store.LinkSystem)
	require.NoError(t, err)

	// Get head when boost starts
	headAtStartCid, _ := getAdvertisement(ctx, t, sub, store, addrs, cid.Undef)

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	fileSize := 200000
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, fileSize)
	require.NoError(t, err)

	// create a dense carv2 for deal making
	rootCid, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)

	// Start a web server to serve the car files
	log.Debug("starting webserver")
	server, err := testutil.HttpTestFileServer(t, tempdir)
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	log.Debug("creating dummy deal")
	dealUuid := uuid.New()

	// Make a deal
	res, err := f.MakeDummyDeal(dealUuid, carFilepath, rootCid, server.URL+"/"+filepath.Base(carFilepath), false)
	require.NoError(t, err)
	require.True(t, res.Result.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))

	// Wait for the deal to be added to a sector
	err = f.WaitForDealAddedToSector(dealUuid)
	require.NoError(t, err)

	// Get current head after the deal
	_, headAfterDeal := getAdvertisement(ctx, t, sub, store, addrs, cid.Undef)
	require.NoError(t, err)

	// Verify new advertisement is added to the chain and previous head is linked
	require.Equal(t, headAtStartCid, headAfterDeal.PreviousCid())

	// Confirm this ad is not a remove type
	require.False(t, headAfterDeal.IsRm)

	// Check that advertisement has entries
	require.NotNil(t, headAfterDeal.Entries)
	entriesCid := headAfterDeal.Entries.(cidlink.Link).Cid
	require.NotEqual(t, cid.Undef, entriesCid)

	// Sync the entries chain
	err = sub.SyncEntries(ctx, addrs, entriesCid)
	require.NoError(t, err)

	// Get all multihashes - indexer retrieval
	next := entriesCid
	var count int
	var foundRoot bool
	for next != schema.NoEntries.Cid && next != cid.Undef {
		var mhs []multihash.Multihash
		next, mhs, err = store.getEntriesChunk(ctx, next)
		require.NoError(t, err)
		count += len(mhs)
		if !foundRoot {
			rootMh := rootCid.Hash()
			for _, mh := range mhs {
				if bytes.Equal(rootMh, mh) {
					foundRoot = true
					break
				}
			}
		}
	}

	require.Greater(t, count, 0)
	require.True(t, foundRoot, "root cid not found")
}

func (s *ClientStore) getEntriesChunk(ctx context.Context, target cid.Cid) (cid.Cid, []multihash.Multihash, error) {
	n, err := s.LinkSystem.Load(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: target}, schema.EntryChunkPrototype)
	if err != nil {
		return cid.Undef, nil, err
	}

	chunk, err := schema.UnwrapEntryChunk(n)
	if err != nil {
		return cid.Undef, nil, err
	}
	var next cid.Cid
	if chunk.Next == nil {
		next = cid.Undef
	} else {
		next = chunk.Next.(cidlink.Link).Cid
	}

	return next, chunk.Entries, nil
}
