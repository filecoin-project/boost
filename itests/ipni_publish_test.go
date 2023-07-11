package itests

import (
	"context"
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
	"github.com/ipni/ipni-cli/pkg/adpub"
	"github.com/stretchr/testify/require"
)

func TestIPNIPublish(t *testing.T) {
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

	// Get the listen address
	addrs, err := f.Boost.NetAddrsListen(ctx)
	require.NoError(t, err)

	// Create new ipni-cli client
	ipniClient, err := adpub.NewClient(addrs, adpub.WithTopicName("/indexer/ingest/mainnet"), adpub.WithEntriesDepthLimit(100000))
	require.NoError(t, err)

	// Get head when boost starts
	headAtStart, err := ipniClient.GetAdvertisement(ctx, cid.Undef)
	require.NoError(t, err)

	err = f.AddClientProviderBalance(abi.NewTokenAmount(1e15))
	require.NoError(t, err)

	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	fileSize := 2000000
	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, fileSize)
	require.NoError(t, err)

	// NOTE: these calls to CreateDenseCARv2 have the identity CID builder enabled so will
	// produce a root identity CID for this case. So we're testing deal-making and retrieval
	// where a DAG has an identity CID root
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

	time.Sleep(2 * time.Second)

	// Wait for the first deal to be added to a sector and cleaned up so space is made
	err = f.WaitForDealAddedToSector(dealUuid)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// Get current head after the deal
	headAfterDeal, err := ipniClient.GetAdvertisement(ctx, cid.Undef)
	require.NoError(t, err)

	// Verify new advertisement is added to the chain and previous head is linked
	require.Equal(t, headAtStart.ID, headAfterDeal.PreviousID)

	// Confirm this ad is not a remove type
	require.False(t, headAfterDeal.IsRemove)
	j := rootCid.String()
	x := rootCid.Hash().String()
	y := rootCid.Hash().B58String()
	z := rootCid.Hash().HexString()
	t.Log(x, y, z, j)

	// Get all multihashes - indexer retrieval
	mhs, err := headAfterDeal.Entries.Drain()
	require.NoError(t, err)

	var mhsString []string

	require.Greater(t, len(mhs), 0)
	for _, mh := range mhs {
		mhsString = append(mhsString, mh.String())
	}

	t.Log("Stop")

	t.Log(mhsString)
}
