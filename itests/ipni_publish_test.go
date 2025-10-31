package itests

import (
	"context"
	"path/filepath"
	"testing"

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
	f := framework.NewTestFramework(ctx, t, opts...)
	err := f.Start()
	require.NoError(t, err)
	defer f.Stop()

	// Get the listen address
	addrs, err := f.Boost.NetAddrsListen(ctx)
	require.NoError(t, err)

	// Create new ipni-cli client
	ipniClient, err := adpub.NewClient(addrs, adpub.WithEntriesDepthLimit(100000))
	require.NoError(t, err)

	// Get head when boost starts
	headAtStart, err := ipniClient.GetAdvertisement(ctx, cid.Undef)
	require.NoError(t, err)

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
	headAfterDeal, err := ipniClient.GetAdvertisement(ctx, cid.Undef)
	require.NoError(t, err)

	// Verify new advertisement is added to the chain and previous head is linked
	require.Equal(t, headAtStart.ID, headAfterDeal.PreviousID)

	// Confirm this ad is not a remove type
	require.False(t, headAfterDeal.IsRemove)

	// Check that advertisement has entries
	require.True(t, headAfterDeal.HasEntries())

	// Sync the entries chain
	err = ipniClient.SyncEntriesWithRetry(ctx, headAfterDeal.Entries.Root())
	require.NoError(t, err)

	// Get all multihashes - indexer retrieval
	mhs, err := headAfterDeal.Entries.Drain()
	require.NoError(t, err)

	require.Greater(t, len(mhs), 0)

	require.Contains(t, mhs, rootCid.Hash())
}
