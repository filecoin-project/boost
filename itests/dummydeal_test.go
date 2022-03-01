package itests

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/pkg/devnet"
	"github.com/filecoin-project/boost/testutil"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
)

func init() {
	build.MessageConfidence = 1
}

func setLogLevel() {
	_ = logging.SetLogLevel("boosttest", "DEBUG")
	_ = logging.SetLogLevel("devnet", "DEBUG")
	_ = logging.SetLogLevel("boost", "DEBUG")
	_ = logging.SetLogLevel("actors", "DEBUG")
	_ = logging.SetLogLevel("provider", "DEBUG")
	_ = logging.SetLogLevel("http-transfer", "DEBUG")
	_ = logging.SetLogLevel("boost-provider", "DEBUG")
	_ = logging.SetLogLevel("storagemanager", "DEBUG")
}

func TestDummydeal(t *testing.T) {
	setLogLevel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempHome := t.TempDir()

	done := make(chan struct{})
	go devnet.Run(ctx, tempHome, done)

	// Wait for the miner to start up by polling it
	minerReadyCmd := "lotus-miner sectors list"
	for waitAttempts := 0; ; waitAttempts++ {
		// Check every second
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}

		cmd := exec.CommandContext(ctx, "sh", "-c", minerReadyCmd)
		cmd.Env = []string{fmt.Sprintf("HOME=%s", tempHome)}
		_, err := cmd.CombinedOutput()
		if err != nil {
			// Still not ready
			if waitAttempts%5 == 0 {
				log.Debugw("miner not ready")
			}
			continue
		}

		// Miner is ready
		log.Debugw("miner ready")
		time.Sleep(5 * time.Second) // wait for AddPiece
		break
	}

	f := newTestFramework(ctx, t, tempHome)
	f.start()

	// Create a CAR file
	tempdir := t.TempDir()
	log.Debugw("using tempdir", "dir", tempdir)

	randomFilepath, err := testutil.CreateRandomFile(tempdir, 5, 2000000)
	require.NoError(t, err)

	failingFilepath, err := testutil.CreateRandomFile(tempdir, 5, 2000000)
	require.NoError(t, err)

	rootCid, carFilepath, err := testutil.CreateDenseCARv2(tempdir, randomFilepath)
	require.NoError(t, err)

	failingRootCid, failingCarFilepath, err := testutil.CreateDenseCARv2(tempdir, failingFilepath)
	require.NoError(t, err)

	// Start a web server to serve the car files
	server, err := testutil.HttpTestFileServer(t, tempdir)
	require.NoError(t, err)
	defer server.Close()

	// Create a new dummy deal
	dealUuid := uuid.New()

	res, err := f.makeDummyDeal(dealUuid, carFilepath, rootCid, server.URL+"/"+filepath.Base(carFilepath))
	require.NoError(t, err)
	require.True(t, res.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res", spew.Sdump(res))

	time.Sleep(2 * time.Second)

	failingDealUuid := uuid.New()
	res2, err2 := f.makeDummyDeal(failingDealUuid, failingCarFilepath, failingRootCid, server.URL+"/"+filepath.Base(failingCarFilepath))
	require.NoError(t, err2)
	require.Equal(t, "cannot accept piece of size 2254421, on top of already allocated 2254421 bytes, because it would exceed max staging area size 4000000", res2.Reason)
	log.Debugw("got response from MarketDummyDeal for failing deal", "res2", spew.Sdump(res2))

	// Wait for the deal to be added to a sector and be cleanedup so space is made
	err = f.waitForDealAddedToSector(dealUuid)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	passingDealUuid := uuid.New()
	res2, err2 = f.makeDummyDeal(passingDealUuid, failingCarFilepath, failingRootCid, server.URL+"/"+filepath.Base(failingCarFilepath))
	require.NoError(t, err2)
	require.True(t, res2.Accepted)
	log.Debugw("got response from MarketDummyDeal", "res2", spew.Sdump(res2))

	// Wait for the deal to be added to a sector
	err = f.waitForDealAddedToSector(passingDealUuid)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)

	cancel()
	go f.stop()
	<-done
}
