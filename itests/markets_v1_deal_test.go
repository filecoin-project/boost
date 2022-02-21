package itests

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/filecoin-project/boost/pkg/devnet"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/require"

	lapi "github.com/filecoin-project/lotus/api"
)

func TestMarketsV1Deal(t *testing.T) {
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

	rseed := 0
	size := 7 << 20 // 7MiB file

	path := CreateRandomFile(f.t, rseed, size)
	res, err := f.fullNode.ClientImport(ctx, lapi.FileRef{Path: path})
	require.NoError(f.t, err)

	// Create a new markets v1 deal
	dp := f.DefaultMarketsV1DealParams()
	dp.Data.Root = res.Root
	dp.FastRetrieval = true
	dp.EpochPrice = abi.NewTokenAmount(62500000) // minimum asking price.
	dealProposalCid, err := f.fullNode.ClientStartDeal(ctx, &dp)
	_ = dealProposalCid
	require.NoError(t, err)

	f.WaitDealSealed(ctx, dealProposalCid, false, false, nil)

	time.Sleep(2 * time.Second)

	cancel()
	go f.stop()
	<-done
}

func CreateRandomFile(t *testing.T, rseed, size int) (path string) {
	if size == 0 {
		size = 1600
	}

	source := io.LimitReader(rand.New(rand.NewSource(int64(rseed))), int64(size))

	file, err := os.CreateTemp(t.TempDir(), "sourcefile.dat")
	require.NoError(t, err)

	n, err := io.Copy(file, source)
	require.NoError(t, err)
	require.EqualValues(t, n, size)

	return file.Name()
}
