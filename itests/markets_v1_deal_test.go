package itests

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/filecoin-project/boost/pkg/devnet"
	"github.com/filecoin-project/boost/testutil"
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

	path, err := testutil.CreateRandomFile(f.t.TempDir(), rseed, size)
	require.NoError(f.t, err)
	res, err := f.fullNode.ClientImport(ctx, lapi.FileRef{Path: path})
	require.NoError(f.t, err)

	// Create a new markets v1 deal
	dp := f.DefaultMarketsV1DealParams()
	dp.Data.Root = res.Root

	dealProposalCid, err := f.fullNode.ClientStartDeal(ctx, &dp)
	require.NoError(t, err)

	f.WaitDealSealed(ctx, dealProposalCid)

	//TODO: confirm retrieval works

	cancel()
	go f.stop()
	<-done
}
