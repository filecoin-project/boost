package itests

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/pkg/devnet"
	logging "github.com/ipfs/go-log/v2"
)

var (
	tempHome string // home directory where we hold the repos for various lotus services
	f        *testFramework
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

func TestMain(m *testing.M) {
	setLogLevel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempHome, _ = ioutil.TempDir("", "boost-tests-")

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

	f = newTestFramework(ctx, tempHome)
	err := f.start()
	if err != nil {
		log.Fatalw("test framework failed to start", "err", err.Error())
		os.Exit(1)
	}

	exitcode := m.Run()

	go f.stop()
	cancel()
	<-done

	os.Exit(exitcode)
}
