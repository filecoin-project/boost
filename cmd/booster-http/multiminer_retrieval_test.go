package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"github.com/filecoin-project/boost/itests/shared"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/stretchr/testify/require"
)

func TestMultiMinerHttpRetrieval(t *testing.T) {
	shared.RunMultiminerRetrievalTest(t, func(ctx context.Context, t *testing.T, rt *shared.RetrievalTest) {
		minerApiInfos, err := rt.BoostAndMiners.LotusMinerApiInfos()
		require.NoError(t, err)

		fullNode2ApiInfo, err := rt.BoostAndMiners.LotusFullNodeApiInfo()
		require.NoError(t, err)

		runCtx, cancelRun := context.WithCancel(ctx)
		defer cancelRun()
		go func() {
			// Configure booster-http to
			// - Get piece location information from the shared LID instance
			// - Get the piece data from either miner1 or miner2 (depending on the location info)
			_ = runBoosterHttp(runCtx, t.TempDir(), minerApiInfos, fullNode2ApiInfo, "ws://localhost:8042")
		}()

		t.Logf("waiting for server to come up")
		start := time.Now()
		require.Eventually(t, func() bool {
			// Wait for server to come up
			resp, err := http.Get("http://localhost:7777")
			if err == nil && resp != nil && resp.StatusCode == 200 {
				return true
			}
			msg := "Waiting for http server to come up: "
			if err != nil {
				msg += " " + err.Error()
			}
			if resp != nil {
				respBody, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				msg += " / Resp: " + resp.Status + "\n" + string(respBody)
			}
			t.Log(msg)
			return false
		}, 30*time.Second, 100*time.Millisecond)
		t.Logf("booster-http is up after %s", time.Since(start))

		for i, rootCid := range rt.RootCids {
			resp, err := http.Get("http://localhost:7777/ipfs/" + rootCid.String())
			require.NoError(t, err)
			if resp.StatusCode != 200 {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				msg := fmt.Sprintf("Failed to fetch root cid: %s\n%s", resp.Status, string(body))
				require.Fail(t, msg)
			}

			respBytes, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			outPath := path.Join(t.TempDir(), "out.dat")
			err = os.WriteFile(outPath, respBytes, 0666)
			require.NoError(t, err)

			t.Logf("retrieval is done, compare in- and out- files in: %s, out: %s", rt.SampleFilePaths[i], outPath)
			kit.AssertFilesEqual(t, rt.SampleFilePaths[i], outPath)
			t.Logf("file retrieved successfully")
		}
	})
}

func runBoosterHttp(ctx context.Context, repo string, minerApiInfo []string, fullNodeApiInfo string, lidApiInfo string) error {
	app.Setup()

	args := []string{"booster-http",
		"--repo=" + repo,
		"--vv",
		"run",
		"--api-fullnode=" + fullNodeApiInfo,
		"--api-lid=" + lidApiInfo,
		"--serve-files=true",
		"--api-version-check=false",
	}
	for _, apiInfo := range minerApiInfo {
		args = append(args, "--api-storage="+apiInfo)
	}
	return app.RunContext(ctx, args)
}
