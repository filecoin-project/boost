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
	"github.com/urfave/cli/v2"
)

func TestMultiMinerHttpRetrieval(t *testing.T) {
	shared.RunMultiminerRetrievalTest(t, func(ctx context.Context, t *testing.T, rt *shared.RetrievalTest) {
		miner1ApiInfo, err := rt.BoostAndMiner1.LotusMinerApiInfo()
		require.NoError(t, err)

		miner2ApiInfo, err := rt.BoostAndMiner2.LotusMinerApiInfo()
		require.NoError(t, err)

		fullNode2ApiInfo, err := rt.BoostAndMiner2.LotusFullNodeApiInfo()
		require.NoError(t, err)

		runAndWaitForBoosterHttp(ctx, t, []string{miner1ApiInfo, miner2ApiInfo}, fullNode2ApiInfo, 7777)

		resp, err := http.Get("http://localhost:7777/ipfs/" + rt.RootCid.String())
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

		t.Logf("retrieval is done, compare in- and out- files in: %s, out: %s", rt.SampleFilePath, outPath)
		kit.AssertFilesEqual(t, rt.SampleFilePath, outPath)
		t.Logf("file retrieved successfully")
	})
}

func runAndWaitForBoosterHttp(ctx context.Context, t *testing.T, minerApiInfo []string, fullNodeApiInfo string, port int, args ...string) {
	req := require.New(t)

	args = append(args, fmt.Sprintf("--port=%d", port))

	go func() {
		_ = runBoosterHttp(ctx, t.TempDir(), minerApiInfo, fullNodeApiInfo, "ws://localhost:8042", args...)
	}()

	t.Logf("waiting for booster-http server to come up on port %d...", port)
	start := time.Now()
	req.Eventually(func() bool {
		// Wait for server to come up
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d", port))
		if err == nil && resp != nil && resp.StatusCode == 200 {
			return true
		}
		msg := "Waiting for http server to come up: "
		if err != nil {
			msg += " " + err.Error()
		}
		if resp != nil {
			respBody, err := io.ReadAll(resp.Body)
			req.NoError(err)
			msg += " / Resp: " + resp.Status + "\n" + string(respBody)
		}
		t.Log(msg)
		return false
	}, 30*time.Second, 1*time.Second)
	t.Logf("booster-http is up after %s", time.Since(start))
}

func runBoosterHttp(ctx context.Context, repo string, minerApiInfo []string, fullNodeApiInfo string, lidApiInfo string, args ...string) error {
	args = append([]string{"booster-http",
		"--repo=" + repo,
		"--vv",
		"run",
		"--api-fullnode=" + fullNodeApiInfo,
		"--api-lid=" + lidApiInfo,
		"--serve-files=true",
		"--api-version-check=false",
	}, args...)
	for _, apiInfo := range minerApiInfo {
		args = append(args, "--api-storage="+apiInfo)
	}
	// new app per call
	app := cli.NewApp()
	app.Name = "booster-http"
	app.Flags = []cli.Flag{FlagRepo}
	app.Commands = []*cli.Command{runCmd}
	return app.RunContext(ctx, args)
}
