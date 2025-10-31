package main

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	unixfsgen "github.com/ipld/go-fixtureplate/generator"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/storetheindex/test"
	"github.com/stretchr/testify/require"
)

// Test a full deal -> booster-http serve via lassie trustless and fronted with
// bifrost-gateway with curl-style trusted file fetch

func TestE2E(t *testing.T) {
	req := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := test.NewTestIpniRunner(t, ctx, t.TempDir())

	t.Log("Running in test directory:", tr.Dir)

	kit.QuietMiningLogs()
	framework.SetLogLevel()

	t.Log("Starting boost and miner")
	boostAndMiner := framework.NewTestFramework(ctx, t, framework.SetMaxStagingBytes(10485760))
	req.NoError(boostAndMiner.Start())
	defer boostAndMiner.Stop()

	req.NoError(boostAndMiner.AddClientProviderBalance(abi.NewTokenAmount(1e15)))

	// Get the listen address of the miner
	minerApiInfo, err := boostAndMiner.LotusMinerApiInfo()
	req.NoError(err)
	fullNodeApiInfo, err := boostAndMiner.LotusFullNodeApiInfo()
	req.NoError(err)

	boosterHttpPort, err := testutil.FreePort()
	require.NoError(t, err)

	t.Log("Starting booster-http")
	runAndWaitForBoosterHttp(ctx, t, []string{minerApiInfo}, fullNodeApiInfo, boosterHttpPort, "--serve-pieces=false", "--serve-cars=true")

	cwd, err := os.Getwd()
	req.NoError(err)
	err = os.Chdir(tr.Dir)
	req.NoError(err)

	rndSeed := time.Now().UTC().UnixNano()
	t.Logf("Random seed: %d", rndSeed)
	var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

	t.Log("Generate a CAR file with some content")
	carFilepath := filepath.Join(tr.Dir, "test.car")
	carFile, err := os.Create(carFilepath)
	req.NoError(err)
	store, err := storage.NewReadableWritable(carFile, nil, car.WriteAsCarV1(true))
	req.NoError(err)
	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	entity, err := unixfsgen.Parse(`dir(dir{name:"foo"}(dir{name:"bar"}(file:6MiB{name:"baz.txt"},file:1KiB{name:"boom.png"},file:1{name:"fizz.mov"})),file:1KiB{name:"qux.html"})`)
	req.NoError(err)
	t.Logf("Generating: %s", entity.Describe(""))
	rootEnt, err := entity.Generate(lsys, rndReader)
	req.NoError(err)
	req.NoError(carFile.Close())

	dealTestCarInParts(ctx, t, boostAndMiner, carFilepath, rootEnt.Root)

	bifrostGateway := filepath.Join(tr.Dir, "bifrost-gateway")
	tr.Run("go", "install", "github.com/ipfs/bifrost-gateway@latest")

	t.Log("Install lassie to perform a fetch of our content")
	lassie := filepath.Join(tr.Dir, "lassie")
	tr.Run("go", "install", "github.com/filecoin-project/lassie/cmd/lassie@latest")

	t.Log("Start bifrost-gateway")
	bifrostPort, err := testutil.FreePort()
	req.NoError(err)
	bifrostMetricsPort, err := testutil.FreePort()
	req.NoError(err)
	bifrostReady := test.NewStdoutWatcher("Path gateway listening on ")
	tr.Env = append(tr.Env,
		fmt.Sprintf("PROXY_GATEWAY_URL=http://0.0.0.0:%d", boosterHttpPort),
		"GRAPH_BACKEND=true", // enable "graph" mode, instead of blockstore mode which just fetches raw blocks
	)

	cmdBifrost := tr.Start(test.NewExecution(bifrostGateway,
		"--gateway-port", fmt.Sprintf("%d", bifrostPort),
		"--metrics-port", fmt.Sprintf("%d", bifrostMetricsPort),
	).WithWatcher(bifrostReady))

	select {
	case <-bifrostReady.Signal:
	case <-ctx.Done():
		t.Fatal("timed out waiting for bifrost-gateway to start")
	}

	// we don't have a clear stdout signal for bifrost-gateway, so we need to
	// probe for it
	t.Logf("Waiting for bifrost-gateway server to fully come up on port %d...", bifrostPort)
	start := time.Now()
	waitForHttp(ctx, t, bifrostPort, http.StatusNotFound, 20*time.Second)
	t.Logf("bifrost-gateway is up after %s", time.Since(start))

	t.Log("Perform some curl requests to bifrost-gateway")
	for _, fetch := range []struct {
		path       string
		expect     []byte
		expectType string
	}{
		{"/foo/bar/baz.txt", rootEnt.Children[0].Children[0].Children[0].Content, "text/plain; charset=utf-8"},
		{"/foo/bar/boom.png", rootEnt.Children[0].Children[0].Children[1].Content, "image/png"},
		{"/foo/bar/fizz.mov", rootEnt.Children[0].Children[0].Children[2].Content, "video/quicktime"},
		{"/qux.html", rootEnt.Children[1].Content, "text/html"},
	} {
		byts, ct, code := requireHttpResponse(t, fmt.Sprintf("http://0.0.0.0:%d/ipfs/%s%s", bifrostPort, rootEnt.Root.String(), fetch.path))
		req.Equal(http.StatusOK, code)
		req.Equal(fetch.expect, byts)
		req.Equal(fetch.expectType, ct)
	}

	t.Log("Perform some curl requests to bifrost-gateway that should fail")

	byts, ct, code := requireHttpResponse(t, fmt.Sprintf("http://0.0.0.0:%d/ipfs/%s/nope", bifrostPort, rootEnt.Root.String()))
	req.Equal(http.StatusNotFound, code)
	req.Contains(string(byts), "failed to resolve")
	req.Equal("text/plain; charset=utf-8", ct)

	byts, ct, code = requireHttpResponse(t, fmt.Sprintf("http://0.0.0.0:%d/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi", bifrostPort))
	req.Equal(http.StatusBadGateway, code)
	req.Contains(string(byts), "failed to resolve")
	req.Equal("text/plain; charset=utf-8", ct)

	t.Log("Perform a direct CAR fetch with lassie")
	tr.Run(lassie,
		"fetch",
		"--provider", fmt.Sprintf("http://0.0.0.0:%d", boosterHttpPort),
		"--output", "lassie.car",
		rootEnt.Link().String()+"/foo/bar/baz.txt",
	)

	t.Log("Verify that the Lassie CAR contains the expected CIDs")
	verifyCarContains(t, "lassie.car",
		append(append([]cid.Cid{}, rootEnt.Root, rootEnt.Children[0].Root, rootEnt.Children[0].Children[0].Root), rootEnt.Children[0].Children[0].Children[0].SelfCids...),
	)

	t.Log("Cleanup ...")

	tr.Stop(cmdBifrost, time.Second)

	err = os.Chdir(cwd)
	req.NoError(err)
}

func verifyCarContains(t *testing.T, carFilepath string, wantCids []cid.Cid) {
	req := require.New(t)

	carReader, err := os.Open(carFilepath)
	req.NoError(err)
	cr, err := car.NewBlockReader(carReader)
	req.NoError(err)

	gotCids := make([]cid.Cid, 0)
	for {
		blk, err := cr.Next()
		if err != nil {
			req.Equal(io.EOF, err)
			break
		}
		gotCids = append(gotCids, blk.Cid())
	}
	req.ElementsMatch(wantCids, gotCids)
}

// simulate a curl to the url and return the body bytes, content type and status code
func requireHttpResponse(t *testing.T, to string) ([]byte, string, int) {
	req, err := http.Get(to)
	require.NoError(t, err)
	defer func() {
		_ = req.Body.Close()
	}()
	byts, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	return byts, req.Header.Get("Content-Type"), req.StatusCode
}
