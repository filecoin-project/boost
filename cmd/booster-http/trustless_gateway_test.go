package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/itests/framework"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	trustlesspathing "github.com/ipld/ipld/specs/pkg-go/trustless-pathing"
	"github.com/stretchr/testify/require"
)

func TestTrustlessGateway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := require.New(t)

	kit.QuietMiningLogs()
	framework.SetLogLevel()

	boostAndMiner := framework.NewTestFramework(ctx, t, framework.SetMaxStagingBytes(10485760))
	req.NoError(boostAndMiner.Start())
	defer boostAndMiner.Stop()

	req.NoError(boostAndMiner.AddClientProviderBalance(abi.NewTokenAmount(1e15)))

	// Get the listen address of the miner
	minerApiInfo, err := boostAndMiner.LotusMinerApiInfo()
	req.NoError(err)
	fullNodeApiInfo, err := boostAndMiner.LotusFullNodeApiInfo()
	req.NoError(err)

	testCases, rootCid, err := trustlesspathing.Unixfs20mVarietyCases()
	req.NoError(err)
	carFilepath := trustlesspathing.Unixfs20mVarietyCARPath()

	dealTestCarInParts(ctx, t, boostAndMiner, carFilepath, rootCid)

	port, err := testutil.FreePort()
	require.NoError(t, err)

	runAndWaitForBoosterHttp(ctx, t, []string{minerApiInfo}, fullNodeApiInfo, port, "--serve-pieces=false", "--serve-cars=true")

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Logf("query=%s, blocks=%d", tc.AsQuery(), len(tc.ExpectedCids))
			req := require.New(t)

			request, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d%s", port, tc.AsQuery()), nil)
			req.NoError(err)
			request.Header.Set("Accept", "application/vnd.ipld.car; version=1; order=dfs; dups=n")
			res, err := http.DefaultClient.Do(request)
			req.NoError(err)
			req.Equal(http.StatusOK, res.StatusCode)

			carReader, err := car.NewBlockReader(res.Body)
			req.NoError(err)
			req.Equal(uint64(1), carReader.Version)
			req.Equal([]cid.Cid{tc.Root}, carReader.Roots)

			for ii, expectedCid := range tc.ExpectedCids {
				blk, err := carReader.Next()
				if err != nil {
					req.Equal(io.EOF, err)
					req.Len(tc.ExpectedCids, ii+1)
					break
				}
				req.Equal(expectedCid, blk.Cid())
			}
		})
	}
}

// dealTestCarInParts creates deals for the given CAR file in 7M chunks since we
// are running with a sector size of 8M. If the CAR is <7M, it will create a
// single deal for the entire CAR; otherwise it will make new CARs that are <7M,
// with the same root in each, and make separate deals for them, waiting in
// sequence for them to complete.
func dealTestCarInParts(ctx context.Context, t *testing.T, boostAndMiner *framework.TestFramework, carFilepath string, rootCid cid.Cid) {
	req := require.New(t)

	tempDir := t.TempDir()
	// Start a web server to serve the car files
	t.Logf("starting webserver for %s", tempDir)
	server, err := testutil.HttpTestFileServer(t, tempDir)
	req.NoError(err)
	defer server.Close()

	carReader, err := os.Open(carFilepath)
	req.NoError(err)
	cr, err := car.NewBlockReader(carReader)
	req.NoError(err)

	t.Log("making <7M deals out of CAR...")
	var endOfCar bool
	for !endOfCar {
		file, err := os.CreateTemp(tempDir, "partcar.car")
		req.NoError(err)
		t.Logf("creating partcar %s", file.Name())
		sw := &sizeWriter{file, 0}
		cw, err := storage.NewWritable(sw, cr.Roots, car.WriteAsCarV1(true))
		req.NoError(err)
		for sw.size < 7<<20 {
			blk, err := cr.Next()
			if err != nil {
				req.Equal(io.EOF, err)
				endOfCar = true
				break
			}
			req.NoError(cw.Put(ctx, blk.Cid().KeyString(), blk.RawData()))
		}
		req.NoError(file.Close())

		// Create a new dummy deal
		t.Logf("creating dummy deal for %d byte file", sw.size)
		dealUuid := uuid.New()

		// Make a storage deal on the first boost, which will store the index to
		// LID and store the data on the first miner
		res, err := boostAndMiner.MakeDummyDeal(dealUuid, file.Name(), rootCid, server.URL+"/"+filepath.Base(file.Name()), true)
		req.NoError(err)
		t.Logf("created MarketDummyDeal %s", spew.Sdump(res))
		req.True(res.Result.Accepted)
		res1, err := boostAndMiner.Boost.BoostOfflineDealWithData(context.Background(), dealUuid, file.Name(), false)
		require.NoError(t, err)
		require.True(t, res1.Accepted)
		req.NoError(boostAndMiner.WaitForDealAddedToSector(dealUuid))
	}
}

type sizeWriter struct {
	io.Writer
	size int64
}

func (w *sizeWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	w.size += int64(n)
	return n, err
}
