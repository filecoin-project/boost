package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	mocks_booster_http "github.com/filecoin-project/boost/cmd/booster-http/mocks"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/testutil"
	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	unixfstestutil "github.com/ipfs/go-unixfsnode/testutil"
	"github.com/ipld/go-car/v2"
	unixfsgen "github.com/ipld/go-fixtureplate/generator"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	trustlesstestutil "github.com/ipld/go-trustless-utils/testutil"
	"github.com/stretchr/testify/require"
)

const (
	testFile     = "test/test_file"
	testPieceCid = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
)

func TestNewHttpServer(t *testing.T) {
	// Create a new mock Http server
	port, err := testutil.FreePort()
	require.NoError(t, err)
	ctrl := gomock.NewController(t)
	httpServer := NewHttpServer("", "0.0.0.0", port, mocks_booster_http.NewMockHttpServerApi(ctrl), nil)
	err = httpServer.Start(context.Background())
	require.NoError(t, err)
	waitServerUp(t, port)

	// Check that server is responding with 200 status code
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/", port))
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	// Create a request with Cors header
	req, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%d/", port), nil)
	require.NoError(t, err)
	req.Header.Add("Origin", "test")
	client := new(http.Client)
	response, err := client.Do(req)
	require.NoError(t, err)

	// Check for Cors header
	require.Equal(t, "*", response.Header.Get("Access-Control-Allow-Origin"))

	// Stop the server
	err = httpServer.Stop()
	require.NoError(t, err)
}

func TestHttpGzipResponse(t *testing.T) {
	// Create a new mock Http server with custom functions
	port, err := testutil.FreePort()
	require.NoError(t, err)
	ctrl := gomock.NewController(t)
	mockHttpServer := mocks_booster_http.NewMockHttpServerApi(ctrl)

	// Create mock unsealed file for piece/car
	f, _ := os.Open(testFile)
	testFileBytes, err := io.ReadAll(f)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	defer f.Close()

	// Crate pieceInfo
	deal := model.DealInfo{
		ChainDealID:  1234567,
		SectorID:     0,
		PieceOffset:  1233,
		PieceLength:  123,
		IsDirectDeal: false,
	}
	deals := []model.DealInfo{deal}

	mockHttpServer.EXPECT().UnsealSectorAt(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(f, nil)
	mockHttpServer.EXPECT().IsUnsealed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	mockHttpServer.EXPECT().GetPieceDeals(gomock.Any(), gomock.Any()).AnyTimes().Return(deals, nil)

	rndSeed := time.Now().UTC().UnixNano()
	t.Logf("random seed: %d", rndSeed)
	var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

	store := &trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{Bag: make(map[string][]byte)}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	lsys.TrustedStorage = true

	entity, err := unixfsgen.Parse("file:1MiB{zero}")
	require.NoError(t, err)
	t.Logf("Generating: %s", entity.Describe(""))
	rootEnt, err := entity.Generate(lsys, rndReader)
	require.NoError(t, err)

	// mirror of a similar set of tests in frisbii, but includes piece retrieval too
	testCases := []struct {
		name                   string
		acceptGzip             bool
		noClientCompression    bool
		serverCompressionLevel int
		expectGzip             bool
	}{
		{
			name: "default",
		},
		{
			name:                   "no client compression (no server gzip)",
			noClientCompression:    true,
			serverCompressionLevel: gzip.NoCompression,
			expectGzip:             false,
		},
		{
			name:                   "no client compression (with server gzip)",
			noClientCompression:    true,
			serverCompressionLevel: gzip.DefaultCompression,
			expectGzip:             false,
		},
		{
			name:                   "gzip (with server 1)",
			acceptGzip:             true,
			serverCompressionLevel: gzip.BestSpeed,
			expectGzip:             true,
		},
		{
			name:                   "gzip (with server 9)",
			acceptGzip:             true,
			serverCompressionLevel: gzip.BestCompression,
			expectGzip:             true,
		},
		{
			name:                   "gzip (no server gzip)",
			acceptGzip:             true,
			serverCompressionLevel: gzip.NoCompression,
			expectGzip:             false,
		},
		{
			name:                   "gzip transparent (no server gzip)",
			serverCompressionLevel: gzip.NoCompression,
			expectGzip:             false,
		},
		{
			name:                   "gzip transparent (with server gzip)",
			serverCompressionLevel: gzip.DefaultCompression,
			expectGzip:             true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			httpServer := NewHttpServer("", "0.0.0.0", port, mockHttpServer, &HttpServerOptions{
				ServePieces:      true,
				ServeTrustless:   true,
				CompressionLevel: tc.serverCompressionLevel,
				Blockstore:       testutil.NewLinkSystemBlockstore(lsys),
			})
			err = httpServer.Start(ctx)
			require.NoError(t, err)
			waitServerUp(t, port)
			defer func() {
				// Stop the server
				err = httpServer.Stop()
				require.NoError(t, err)
			}()

			{ // test /piece retrieval
				request, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%d/piece/%s", port, testPieceCid), nil)
				request.Header.Set("Accept", "application/vnd.ipld.car")
				if tc.acceptGzip {
					request.Header.Set("Accept-Encoding", "gzip")
				}
				req.NoError(err)
				request = request.WithContext(ctx)
				client := &http.Client{Transport: &http.Transport{DisableCompression: tc.noClientCompression}}
				response, err := client.Do(request)
				require.NoError(t, err)
				defer response.Body.Close()

				if response.StatusCode != http.StatusOK {
					body, _ := io.ReadAll(response.Body)
					req.Failf("wrong response code not received", "expected %d, got %d; body: [%s]", http.StatusOK, response.StatusCode, string(body))
				}

				req.Equal("Accept-Encoding", response.Header.Get("Vary"))
				req.Equal("application/piece", response.Header.Get("Content-Type"))
				req.Equal("public, max-age=29030400, immutable", response.Header.Get("Cache-Control"))

				rdr := response.Body
				if tc.expectGzip {
					if tc.noClientCompression || tc.acceptGzip { // in either of these cases we expect to handle it ourselves
						req.Equal("gzip", response.Header.Get("Content-Encoding"))
						rdr, err = gzip.NewReader(response.Body)
						req.NoError(err)
					} // else should be handled by the go client
					req.Equal(`"`+testPieceCid+`.gz"`, response.Header.Get("Etag"))
				} else {
					req.Equal("", response.Header.Get("Content-Encoding"))
					req.Equal(`"`+testPieceCid+`"`, response.Header.Get("Etag"))
				}

				// Get the uncompressed bytes
				out, err := io.ReadAll(rdr)
				require.NoError(t, err)

				// Compare bytes from original file to uncompressed http response
				require.Equal(t, testFileBytes, out)
			}

			{ // test /ipfs CAR retrieval
				request, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%d/ipfs/%s", port, rootEnt.Root.String()), nil)
				request.Header.Set("Accept", "application/vnd.ipld.car")
				if tc.acceptGzip {
					request.Header.Set("Accept-Encoding", "gzip")
				}
				req.NoError(err)
				request = request.WithContext(ctx)
				client := &http.Client{Transport: &http.Transport{DisableCompression: tc.noClientCompression}}
				response, err := client.Do(request)
				req.NoError(err)
				if response.StatusCode != http.StatusOK {
					body, _ := io.ReadAll(response.Body)
					req.Failf("wrong response code not received", "expected %d, got %d; body: [%s]", http.StatusOK, response.StatusCode, string(body))
				}

				req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y", response.Header.Get("Content-Type"))
				req.Equal("Accept, Accept-Encoding", response.Header.Get("Vary"))

				rdr := response.Body
				if tc.expectGzip {
					if tc.noClientCompression || tc.acceptGzip { // in either of these cases we expect to handle it ourselves
						req.Equal("gzip", response.Header.Get("Content-Encoding"))
						rdr, err = gzip.NewReader(response.Body)
						req.NoError(err)
					} // else should be handled by the go client
					req.Regexp(`^"`+rootEnt.Root.String()+`\.car\.\w{2,13}\.gz"$`, response.Header.Get("Etag"))
				} else {
					req.Regexp(`\.car\.\w{12,13}"$`, response.Header.Get("Etag"))
				}
				cr, err := car.NewBlockReader(rdr)
				req.NoError(err)
				req.Equal(cr.Version, uint64(1))
				req.Equal(cr.Roots, []cid.Cid{rootEnt.Root})

				wantCids := toCids(rootEnt)
				gotCids := make([]cid.Cid, 0)
				for {
					blk, err := cr.Next()
					if err != nil {
						req.ErrorIs(err, io.EOF)
						break
					}
					req.NoError(err)
					gotCids = append(gotCids, blk.Cid())
				}
				req.ElementsMatch(wantCids, gotCids)
			}
		})
	}
}

func TestHttpInfo(t *testing.T) {
	var v apiVersion

	port, err := testutil.FreePort()
	require.NoError(t, err)
	// Create a new mock Http server
	ctrl := gomock.NewController(t)
	httpServer := NewHttpServer("", "0.0.0.0", port, mocks_booster_http.NewMockHttpServerApi(ctrl), nil)
	err = httpServer.Start(context.Background())
	require.NoError(t, err)
	waitServerUp(t, port)

	response, err := http.Get(fmt.Sprintf("http://localhost:%d/info", port))
	require.NoError(t, err)
	defer response.Body.Close()

	json.NewDecoder(response.Body).Decode(&v) //nolint:errcheck
	require.Equal(t, "0.3.0", v.Version)

	// Stop the server
	err = httpServer.Stop()
	require.NoError(t, err)
}

func waitServerUp(t *testing.T, port int) {
	require.Eventually(t, func() bool {
		_, err := http.Get(fmt.Sprintf("http://localhost:%d", port))
		return err == nil
	}, time.Second, 100*time.Millisecond)
}

func toCids(e unixfstestutil.DirEntry) []cid.Cid {
	cids := make([]cid.Cid, 0)
	var r func(e unixfstestutil.DirEntry)
	r = func(e unixfstestutil.DirEntry) {
		cids = append(cids, e.SelfCids...)
		for _, c := range e.Children {
			r(c)
		}
	}
	r(e)
	return cids
}
