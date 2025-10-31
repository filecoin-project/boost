package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
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
	req := require.New(t)

	// Create a new mock Http server
	port, err := testutil.FreePort()
	req.NoError(err)
	ctrl := gomock.NewController(t)

	var requestCount int
	serverUpCh := make(chan struct{})
	logHandler := func(ts time.Time, remoteAddr, method string, url url.URL, status int, duration time.Duration, bytes int, compressionRatio, userAgent, msg string) {
		select {
		case <-serverUpCh:
		default:
			return
		}

		t.Logf("%s %s %s %s %d %s %d %s %s %s", ts.Format(time.RFC3339), remoteAddr, method, url.String(), status, duration, bytes, compressionRatio, userAgent, msg)
		requestCount++
		req.Equal(http.MethodGet, method)
		req.Equal("-", compressionRatio)

		switch requestCount {
		case 1, 2:
			req.Equal("/", url.Path)
			req.Equal(http.StatusOK, status)
		case 3:
			req.Equal("/piece/bafynotacid!", url.Path)
			req.Equal(http.StatusBadRequest, status)
			req.Contains(msg, "invalid cid")
		default:
			req.Failf("unexpected request count", "count: %d", requestCount)
		}
	}

	httpServer := NewHttpServer(
		"",
		"0.0.0.0",
		port,
		mocks_booster_http.NewMockHttpServerApi(ctrl),
		&HttpServerOptions{ServePieces: true, LogHandler: logHandler},
	)
	err = httpServer.Start(context.Background())
	req.NoError(err)
	waitServerUp(t, port)
	close(serverUpCh)

	// Check that server is responding with 200 status code
	resp, err := http.Get(fmt.Sprintf("http://localhost:%d/", port))
	req.NoError(err)
	req.Equal(200, resp.StatusCode)

	// Create a request with Cors header
	request, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d/", port), nil)
	req.NoError(err)
	request.Header.Add("Origin", "test")
	client := new(http.Client)
	response, err := client.Do(request)
	req.NoError(err)

	// Check for Cors header
	req.Equal("*", response.Header.Get("Access-Control-Allow-Origin"))

	// Test an error condition
	request, err = http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d/piece/bafynotacid!", port), nil)
	req.NoError(err)
	response, err = client.Do(request)
	req.NoError(err)
	if response.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(response.Body)
		req.Failf("wrong response code not received", "expected %d, got %d; body: [%s]", http.StatusOK, response.StatusCode, string(body))
	}

	// Stop the server
	err = httpServer.Stop()
	req.NoError(err)
}

func TestHttpGzipResponse(t *testing.T) {
	req := require.New(t)

	// Create a new mock Http server with custom functions
	port, err := testutil.FreePort()
	req.NoError(err)
	ctrl := gomock.NewController(t)
	mockHttpServer := mocks_booster_http.NewMockHttpServerApi(ctrl)

	// Create mock unsealed file for piece/car
	f, _ := os.Open(testFile)
	testFileBytes, err := io.ReadAll(f)
	req.NoError(err)
	_, err = f.Seek(0, io.SeekStart)
	req.NoError(err)
	defer func() {
		_ = f.Close()
	}()

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
	req.NoError(err)
	t.Logf("Generating: %s", entity.Describe(""))
	rootEnt, err := entity.Generate(lsys, rndReader)
	req.NoError(err)

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

			logHandler := func(ts time.Time, remoteAddr, method string, url url.URL, status int, duration time.Duration, bytes int, compressionRatio, userAgent, msg string) {
				t.Logf("%s %s %s %s %d %s %d %s %s %s", ts.Format(time.RFC3339), remoteAddr, method, url.String(), status, duration, bytes, compressionRatio, userAgent, msg)
				req.Condition(func() (success bool) {
					if method == http.MethodGet || method == http.MethodHead {
						return true
					}
					return false
				})
				if url.Path == "/" { // waitServerUp
					return
				}
				if strings.HasPrefix(url.Path, "/piece") {
					req.Equal("/piece/"+testPieceCid, url.Path)
				} else if strings.HasPrefix(url.Path, "/ipfs") {
					req.Condition(func() (success bool) {
						if url.Path == "/ipfs/"+rootEnt.Root.String() || url.Path == "/ipfs/bafkqaaa" || url.Path == "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi" {
							return true
						}
						return false
					})
				} else {
					req.Failf("unexpected url path", "path: %s", url.Path)
				}
				//req.Equal(http.StatusOK, status)
				if method == http.MethodGet {
					if tc.expectGzip && url.Path != "/ipfs/bafkqaaa" {
						req.NotEqual("-", compressionRatio, "compression ratio should be set for %s", url.Path)
						// convert compressionRatio string to a float64
						compressionRatio, err := strconv.ParseFloat(compressionRatio, 64)
						req.NoError(err)
						req.True(compressionRatio > 10, "compression ratio (%s) should be > 10 for %s", compressionRatio, url.Path) // it's all zeros
					}
				}
			}

			httpServer := NewHttpServer("", "0.0.0.0", port, mockHttpServer, &HttpServerOptions{
				ServePieces:      true,
				ServeTrustless:   true,
				CompressionLevel: tc.serverCompressionLevel,
				Blockstore:       testutil.NewLinkSystemBlockstore(lsys),
				LogWriter:        os.Stderr,
				LogHandler:       logHandler,
			})
			err = httpServer.Start(ctx)
			req.NoError(err)
			waitServerUp(t, port)
			defer func() {
				// Stop the server
				err = httpServer.Stop()
				req.NoError(err)
			}()

			{ // test /piece retrieval
				request, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d/piece/%s", port, testPieceCid), nil)
				request.Header.Set("Accept", "application/vnd.ipld.car")
				if tc.acceptGzip {
					request.Header.Set("Accept-Encoding", "gzip")
				}
				req.NoError(err)
				request = request.WithContext(ctx)
				client := &http.Client{Transport: &http.Transport{DisableCompression: tc.noClientCompression}}
				response, err := client.Do(request)
				req.NoError(err)
				defer func() {
					_ = response.Body.Close()
				}()

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
				req.NoError(err)

				// Compare bytes from original file to uncompressed http response
				req.Equal(testFileBytes, out)
			}

			{ // test /ipfs CAR retrieval
				request, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d/ipfs/%s", port, rootEnt.Root.String()), nil)
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

			{ // test HEAD request for existing content
				request, err := http.NewRequest(http.MethodHead, fmt.Sprintf("http://localhost:%d/ipfs/%s", port, rootEnt.Root.String()), nil)
				request.Header.Set("Accept", "application/vnd.ipld.car")
				req.NoError(err)
				request = request.WithContext(ctx)
				client := &http.Client{}
				response, err := client.Do(request)
				req.NoError(err)
				req.Equal(http.StatusOK, response.StatusCode)

				// HEAD should have headers but no body
				req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y", response.Header.Get("Content-Type"))
				req.NotEmpty(response.Header.Get("Etag"))
				req.Equal("Accept, Accept-Encoding", response.Header.Get("Vary"))

				body, err := io.ReadAll(response.Body)
				req.NoError(err)
				req.Empty(body, "HEAD request should not return a body")
			}

			{ // test HEAD request with raw format
				request, err := http.NewRequest(http.MethodHead, fmt.Sprintf("http://localhost:%d/ipfs/%s", port, rootEnt.Root.String()), nil)
				request.Header.Set("Accept", "application/vnd.ipld.raw")
				req.NoError(err)
				request = request.WithContext(ctx)
				client := &http.Client{}
				response, err := client.Do(request)
				req.NoError(err)
				req.Equal(http.StatusOK, response.StatusCode)

				// HEAD should have headers but no body
				req.Equal("application/vnd.ipld.raw", response.Header.Get("Content-Type"))
				req.NotEmpty(response.Header.Get("Etag"))

				body, err := io.ReadAll(response.Body)
				req.NoError(err)
				req.Empty(body, "HEAD request should not return a body")
			}

			{ // test probe path with GET
				request, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d/ipfs/bafkqaaa", port), nil)
				request.Header.Set("Accept", "application/vnd.ipld.raw")
				req.NoError(err)
				request = request.WithContext(ctx)
				client := &http.Client{}
				response, err := client.Do(request)
				req.NoError(err)
				req.Equal(http.StatusOK, response.StatusCode)

				// Probe CID (identity CID) should return empty content for raw format
				body, err := io.ReadAll(response.Body)
				req.NoError(err)
				req.Empty(body, "Probe CID should return empty content for raw format")
			}

			{ // test probe path with HEAD
				request, err := http.NewRequest(http.MethodHead, fmt.Sprintf("http://localhost:%d/ipfs/bafkqaaa", port), nil)
				request.Header.Set("Accept", "application/vnd.ipld.raw")
				req.NoError(err)
				request = request.WithContext(ctx)
				client := &http.Client{}
				response, err := client.Do(request)
				req.NoError(err)
				req.Equal(http.StatusOK, response.StatusCode)

				// HEAD should have headers but no body
				req.NotEmpty(response.Header.Get("Etag"))
				body, err := io.ReadAll(response.Body)
				req.NoError(err)
				req.Empty(body, "HEAD request should not return a body")
			}

			{ // test probe path with CAR format
				request, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:%d/ipfs/bafkqaaa", port), nil)
				request.Header.Set("Accept", "application/vnd.ipld.car")
				req.NoError(err)
				request = request.WithContext(ctx)
				client := &http.Client{}
				response, err := client.Do(request)
				req.NoError(err)
				req.Equal(http.StatusOK, response.StatusCode)

				// Probe CID should return a minimal CAR
				req.Equal("application/vnd.ipld.car;version=1;order=dfs;dups=y", response.Header.Get("Content-Type"))

				// Parse the CAR to verify it's valid
				cr, err := car.NewBlockReader(response.Body)
				req.NoError(err)
				req.Equal(uint64(1), cr.Version)
				req.Equal(1, len(cr.Roots))
				req.Equal("bafkqaaa", cr.Roots[0].String())

				// The CAR should be minimal (no blocks for identity CID)
				_, err = cr.Next()
				req.ErrorIs(err, io.EOF)
			}

			{ // test HEAD for non-existing content
				request, err := http.NewRequest(http.MethodHead, fmt.Sprintf("http://localhost:%d/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi", port), nil)
				request.Header.Set("Accept", "application/vnd.ipld.raw")
				req.NoError(err)
				request = request.WithContext(ctx)
				client := &http.Client{}
				response, err := client.Do(request)
				req.NoError(err)
				req.Equal(http.StatusInternalServerError, response.StatusCode)

				body, err := io.ReadAll(response.Body)
				req.NoError(err)
				req.Empty(body, "HEAD request should not return a body even for errors")
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
	defer func() {
		_ = response.Body.Close()
	}()

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
