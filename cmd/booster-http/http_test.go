package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	mocks_booster_http "github.com/filecoin-project/boost/cmd/booster-http/mocks"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const testFile = "test/test_file"

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
	httpServer := NewHttpServer("", "0.0.0.0", port, mockHttpServer, nil)
	err = httpServer.Start(context.Background())
	require.NoError(t, err)
	waitServerUp(t, port)

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

	//Create a client and make request with Encoding header
	client := new(http.Client)
	request, err := http.NewRequest("GET", fmt.Sprintf("http://localhost:%d/piece/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi", port), nil)
	require.NoError(t, err)
	request.Header.Add("Accept-Encoding", "gzip")

	response, err := client.Do(request)
	require.NoError(t, err)
	require.Equal(t, "gzip", response.Header.Get("Content-Encoding"))
	defer response.Body.Close()

	// Read reponse in gzip reader
	rawReader, err := gzip.NewReader(response.Body)
	require.NoError(t, err)

	// Get the uncompressed bytes
	out, err := io.ReadAll(rawReader)
	require.NoError(t, err)

	// Compare bytes from original file to uncompressed http response
	require.Equal(t, testFileBytes, out)

	// Stop the server
	err = httpServer.Stop()
	require.NoError(t, err)
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
