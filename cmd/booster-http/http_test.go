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

	"github.com/filecoin-project/boostd-data/model"

	mocks_booster_http "github.com/filecoin-project/boost/cmd/booster-http/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const testFile = "test/test_file"

func TestNewHttpServer(t *testing.T) {
	// Create a new mock Http server
	ctrl := gomock.NewController(t)
	httpServer := NewHttpServer("", 7777, mocks_booster_http.NewMockHttpServerApi(ctrl))
	httpServer.Start(context.Background())

	// Check that server is responding with 200 status code
	resp, err := http.Get("http://localhost:7777/")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)

	// Stop the server
	err = httpServer.Stop()
	require.NoError(t, err)
}

func TestHttpGzipResponse(t *testing.T) {
	// Create a new mock Http server with custom functions
	ctrl := gomock.NewController(t)
	mockHttpServer := mocks_booster_http.NewMockHttpServerApi(ctrl)
	httpServer := NewHttpServer("", 7778, mockHttpServer)
	httpServer.Start(context.Background())

	// Create mock unsealed file for piece/car
	f, _ := os.Open(testFile)
	testFileBytes, err := io.ReadAll(f)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	defer f.Close()

	// Crate pieceInfo
	deal := model.DealInfo{
		ChainDealID: 1234567,
		SectorID:    0,
		PieceOffset: 1233,
		PieceLength: 123,
	}
	deals := []model.DealInfo{deal}

	mockHttpServer.EXPECT().UnsealSectorAt(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(f, nil)
	mockHttpServer.EXPECT().IsUnsealed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	mockHttpServer.EXPECT().GetPieceDeals(gomock.Any(), gomock.Any()).AnyTimes().Return(deals, nil)

	//Create a client and make request with Encoding header
	client := new(http.Client)
	request, err := http.NewRequest("GET", "http://localhost:7778/piece/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi", nil)
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

	// Create a new mock Http server
	ctrl := gomock.NewController(t)
	httpServer := NewHttpServer("", 7780, mocks_booster_http.NewMockHttpServerApi(ctrl))
	httpServer.Start(context.Background())

	response, err := http.Get("http://localhost:7780/info")
	require.NoError(t, err)
	defer response.Body.Close()

	json.NewDecoder(response.Body).Decode(&v) //nolint:errcheck
	require.Equal(t, "0.3.0", v.Version)

	// Stop the server
	err = httpServer.Stop()
	require.NoError(t, err)
}
