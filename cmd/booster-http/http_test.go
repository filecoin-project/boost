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

	"github.com/filecoin-project/boost-gfm/piecestore"
	mocks_booster_http "github.com/filecoin-project/boost/cmd/booster-http/mocks"
	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

const testFile = "test/test_file"

func TestNewHttpServer(t *testing.T) {
	// Create a new mock Http server
	ctrl := gomock.NewController(t)
	httpServer := NewHttpServer("", 7777, mocks_booster_http.NewMockHttpServerApi(ctrl), nil)
	err := httpServer.Start(context.Background())
	require.NoError(t, err)
	waitServerUp(t, 7777)

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
	httpServer := NewHttpServer("", 7777, mockHttpServer, nil)
	err := httpServer.Start(context.Background())
	require.NoError(t, err)
	waitServerUp(t, 7777)

	// Create mock unsealed file for piece/car
	f, _ := os.Open(testFile)
	testFileBytes, err := io.ReadAll(f)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	defer f.Close()

	//Create CID
	cid, err := cid.Parse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	require.NoError(t, err)

	// Crate pieceInfo
	deal := piecestore.DealInfo{
		DealID:   1234567,
		SectorID: 0,
		Offset:   1233,
		Length:   123,
	}
	var deals []piecestore.DealInfo

	pieceInfo := piecestore.PieceInfo{
		PieceCID: cid,
		Deals:    append(deals, deal),
	}

	mockHttpServer.EXPECT().UnsealSectorAt(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(f, nil)
	mockHttpServer.EXPECT().IsUnsealed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)
	mockHttpServer.EXPECT().GetPieceInfo(gomock.Any()).AnyTimes().Return(&pieceInfo, nil)

	//Create a client and make request with Encoding header
	client := new(http.Client)
	request, err := http.NewRequest("GET", "http://localhost:7777/piece/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi", nil)
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
	httpServer := NewHttpServer("", 7777, mocks_booster_http.NewMockHttpServerApi(ctrl), nil)
	err := httpServer.Start(context.Background())
	require.NoError(t, err)
	waitServerUp(t, 7777)

	response, err := http.Get("http://localhost:7777/info")
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
