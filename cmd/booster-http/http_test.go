package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"testing"

	mocks_booster_http "github.com/filecoin-project/boost/cmd/booster-http/mocks"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

const testFile = "test/test_file"
const gzTestFile = "test/test_file.gz"

func TestNewHttpServer(t *testing.T) {

	// Create a new mock Http server
	ctrl := gomock.NewController(t)
	httpServer := NewHttpServer("", 7777, false, mocks_booster_http.NewMockHttpServerApi(ctrl))
	httpServer.Start(context.Background())

	// Check that server is up
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
	httpServer := NewHttpServer("", 7777, false, mockHttpServer)
	httpServer.Start(context.Background())

	// Create mock unsealed file for piece/car
	f, _ := os.Open(testFile)
	defer f.Close()

	//Create CID
	var cids []cid.Cid
	cid, err := cid.Parse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	require.NoError(t, err)
	cids = append(cids, cid)

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
	mockHttpServer.EXPECT().PiecesContainingMultihash(gomock.Any(), gomock.Any()).AnyTimes().Return(cids, nil)
	mockHttpServer.EXPECT().GetPieceInfo(gomock.Any()).AnyTimes().Return(&pieceInfo, nil)

	client := new(http.Client)
	request, err := http.NewRequest("GET", "http://localhost:7777/piece?payloadCid=bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi&format=piece", nil)
	require.NoError(t, err)
	request.Header.Add("Accept-Encoding", "gzip")

	response, err := client.Do(request)
	require.NoError(t, err)
	require.Equal(t, "gzip", response.Header.Get("Content-Encoding"))
	defer response.Body.Close()

	out, err := io.ReadAll(response.Body)
	//l := len(out)
	//fmt.Println(out[l-10 : l])
	//t.Log(len(out))

	outF, err := os.CreateTemp("test", "tf")
	require.NoError(t, err)

	//oB, err := io.ReadAll(response.Body)
	//require.NoError(t, err)

	n, err := outF.Write(out)
	require.NoError(t, err)

	t.Log(n)

	outF.Close()

	//out, err := gzip.NewReader(outF)
	//outBytes, err := io.ReadAll(out)
	//require.NoError(t, err)
	//
	//gzBytes, err := io.ReadAll(f)
	//require.NoError(t, err)
	//require.Equal(t, 0, bytes.Compare(gzBytes, outBytes))

	// Stop the server
	err = httpServer.Stop()
	require.NoError(t, err)
}
