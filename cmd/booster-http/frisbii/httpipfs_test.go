package frisbii_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/filecoin-project/boost/cmd/booster-http/frisbii"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	trustlesspathing "github.com/ipld/ipld/specs/pkg-go/trustless-pathing"
	"github.com/stretchr/testify/require"
)

func TestHttpIpfsHandler(t *testing.T) {
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(&CorrectedMemStore{Store: &memstore.Store{}})
	handler := frisbii.NewHttpIpfs(context.Background(), lsys)
	testServer := httptest.NewServer(handler)
	defer testServer.Close()

	for _, testCase := range []struct {
		name               string
		path               string
		accept             string
		expectedStatusCode int
		expectedBody       string
	}{
		{
			name:               "404",
			path:               "/not here",
			expectedStatusCode: http.StatusNotFound,
			expectedBody:       "not found",
		},
		{
			name:               "bad cid",
			path:               "/ipfs/foobarbaz",
			accept:             trustlesshttp.RequestAcceptHeader(true),
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "failed to parse CID path parameter",
		},
		{
			name:               "bad dag-scope",
			path:               "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=bork",
			accept:             trustlesshttp.RequestAcceptHeader(true),
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "invalid dag-scope parameter",
		},
		{
			name:               "bad entity-bytes",
			path:               "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?entity-bytes=bork",
			accept:             trustlesshttp.RequestAcceptHeader(true),
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "invalid entity-bytes parameter",
		},
		{
			name:               "bad Accept",
			path:               "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
			accept:             "applicaiton/json",
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "invalid Accept header; unsupported: \"applicaiton/json\"",
		},
		{
			// special case where we get to start the request because everything
			// is valid, but the block isn't in our blockstore; passing this
			// depends on deferring writing the CAR output until after we've
			// at least loaded the first block.
			name:               "block not found",
			path:               "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
			accept:             trustlesshttp.RequestAcceptHeader(true),
			expectedStatusCode: http.StatusInternalServerError,
			expectedBody:       "failed to load root node: failed to load root CID: ipld: could not find bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			req := require.New(t)
			request, err := http.NewRequest(http.MethodGet, testServer.URL+testCase.path, nil)
			req.NoError(err)
			if testCase.accept != "" {
				request.Header.Set("Accept", testCase.accept)
			}
			res, err := http.DefaultClient.Do(request)
			req.NoError(err)
			req.Equal(testCase.expectedStatusCode, res.StatusCode)
			body, err := io.ReadAll(res.Body)
			req.NoError(err)
			req.Equal(testCase.expectedBody, string(body))
		})
	}
}

func TestIntegration_Unixfs20mVariety(t *testing.T) {
	req := require.New(t)

	testCases, _, err := trustlesspathing.Unixfs20mVarietyCases()
	req.NoError(err)
	storage, closer, err := trustlesspathing.Unixfs20mVarietyReadableStorage()
	req.NoError(err)
	defer closer.Close()

	lsys := cidlink.DefaultLinkSystem()
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
	lsys.TrustedStorage = true
	lsys.SetReadStorage(storage)

	handler := frisbii.NewHttpIpfs(context.Background(), lsys)
	testServer := httptest.NewServer(handler)
	defer testServer.Close()

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			req := require.New(t)

			t.Logf("query=%s, blocks=%d", tc.AsQuery(), len(tc.ExpectedCids))

			request, err := http.NewRequest(http.MethodGet, testServer.URL+tc.AsQuery(), nil)
			req.NoError(err)
			request.Header.Set("Accept", trustlesshttp.RequestAcceptHeader(false))
			res, err := http.DefaultClient.Do(request)
			req.NoError(err)
			req.Equal(http.StatusOK, res.StatusCode)
			req.Equal(trustlesshttp.ResponseContentTypeHeader(false), res.Header.Get("Content-Type"))

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
