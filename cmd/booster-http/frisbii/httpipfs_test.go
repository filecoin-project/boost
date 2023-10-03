package frisbii_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/filecoin-project/boost/cmd/booster-http/frisbii"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
	trustlesstestutil "github.com/ipld/go-trustless-utils/testutil"
	trustlesspathing "github.com/ipld/ipld/specs/pkg-go/trustless-pathing"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestHttpIpfsHandler(t *testing.T) {
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(&trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{}})
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
			accept:             trustlesshttp.DefaultContentType().String(),
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "failed to parse CID path parameter",
		},
		{
			name:               "bad dag-scope",
			path:               "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?dag-scope=bork",
			accept:             trustlesshttp.DefaultContentType().String(),
			expectedStatusCode: http.StatusBadRequest,
			expectedBody:       "invalid dag-scope parameter",
		},
		{
			name:               "bad entity-bytes",
			path:               "/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi?entity-bytes=bork",
			accept:             trustlesshttp.DefaultContentType().String(),
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
			accept:             trustlesshttp.DefaultContentType().String(),
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

func TestHttpIpfsIntegration_Unixfs20mVariety(t *testing.T) {
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
			request.Header.Set("Accept", trustlesshttp.DefaultContentType().WithDuplicates(false).String())
			res, err := http.DefaultClient.Do(request)
			req.NoError(err)
			req.Equal(http.StatusOK, res.StatusCode)
			req.Equal(trustlesshttp.DefaultContentType().WithDuplicates(false).String(), res.Header.Get("Content-Type"))

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

func TestHttpIpfsDuplicates(t *testing.T) {
	store := &trustlesstestutil.CorrectedMemStore{ParentStore: &memstore.Store{}}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	dupyLinks, dupyLinksDeduped := mkDupy(lsys)

	handler := frisbii.NewHttpIpfs(context.Background(), lsys)
	testServer := httptest.NewServer(handler)
	defer testServer.Close()

	for _, tc := range []struct {
		name                string
		accepts             []string
		expectedContentType string
		expectedCids        []cid.Cid
	}{
		{
			name:                "default",
			accepts:             []string{trustlesshttp.DefaultContentType().String()},
			expectedContentType: trustlesshttp.DefaultContentType().String(),
			expectedCids:        dupyLinks,
		},
		{
			// note that we're pretty permissive, as long as you send an Accept
			// that vaguely signals you're willing to accept what we give
			name:                "*/*",
			accepts:             []string{"*/*"},
			expectedContentType: trustlesshttp.DefaultContentType().String(),
			expectedCids:        dupyLinks,
		},
		{
			name:                "dups",
			accepts:             []string{trustlesshttp.DefaultContentType().WithDuplicates(true).String()},
			expectedContentType: trustlesshttp.DefaultContentType().WithDuplicates(true).String(),
			expectedCids:        dupyLinks,
		},
		{
			name:                "no dups",
			accepts:             []string{trustlesshttp.DefaultContentType().WithDuplicates(false).String()},
			expectedContentType: trustlesshttp.DefaultContentType().WithDuplicates(false).String(),
			expectedCids:        dupyLinksDeduped,
		},
		{
			name: "ranked w/ dups",
			accepts: []string{
				"text/html",
				trustlesshttp.DefaultContentType().WithDuplicates(false).WithQuality(0.7).String(),
				trustlesshttp.DefaultContentType().WithDuplicates(true).WithQuality(0.8).String(),
				"*/*;q=0.2",
			},
			expectedContentType: trustlesshttp.DefaultContentType().WithDuplicates(true).String(),
			expectedCids:        dupyLinks,
		},
		{
			name: "ranked w/ no dups",
			accepts: []string{
				"text/html",
				trustlesshttp.DefaultContentType().WithDuplicates(false).WithQuality(0.8).String(),
				trustlesshttp.DefaultContentType().WithDuplicates(true).WithQuality(0.7).String(),
				"*/*;q=0.2",
			},
			expectedContentType: trustlesshttp.DefaultContentType().WithDuplicates(false).String(),
			expectedCids:        dupyLinksDeduped,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)

			request, err := http.NewRequest(http.MethodGet, testServer.URL+"/ipfs/"+dupyLinks[0].String(), nil)
			req.NoError(err)
			var accept strings.Builder
			for _, a := range tc.accepts {
				if accept.Len() > 0 {
					accept.WriteString(", ")
				}
				accept.WriteString(a)
			}
			request.Header.Set("Accept", accept.String())
			res, err := http.DefaultClient.Do(request)
			req.NoError(err)
			req.Equal(http.StatusOK, res.StatusCode)
			req.Equal(tc.expectedContentType, res.Header.Get("Content-Type"))

			carReader, err := car.NewBlockReader(res.Body)
			req.NoError(err)
			req.Equal(uint64(1), carReader.Version)
			req.Equal([]cid.Cid{dupyLinks[0]}, carReader.Roots)

			for ii, expectedCid := range tc.expectedCids {
				blk, err := carReader.Next()
				if err != nil {
					req.Equal(io.EOF, err)
					req.Len(tc.expectedCids, ii+1)
					break
				}
				req.Equal(expectedCid, blk.Cid())
			}
		})
	}
}

var pblp = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.DagProtobuf,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}

var rawlp = cidlink.LinkPrototype{
	Prefix: cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   multihash.SHA2_256,
		MhLength: 32,
	},
}

func mkBlockWithBytes(lsys linking.LinkSystem, bytes []byte) cid.Cid {
	l, err := lsys.Store(linking.LinkContext{}, rawlp, basicnode.NewBytes(bytes))
	if err != nil {
		panic(err)
	}
	return l.(cidlink.Link).Cid
}

func mkDupy(lsys linking.LinkSystem) ([]cid.Cid, []cid.Cid) {
	dupy := mkBlockWithBytes(lsys, []byte("duplicate data"))

	n, err := qp.BuildMap(dagpb.Type.PBNode, 1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "Links", qp.List(100, func(la datamodel.ListAssembler) {
			for i := 0; i < 100; i++ {
				qp.ListEntry(la, qp.Map(2, func(ma datamodel.MapAssembler) {
					qp.MapEntry(ma, "Name", qp.String(fmt.Sprintf("%03d", i)))
					qp.MapEntry(ma, "Hash", qp.Link(cidlink.Link{Cid: dupy}))
				}))
			}
		}))
	})
	if err != nil {
		panic(err)
	}
	l, err := lsys.Store(linking.LinkContext{}, pblp, n)
	if err != nil {
		panic(err)
	}

	// dupyLinks contains the duplicates
	dupyLinks := []cid.Cid{l.(cidlink.Link).Cid}
	for i := 0; i < 100; i++ {
		dupyLinks = append(dupyLinks, dupy)
	}
	// dupyLinksDeduped contains just the unique links
	dupyLinksDeduped := []cid.Cid{l.(cidlink.Link).Cid, dupy}

	return dupyLinks, dupyLinksDeduped
}
