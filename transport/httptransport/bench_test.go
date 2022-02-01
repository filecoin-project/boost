package httptransport

import (
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/filecoin-project/boost/transport/types"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipfs/go-merkledag"
	"github.com/ipld/go-car"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/stretchr/testify/require"
)

type testServer interface {
	Request(authToken string) types.HttpRequest
	Client() *httpTransport
	Server() *Libp2pCarServer
	Stop() error
}

func TestBenchTransport(t *testing.T) {
	ctx := context.Background()

	rawSize := 1024 * 1024 * 1024
	//rawSize := 2 * 1024 * 1024
	t.Logf("Benchmark file of size %d (%.2f MB)", rawSize, float64(rawSize)/(1024*1024))

	rseed := 5
	source := io.LimitReader(rand.New(rand.NewSource(int64(rseed))), int64(rawSize))
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := bstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, nil)
	dserv := merkledag.NewDAGService(bserv)

	t.Log("starting import")
	importStart := time.Now()
	nd, err := dagImport(dserv, source)
	require.NoError(t, err)
	t.Logf("import took %s", time.Since(importStart))

	// Get the size of the CAR file
	t.Log("getting car file size")
	carSizeStart := time.Now()
	cw := &countWriter{}
	err = car.WriteCar(context.Background(), dserv, []cid.Cid{nd.Cid()}, cw)
	require.NoError(t, err)
	t.Logf("car size: %d bytes (%.2f MB) - took %s", cw.total, float64(cw.total)/(1024*1024), time.Since(carSizeStart))

	performTransfer := func(t *testing.T, ts testServer) {
		srv := ts.Server()
		defer ts.Stop() //nolint:errcheck

		// Create an auth token
		carSize := cw.total
		proposalCid, err := cid.Parse("bafkqaaa")
		require.NoError(t, err)
		dbid := uint(1)
		xfer, err := srv.PrepareForDataRequest(context.Background(), dbid, proposalCid, nd.Cid(), uint64(carSize))
		require.NoError(t, err)

		// Perform retrieval with the auth token
		req := ts.Request(xfer.AuthToken)
		of := getTempFilePath(t)

		xferStart := time.Now()
		th := executeTransfer(t, ctx, ts.Client(), carSize, req, of)
		require.NotNil(t, th)

		// Wait for the transfer to complete
		clientEvts := waitForTransferComplete(th)
		end := time.Now()

		require.NotEmpty(t, clientEvts)
		lastClientEvt := clientEvts[len(clientEvts)-1]
		require.EqualValues(t, carSize, lastClientEvt.NBytesReceived)

		xferTime := end.Sub(xferStart)
		mbPerS := (float64(cw.total) / (1024 * 1024)) / (float64(xferTime) / float64(time.Second))
		t.Logf("transfer of %.2f MB took %s: %.2f MB / s", float64(cw.total)/(1024*1024), xferTime, mbPerS)
	}

	t.Run("http over libp2p", func(t *testing.T) {
		clientHost, srvHost := setupLibp2pHosts(t)
		defer srvHost.Close()
		defer clientHost.Close()

		authDB := NewAuthTokenDB(ds)
		srv := NewLibp2pCarServer(srvHost, authDB, bs, ServerConfig{
			AnnounceAddr: srvHost.Addrs()[0],
		})
		err = srv.Start()
		require.NoError(t, err)

		bsrv := &benchLibp2pHttpServer{
			srvHost:    srvHost,
			clientHost: clientHost,
			srv:        srv,
		}

		performTransfer(t, bsrv)
	})

	t.Run("raw http", func(t *testing.T) {
		clientHost, srvHost := setupLibp2pHosts(t)
		defer srvHost.Close()
		defer clientHost.Close()

		authDB := NewAuthTokenDB(ds)
		srv := NewLibp2pCarServer(srvHost, authDB, bs, ServerConfig{
			AnnounceAddr: srvHost.Addrs()[0],
		})

		http.HandleFunc("/", srv.handler)
		listenAddr := "127.0.0.1:8080"
		go func() {
			_ = http.ListenAndServe(listenAddr, nil)
		}()

		bsrv := &benchRawHttpServer{
			srvHost:    srvHost,
			clientHost: clientHost,
			srv:        srv,
			listenAddr: listenAddr,
		}

		performTransfer(t, bsrv)
	})
}

type countWriter struct {
	total int
}

func (cw *countWriter) Write(p []byte) (int, error) {
	n, err := ioutil.Discard.Write(p)
	cw.total += n
	return n, err
}

type benchRawHttpServer struct {
	srvHost    host.Host
	clientHost host.Host
	srv        *Libp2pCarServer
	listenAddr string
}

func (s *benchRawHttpServer) Request(authToken string) types.HttpRequest {
	return types.HttpRequest{
		URL: "http://" + s.listenAddr,
		Headers: map[string]string{
			"Authorization": BasicAuthHeader("", authToken),
		},
	}
}

func (s *benchRawHttpServer) Client() *httpTransport {
	return New(nil)
}

func (s *benchRawHttpServer) Server() *Libp2pCarServer {
	return s.srv
}

func (s *benchRawHttpServer) Stop() error {
	return nil
}

type benchLibp2pHttpServer struct {
	srvHost    host.Host
	clientHost host.Host
	srv        *Libp2pCarServer
}

func (s *benchLibp2pHttpServer) Request(authToken string) types.HttpRequest {
	return newLibp2pHttpRequest(s.srvHost, authToken)
}

func (s *benchLibp2pHttpServer) Client() *httpTransport {
	return New(s.clientHost)
}

func (s *benchLibp2pHttpServer) Server() *Libp2pCarServer {
	return s.srv
}

func (s *benchLibp2pHttpServer) Stop() error {
	return s.srv.Stop()
}
