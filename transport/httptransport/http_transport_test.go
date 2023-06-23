package httptransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemarket/logs"

	"github.com/filecoin-project/boost/transport"
	"github.com/filecoin-project/boost/transport/types"
	"github.com/google/uuid"
	"github.com/ipfs/boxo/blockservice"
	bstore "github.com/ipfs/boxo/blockstore"
	chunk "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-cidutil"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car"
	"github.com/libp2p/go-libp2p"
	gostream "github.com/libp2p/go-libp2p-gostream"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

type serverTest struct {
	t        *testing.T
	data     []byte
	ds       datastore.Batching
	bs       bstore.Blockstore
	root     format.Node
	carBytes []byte
}

func newDealLogger(t *testing.T, ctx context.Context) *logs.DealLogger {
	tmp := db.CreateTestTmpDB(t)
	require.NoError(t, db.CreateAllBoostTables(ctx, tmp, tmp))
	return logs.NewDealLogger(db.NewLogsDB(tmp))
}

func newServerTest(t *testing.T, size int) *serverTest {
	data := []byte(randSeq(size))
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := bstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, nil)
	dserv := merkledag.NewDAGService(bserv)
	source := bytes.NewBuffer(data)

	nd, err := dagImport(dserv, source)
	require.NoError(t, err)

	var buff bytes.Buffer
	err = car.WriteCar(context.Background(), dserv, []cid.Cid{nd.Cid()}, &buff)
	require.NoError(t, err)

	return &serverTest{
		t:        t,
		data:     data,
		ds:       ds,
		bs:       bs,
		root:     nd,
		carBytes: buff.Bytes(),
	}
}

func TestSimpleTransfer(t *testing.T) {
	ctx := context.Background()
	rawSize := (100 * readBufferSize) + 30
	st := newServerTest(t, rawSize)
	carSize := len(st.carBytes)
	svcs := serversWithRangeHandler(st)

	for name, init := range svcs {
		t.Run(name, func(t *testing.T) {
			reqFn, closer, h := init(t)
			defer closer()
			of := getTempFilePath(t)
			th := executeTransfer(t, ctx, New(h, newDealLogger(t, ctx)), carSize, reqFn(), of)
			require.NotNil(t, th)

			evts := waitForTransferComplete(th)
			require.NotEmpty(t, evts)
			require.EqualValues(t, carSize, evts[len(evts)-1].NBytesReceived)
			assertFileContents(t, of, st.carBytes)
		})
	}
}

func TestTransportRespectsContext(t *testing.T) {
	t.Skip("hangs on the CI")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	svcs := serversWithCustomHandler(func(http.ResponseWriter, *http.Request) {
		time.Sleep(1 * time.Hour)
	})

	for name, init := range svcs {
		t.Run(name, func(t *testing.T) {
			reqFn, _, h := init(t)

			of := getTempFilePath(t)
			th := executeTransfer(t, ctx, New(h, newDealLogger(t, ctx)), 100, reqFn(), of)
			require.NotNil(t, th)

			evts := waitForTransferComplete(th)
			require.NotEmpty(t, evts)
			require.Len(t, evts, 1)
			require.Contains(t, evts[0].Error.Error(), "context")
		})
	}
}

func TestConcurrentTransfers(t *testing.T) {
	ctx := context.Background()

	// start server with data to send
	rawSize := (100 * readBufferSize) + 30
	st := newServerTest(t, rawSize)
	size := len(st.carBytes)
	svcs := serversWithRangeHandler(st)

	delete(svcs, "http")
	delete(svcs, "libp2p-http-test")

	for name, s := range svcs {
		t.Run(name, func(t *testing.T) {
			reqFn, closer, h := s(t)
			defer closer()

			var errG errgroup.Group
			for i := 0; i < 10; i++ {
				errG.Go(func() error {
					of := getTempFilePath(t)
					th := executeTransfer(t, ctx, New(h, newDealLogger(t, ctx)), size, reqFn(), of)

					evts := waitForTransferComplete(th)
					if len(evts) == 0 {
						return errors.New("events should NOT be empty")
					}

					if size != int(evts[len(evts)-1].NBytesReceived) {
						return errors.New("size mismatch")
					}

					f, err := os.Open(of)
					if err != nil {
						return err
					}
					bz, err := io.ReadAll(f)
					if err != nil {
						return err
					}

					if !bytes.Equal(st.carBytes, bz) {
						return errors.New("content mismatch")
					}
					return nil
				})
			}

			require.NoError(t, errG.Wait())
		})
	}
}

func TestCompletionOnMultipleAttemptsWithSameFile(t *testing.T) {
	ctx := context.Background()
	of := getTempFilePath(t)

	// start server with data to send
	size := (100 * readBufferSize) + 30
	str := randSeq(size)

	handler := func(i int) func(w http.ResponseWriter, r *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) {
			rangeEnd := readBufferSize * i
			if rangeEnd > size {
				rangeEnd = size
			}
			http.ServeContent(w, r, "", time.Time{}, strings.NewReader(str[:rangeEnd]))
		}
	}

	svcs := map[string]struct {
		init func(t *testing.T, i int) (req types.HttpRequest, close func(), h host.Host)
	}{
		"http": {
			init: func(t *testing.T, i int) (types.HttpRequest, func(), host.Host) {
				svr := httptest.NewServer(http.HandlerFunc(handler((i))))
				return types.HttpRequest{URL: svr.URL}, svr.Close, nil
			},
		},
		"libp2p-http": {
			init: func(t *testing.T, i int) (types.HttpRequest, func(), host.Host) {
				svr := newTestLibp2pHttpServer(t, handler(i))
				return svr.Req, svr.Close, svr.clientHost
			},
		},
	}

	for name, s := range svcs {
		t.Run(name, func(t *testing.T) {
			end := readBufferSize
			for i := 1; ; i++ {
				url, closer, h := s.init(t, i)

				th := executeTransfer(t, ctx, New(h, newDealLogger(t, ctx)), size, url, of)
				require.NotNil(t, th)

				evts := waitForTransferComplete(th)

				if len(evts) == 1 {
					require.EqualValues(t, size, evts[0].NBytesReceived)
					break
				}
				require.Contains(t, evts[len(evts)-1].Error.Error(), "mismatch")

				assertFileContents(t, of, []byte(str[:end]))
				closer()
				end = end + readBufferSize
				if end > size {
					end = size
				}
			}

			// ensure file contents are correct in the end
			assertFileContents(t, of, []byte(str))
		})
	}
}

func TestTransferCancellation(t *testing.T) {
	ctx := context.Background()
	// start server with data to send
	size := (100 * readBufferSize) + 30

	closing := make(chan struct{}, 2)
	svcs := serversWithCustomHandler(func(http.ResponseWriter, *http.Request) {
		fmt.Println("Hello")
		<-closing
	})

	for name, s := range svcs {
		t.Run(name, func(t *testing.T) {
			reqFn, closer, h := s(t)
			defer closer()
			of := getTempFilePath(t)
			th := executeTransfer(t, ctx, New(h, newDealLogger(t, ctx)), size, reqFn(), of)
			require.NotNil(t, th)
			// close the transfer so context is cancelled
			th.Close()

			evts := waitForTransferComplete(th)
			require.Len(t, evts, 1)
			require.True(t, errors.Is(evts[0].Error, context.Canceled))
			closing <- struct{}{}
		})
	}
}

type contextKey struct {
	key string
}

var ConnContextKey = &contextKey{"http-conn"}

func SaveConnInContext(ctx context.Context, c net.Conn) context.Context {
	return context.WithValue(ctx, ConnContextKey, c)
}
func GetConn(r *http.Request) net.Conn {
	return r.Context().Value(ConnContextKey).(net.Conn)
}

func TestTransferResumption(t *testing.T) {
	ctx := context.Background()
	// start server with data to send
	size := (100 * readBufferSize) + 30
	str := strings.Repeat("a", size)

	var nAttempts atomic.Int32

	// start http server that always sends 500Kb and disconnects (total file size is greater than 100 Mb)
	svr := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nAttempts.Inc()
		offset := r.Header.Get("Range")
		finalOffset := strings.TrimSuffix(strings.TrimPrefix(offset, "bytes="), "-")
		start, _ := strconv.ParseInt(finalOffset, 10, 64)

		end := int(start + (readBufferSize + 70))
		if end > size {
			end = size
		}

		w.WriteHeader(200)
		w.Write([]byte(str[start:end])) //nolint:errcheck
		// close the connection so user sees an error while reading the response
		c := GetConn(r)
		c.Close() //nolint:errcheck
	}))
	svr.Config.ConnContext = SaveConnInContext
	svr.Start()
	defer svr.Close()

	ht := New(nil, newDealLogger(t, ctx), BackOffRetryOpt(50*time.Millisecond, 100*time.Millisecond, 2, 1000))
	of := getTempFilePath(t)
	th := executeTransfer(t, ctx, ht, size, types.HttpRequest{URL: svr.URL}, of)
	require.NotNil(t, th)

	evts := waitForTransferComplete(th)
	require.NotEmpty(t, evts)
	require.EqualValues(t, size, evts[len(evts)-1].NBytesReceived)

	assertFileContents(t, of, []byte(str))

	// assert we had to make multiple connections to the server
	require.True(t, nAttempts.Load() > 10)
}

func TestLibp2pTransferResumption(t *testing.T) {
	ctx := context.Background()
	// start server with data to send
	size := (100 * readBufferSize) + 30
	str := strings.Repeat("a", size)

	var nAttempts atomic.Int32

	// start http server that always sends 500Kb and disconnects (total file size is greater than 100 Mb)
	svr := newTestLibp2pHttpServer(t, func(w http.ResponseWriter, r *http.Request) {
		nAttempts.Inc()
		offset := r.Header.Get("Range")
		finalOffset := strings.TrimSuffix(strings.TrimPrefix(offset, "bytes="), "-")
		start, _ := strconv.ParseInt(finalOffset, 10, 64)

		end := int(start + (readBufferSize + 70))
		if end > size {
			end = size
		}

		w.WriteHeader(200)
		w.Write([]byte(str[start:end])) //nolint:errcheck
		// close the connection so user sees an error while reading the response
		c := GetConn(r)
		c.Close() //nolint:errcheck
	})

	defer svr.Close()

	ht := New(svr.clientHost, newDealLogger(t, ctx), BackOffRetryOpt(50*time.Millisecond, 100*time.Millisecond, 2, 1000))
	of := getTempFilePath(t)
	th := executeTransfer(t, context.Background(), ht, size, svr.Req, of)
	require.NotNil(t, th)

	evts := waitForTransferComplete(th)
	require.NotEmpty(t, evts)
	require.EqualValues(t, size, evts[len(evts)-1].NBytesReceived)

	assertFileContents(t, of, []byte(str))

	// assert we had to make multiple connections to the server
	require.True(t, nAttempts.Load() > 10)
}

func executeTransfer(t *testing.T, ctx context.Context, ht *httpTransport, size int, req types.HttpRequest, tmpFile string) transport.Handler {
	dealInfo := &types.TransportDealInfo{
		OutputFile: tmpFile,
		DealSize:   int64(size),
	}
	bz, err := json.Marshal(req)
	require.NoError(t, err)

	th, err := ht.Execute(ctx, bz, dealInfo)
	require.NoError(t, err)
	require.NotNil(t, th)

	return th
}

func assertFileContents(t *testing.T, file string, expected []byte) {
	bz, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, len(expected), len(bz))
	require.Equal(t, expected, bz)
}

func getTempFilePath(t *testing.T) string {
	dir := t.TempDir()
	of, err := os.CreateTemp(dir, "")
	require.NoError(t, err)
	require.NoError(t, of.Close())
	return of.Name()
}

func waitForTransferComplete(th transport.Handler) []types.TransportEvent {
	var evts []types.TransportEvent

	for evt := range th.Sub() {
		evts = append(evts, evt)
	}

	return evts
}

func serversWithRangeHandler(st *serverTest) map[string]func(t *testing.T) (req func() types.HttpRequest, close func(), h host.Host) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		offset := r.Header.Get("Range")
		finalOffset := strings.TrimSuffix(strings.TrimPrefix(offset, "bytes="), "-")
		start, _ := strconv.ParseInt(finalOffset, 10, 64)
		w.WriteHeader(200)
		w.Write(st.carBytes[start:]) //nolint:errcheck
	}

	svcs := serversWithCustomHandler(handler)
	svcs["libp2p-http"] = func(t *testing.T) (func() types.HttpRequest, func(), host.Host) {
		return newLibp2pHttpServer(st)
	}
	return svcs
}

func serversWithCustomHandler(handler http.HandlerFunc) map[string]func(t *testing.T) (req func() types.HttpRequest, close func(), h host.Host) {
	svcs := map[string]func(t *testing.T) (req func() types.HttpRequest, close func(), h host.Host){
		"http": func(t *testing.T) (func() types.HttpRequest, func(), host.Host) {
			svr := httptest.NewServer(handler)
			reqFn := func() types.HttpRequest {
				return types.HttpRequest{URL: svr.URL}
			}
			return reqFn, svr.Close, nil
		},
		"libp2p-http-test": func(t *testing.T) (func() types.HttpRequest, func(), host.Host) {
			svr := newTestLibp2pHttpServer(t, handler)
			reqFn := func() types.HttpRequest {
				return svr.Req
			}
			return reqFn, svr.Close, svr.clientHost
		},
	}

	return svcs
}

func newLibp2pHttpServer(st *serverTest) (func() types.HttpRequest, func(), host.Host) {
	ctx := context.Background()
	clientHost, srvHost := setupLibp2pHosts(st.t)
	authDB := NewAuthTokenDB(st.ds)
	srv := NewLibp2pCarServer(srvHost, authDB, st.bs, ServerConfig{})
	err := srv.Start(ctx)
	require.NoError(st.t, err)

	proposalCid, err := cid.Parse("bafkqaaa")
	require.NoError(st.t, err)

	req := func() types.HttpRequest {
		id := uuid.New().String()
		authToken, err := GenerateAuthToken()
		require.NoError(st.t, err)
		err = authDB.Put(ctx, authToken, AuthValue{
			ID:          id,
			ProposalCid: proposalCid,
			PayloadCid:  st.root.Cid(),
			Size:        uint64(len(st.carBytes)),
		})
		require.NoError(st.t, err)
		return newLibp2pHttpRequest(srvHost, authToken)
	}

	closeServer := func() {
		srvHost.Close()    //nolint:errcheck
		clientHost.Close() //nolint:errcheck
		srv.Stop(ctx)      //nolint:errcheck
	}
	return req, closeServer, clientHost
}

var defaultHashFunction = uint64(multihash.SHA2_256)

func dagImport(dserv format.DAGService, fi io.Reader) (format.Node, error) {
	prefix, err := merkledag.PrefixForCidVersion(1)
	if err != nil {
		return nil, err
	}
	prefix.MhType = defaultHashFunction

	spl := chunk.NewSizeSplitter(fi, 1024*1024)
	dbp := helpers.DagBuilderParams{
		Maxlinks:  1024,
		RawLeaves: true,

		// NOTE: InlineBuilder not recommended, we are using this to test identity CIDs
		CidBuilder: cidutil.InlineBuilder{
			Builder: prefix,
			Limit:   32,
		},

		Dagserv: dserv,
	}

	db, err := dbp.New(spl)
	if err != nil {
		return nil, err
	}

	return balanced.Layout(db)
}

type lip2pHttpServer struct {
	srvHost    host.Host
	clientHost host.Host
	listener   net.Listener
	Req        types.HttpRequest
}

func (l *lip2pHttpServer) Close() {
	l.srvHost.Close()
	l.clientHost.Close()
	l.listener.Close()
}

func newTestLibp2pHttpServer(t *testing.T, handler func(http.ResponseWriter, *http.Request)) *lip2pHttpServer {
	l := &lip2pHttpServer{}

	m1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	m2, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	l.srvHost = newHost(t, m1)
	l.clientHost = newHost(t, m2)

	l.srvHost.Peerstore().AddAddrs(l.clientHost.ID(), l.clientHost.Addrs(), peerstore.PermanentAddrTTL)
	l.clientHost.Peerstore().AddAddrs(l.srvHost.ID(), l.srvHost.Addrs(), peerstore.PermanentAddrTTL)

	listener, err := gostream.Listen(l.srvHost, types.DataTransferProtocol)
	require.NoError(t, err)
	l.listener = listener

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/", handler)
		server := &http.Server{Handler: mux}
		server.ConnContext = SaveConnInContext
		server.Serve(listener) //nolint:errcheck
	}()

	l.Req = newLibp2pHttpRequest(l.srvHost, "")
	return l
}

func newHost(t *testing.T, listen multiaddr.Multiaddr) host.Host {
	h, err := libp2p.New(libp2p.ListenAddrs(listen))
	require.NoError(t, err)
	return h
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
