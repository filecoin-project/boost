package httptransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/net/gostream"
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

func addContentLengthHeader(w http.ResponseWriter, length int) {
	w.Header().Add("Content-Length", strconv.Itoa(length))
	w.WriteHeader(200)
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

			// verify that the output folder contains just a single output file, no temporary chunks
			dir, _ := os.Open(filepath.Dir(of))
			files, _ := dir.Readdir(0)
			require.Equal(t, 1, len(files))
			require.Equal(t, filepath.Base(of), files[0].Name())
		})
	}
}

func httpParallelTransferTest(t *testing.T, of string, rawSize int, nChunks, expectedChunks int) {
	ctx := context.Background()
	st := newServerTest(t, rawSize)
	carSize := len(st.carBytes)
	var cnt atomic.Int32

	var handler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			offset := r.Header.Get("Range")

			startend := strings.Split(strings.TrimPrefix(offset, "bytes="), "-")
			start, _ := strconv.ParseInt(startend[0], 10, 64)
			end, _ := strconv.ParseInt(startend[1], 10, 64)

			w.WriteHeader(200)
			if end == 0 {
				w.Write(st.carBytes[start:]) //nolint:errcheck
			} else {
				w.Write(st.carBytes[start:end]) //nolint:errcheck
			}
			cnt.Inc()
		case http.MethodHead:
			addContentLengthHeader(w, len(st.carBytes))
		}
	}

	svr := httptest.NewServer(handler)
	defer svr.Close()

	th := executeTransfer(t, ctx, New(nil, newDealLogger(t, ctx), NChunksOpt(nChunks)), carSize, types.HttpRequest{URL: svr.URL}, of)
	require.NotNil(t, th)

	evts := waitForTransferComplete(th)
	require.NotEmpty(t, evts)
	require.EqualValues(t, carSize, evts[len(evts)-1].NBytesReceived)
	assertFileContents(t, of, st.carBytes)
	require.Equal(t, expectedChunks, int(cnt.Load()))
}

func TestHttpTransferShouldBeDoneInOneChunk(t *testing.T) {
	httpParallelTransferTest(t, getTempFilePath(t), (100*readBufferSize)+30, 1, 1)
}

func TestHttpTransferShouldBeDoneInMultipleChunks(t *testing.T) {
	httpParallelTransferTest(t, getTempFilePath(t), (100*readBufferSize)+30, 5, 5)
}

func TestChangeNumberOfChunksForUnfinishedDownloads(t *testing.T) {
	// This test ensures that downloads complete with the same chunking setting that they have been started with.
	// For example if a download has been started with NChunks=3, then
	// it should finish in 3 chunks even if the node has been restarted in between with the setting changed.
	of := getTempFilePath(t)
	data, err := json.Marshal(transferConfig{NChunks: 3})
	require.NoError(t, err)
	err = os.WriteFile(of+"-control", data, 0644)
	require.NoError(t, err)
	// here we start download in 5 chunks while control file has 3 captured in it
	httpParallelTransferTest(t, of, (100*readBufferSize)+30, 5, 3)
}

func TestFirstUpgradeToChunking(t *testing.T) {
	// verify that users can finish their existsing downloads when they upgrade to chunking for the first time,
	// regardless of what their new chunking setting is. This test ensures that the number of chunks is set to one
	// for all previously unnfinished downloads

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

			// write some content into the output file to simulate a half-finished download
			err := os.WriteFile(of, st.carBytes[:readBufferSize/10], 0644)
			require.NoError(t, err)

			th := executeTransfer(t, ctx, New(h, newDealLogger(t, ctx)), carSize, reqFn(), of)
			require.NotNil(t, th)

			evts := waitForTransferComplete(th)
			require.NotEmpty(t, evts)
			require.EqualValues(t, carSize, evts[len(evts)-1].NBytesReceived)
			assertFileContents(t, of, st.carBytes)

			// verify that the output folder contains just a single output file, no temporary chunks
			dir, _ := os.Open(filepath.Dir(of))
			files, _ := dir.Readdir(0)
			require.Equal(t, 1, len(files))
			require.Equal(t, filepath.Base(of), files[0].Name())
		})
	}
}

func TestDealSizeIsZero(t *testing.T) {
	// if deal size from deal info is zero then the actual size should be taken from HEAD request
	ctx := context.Background()
	st := newServerTest(t, 100*readBufferSize+30)
	carSize := len(st.carBytes)

	var handler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			offset := r.Header.Get("Range")

			startend := strings.Split(strings.TrimPrefix(offset, "bytes="), "-")
			start, _ := strconv.ParseInt(startend[0], 10, 64)
			end, _ := strconv.ParseInt(startend[1], 10, 64)

			w.WriteHeader(200)
			if end == 0 {
				w.Write(st.carBytes[start:]) //nolint:errcheck
			} else {
				w.Write(st.carBytes[start:end]) //nolint:errcheck
			}
		case http.MethodHead:
			addContentLengthHeader(w, len(st.carBytes))
		}
	}

	svr := httptest.NewServer(handler)
	defer svr.Close()

	of := getTempFilePath(t)
	th := executeTransfer(t, ctx, New(nil, newDealLogger(t, ctx), NChunksOpt(numChunks)), 0, types.HttpRequest{URL: svr.URL}, of)
	require.NotNil(t, th)

	evts := waitForTransferComplete(th)
	require.NotEmpty(t, evts)
	require.EqualValues(t, carSize, evts[len(evts)-1].NBytesReceived)
	assertFileContents(t, of, st.carBytes)
}

func TestFailIfDealSizesDontMatch(t *testing.T) {
	ctx := context.Background()
	st := newServerTest(t, 100*readBufferSize+30)
	carSize := len(st.carBytes)

	var handler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			require.Fail(t, "should never happen")
		case http.MethodHead:
			addContentLengthHeader(w, carSize)
		}
	}

	svr := httptest.NewServer(handler)
	defer svr.Close()

	of := getTempFilePath(t)
	th := executeTransfer(t, ctx, New(nil, newDealLogger(t, ctx), NChunksOpt(numChunks)), carSize/2, types.HttpRequest{URL: svr.URL}, of)
	require.NotNil(t, th)

	evts := waitForTransferComplete(th)
	require.NotEmpty(t, evts)

	require.Contains(t, evts[len(evts)-1].Error.Error(), "deal size mismatch")
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
	// This test is flaky, disabling for now
	t.Skip()

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

func TestDownloadFromPrivateIPs(t *testing.T) {
	ctx := context.Background()
	of := getTempFilePath(t)
	// deal info is irrelevant in this test
	dealInfo := &types.TransportDealInfo{
		OutputFile: of,
		DealSize:   1000,
	}
	// ht := New(nil, newDealLogger(t, ctx), NChunksOpt(nChunks))
	bz, err := json.Marshal(types.HttpRequest{URL: "http://192.168.0.1/blah"})
	require.NoError(t, err)

	// do not allow download from private IP addresses by default
	_, err = New(nil, newDealLogger(t, ctx), NChunksOpt(numChunks)).Execute(ctx, bz, dealInfo)
	require.Error(t, err, "downloading from private addresses is not allowed")
	// allow download from private addresses if explicitly enabled
	_, err = New(nil, newDealLogger(t, ctx), NChunksOpt(numChunks), AllowPrivateIPsOpt(true)).Execute(ctx, bz, dealInfo)
	require.NoError(t, err)
}

func TestDontFollowHttpRedirects(t *testing.T) {
	// we should not follow more than 2 http redirects for security reasons. If the target URL tries to redirect, the client should return 303 response instead.
	// This test sets up 3 servers, with one redirecting to the other. Without the redirect check the download would have been completed successfully.
	rawSize := (100 * readBufferSize) + 30
	ctx := context.Background()
	st := newServerTest(t, rawSize)
	carSize := len(st.carBytes)

	var handler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			offset := r.Header.Get("Range")

			startend := strings.Split(strings.TrimPrefix(offset, "bytes="), "-")
			start, _ := strconv.ParseInt(startend[0], 10, 64)
			end, _ := strconv.ParseInt(startend[1], 10, 64)

			w.WriteHeader(200)
			if end == 0 {
				w.Write(st.carBytes[start:]) //nolint:errcheck
			} else {
				w.Write(st.carBytes[start:end]) //nolint:errcheck
			}
		case http.MethodHead:
			addContentLengthHeader(w, len(st.carBytes))
		}
	}
	svr := httptest.NewServer(handler)
	defer svr.Close()

	var redirectHandler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, svr.URL, http.StatusSeeOther)
	}
	redirectSvr := httptest.NewServer(redirectHandler)
	defer redirectSvr.Close()

	var redirectHandler1 http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectSvr.URL, http.StatusSeeOther)
	}
	redirectSvr1 := httptest.NewServer(redirectHandler1)
	defer redirectSvr1.Close()

	of := getTempFilePath(t)
	th := executeTransfer(t, ctx, New(nil, newDealLogger(t, ctx), NChunksOpt(numChunks)), carSize, types.HttpRequest{URL: redirectSvr1.URL}, of)
	require.NotNil(t, th)

	evts := waitForTransferComplete(th)
	require.NotEmpty(t, evts)
	require.Equal(t, 1, len(evts))
	require.EqualValues(t, 0, evts[0].NBytesReceived)
	require.Contains(t, evts[0].Error.Error(), "303 See Other")
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
			switch r.Method {
			case http.MethodGet:
				http.ServeContent(w, r, "", time.Time{}, strings.NewReader(str[:rangeEnd]))
			case http.MethodHead:
				addContentLengthHeader(w, len(str))
			}
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
			for i := 1; ; i++ {
				url, closer, h := s.init(t, i)

				th := executeTransfer(t, ctx, New(h, newDealLogger(t, ctx)), size, url, of)
				require.NotNil(t, th)

				evts := waitForTransferComplete(th)

				if len(evts) == 1 {
					require.EqualValues(t, size, evts[0].NBytesReceived)
					break
				}

				// there ar 2 scenarios why the transfer might fail
				// 1. a requested range is outside of the payload boundaries. For example if the total payload length is 10 and the requested range is 15-20, the server will
				// 	  return 416: Requested Range Not Satisfiable
				// 2. all but the last chunk have been fully downloaded. In that case appending the last chunk to the output will result into "incomlete chunk" error.
				errMsg := evts[len(evts)-1].Error.Error()
				require.True(t, strings.Contains(errMsg, "incomplete chunk") || strings.Contains(errMsg, "Requested Range Not Satisfiable"))

				s, err := os.Stat(of)
				require.NoError(t, err)
				assertFileContents(t, of, []byte(str[:s.Size()]))
				closer()
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
	svcs := serversWithCustomHandler(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			<-closing
		case http.MethodHead:
			addContentLengthHeader(w, size)
		}
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
		switch r.Method {
		case http.MethodGet:
			nAttempts.Inc()
			offset := r.Header.Get("Range")
			finalOffset := strings.TrimSuffix(strings.TrimPrefix(offset, "bytes="), "-")
			start, _ := strconv.ParseInt(finalOffset, 10, 64)

			end := int(start + (readBufferSize + 70))
			if end > size {
				end = size
			}

			w.WriteHeader(200)
			_, _ = w.Write([]byte(str[start:end]))
			// close the connection so user sees an error while reading the response
			c := GetConn(r)
			_ = c.Close()
		case http.MethodHead:
			addContentLengthHeader(w, len(str))
		}

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
		switch r.Method {
		case http.MethodGet:
			nAttempts.Inc()
			offset := r.Header.Get("Range")
			finalOffset := strings.TrimSuffix(strings.TrimPrefix(offset, "bytes="), "-")
			start, _ := strconv.ParseInt(finalOffset, 10, 64)

			end := int(start + (readBufferSize + 70))
			if end > size {
				end = size
			}

			w.WriteHeader(200)
			_, _ = w.Write([]byte(str[start:end]))
			// close the connection so user sees an error while reading the response
			c := GetConn(r)
			_ = c.Close()
		case http.MethodHead:
			addContentLengthHeader(w, len(str))
		}
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
	// use require.True to prevent long errors in logs when bytes aren't the same
	require.True(t, bytes.Equal(expected, bz))
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
		switch r.Method {
		case http.MethodGet:
			offset := r.Header.Get("Range")

			startend := strings.Split(strings.TrimPrefix(offset, "bytes="), "-")
			start, _ := strconv.ParseInt(startend[0], 10, 64)
			end, _ := strconv.ParseInt(startend[1], 10, 64)

			w.WriteHeader(200)
			if end == 0 {
				w.Write(st.carBytes[start:]) //nolint:errcheck
			} else {
				w.Write(st.carBytes[start:end]) //nolint:errcheck
			}
		case http.MethodHead:
			addContentLengthHeader(w, len(st.carBytes))
		}
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
		_ = srvHost.Close()    //nolint:errcheck
		_ = clientHost.Close() //nolint:errcheck
		_ = srv.Stop(ctx)      //nolint:errcheck
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
	_ = l.srvHost.Close()
	_ = l.clientHost.Close()
	_ = l.listener.Close()
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
		_ = server.Serve(listener)
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
