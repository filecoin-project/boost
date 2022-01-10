package httptransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	p2phttp "github.com/libp2p/go-libp2p-http"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peerstore"
	gostream "github.com/libp2p/go-libp2p-gostream"
	"github.com/multiformats/go-multiaddr"

	"github.com/libp2p/go-libp2p-core/host"
	"go.uber.org/atomic"

	"golang.org/x/xerrors"

	"golang.org/x/sync/errgroup"

	"github.com/filecoin-project/boost/transport"

	"github.com/filecoin-project/boost/transport/types"
	"github.com/stretchr/testify/require"
)

func TestSimpleTransfer(t *testing.T) {
	ctx := context.Background()
	size := (100 * readBufferSize) + 30
	str := randSeq(size)
	svcs := serversWithRangeHandler(t, str)

	for name, init := range svcs {
		t.Run(name, func(t *testing.T) {
			url, closer, h := init(t)
			defer closer()
			of := getTempFilePath(t)
			th := executeTransfer(t, ctx, New(h), size, url, of)
			require.NotNil(t, th)

			evts := waitForTransferComplete(th)
			require.NotEmpty(t, evts)
			require.EqualValues(t, size, evts[len(evts)-1].NBytesReceived)
			assertFileContents(t, of, str)
		})
	}
}

func TestTransportRespectsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	closing := make(chan struct{}, 2)

	svcs := serversWithCustomHandler(t, func(http.ResponseWriter, *http.Request) {
		<-closing
		return
	})

	for name, init := range svcs {
		t.Run(name, func(t *testing.T) {
			url, close, h := init(t)
			defer close()

			of := getTempFilePath(t)
			th := executeTransfer(t, ctx, New(h), 100, url, of)
			require.NotNil(t, th)

			evts := waitForTransferComplete(th)
			require.NotEmpty(t, evts)
			require.Len(t, evts, 1)
			require.Contains(t, evts[0].Error.Error(), "context")

			closing <- struct{}{}
		})
	}
}

func TestConcurrentTransfers(t *testing.T) {
	ctx := context.Background()

	// start server with data to send
	size := (100 * readBufferSize) + 30
	str := randSeq(size)
	svcs := serversWithRangeHandler(t, str)

	for name, s := range svcs {
		t.Run(name, func(t *testing.T) {
			url, closer, h := s(t)
			defer closer()

			var errG errgroup.Group
			for i := 0; i < 10; i++ {
				errG.Go(func() error {
					of := getTempFilePath(t)
					th := executeTransfer(t, ctx, New(h), size, url, of)

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
					bz, err := ioutil.ReadAll(f)
					if err != nil {
						return err
					}

					if str != string(bz) {
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
		init func(t *testing.T, i int) (url string, close func(), h host.Host)
	}{
		"http": {
			init: func(t *testing.T, i int) (string, func(), host.Host) {
				svr := httptest.NewServer(http.HandlerFunc(handler((i))))
				return svr.URL, svr.Close, nil
			},
		},
		"libp2p-http": {
			init: func(t *testing.T, i int) (string, func(), host.Host) {
				svr := newLibp2pHttpServer(t, handler(i))
				return svr.URL, svr.Close, svr.clientHost
			},
		},
	}

	for name, s := range svcs {
		t.Run(name, func(t *testing.T) {
			end := readBufferSize
			for i := 1; ; i++ {
				url, closer, h := s.init(t, i)

				th := executeTransfer(t, ctx, New(h), size, url, of)
				require.NotNil(t, th)

				evts := waitForTransferComplete(th)

				if len(evts) == 1 {
					require.EqualValues(t, size, evts[0].NBytesReceived)
					break
				}
				require.Contains(t, evts[len(evts)-1].Error.Error(), "mismatch")

				assertFileContents(t, of, str[:end])
				closer()
				end = end + readBufferSize
				if end > size {
					end = size
				}
			}

			// ensure file contents are correct in the end
			assertFileContents(t, of, str)
		})
	}
}

func TestTransferCancellation(t *testing.T) {
	// start server with data to send
	size := (100 * readBufferSize) + 30

	closing := make(chan struct{}, 2)
	svcs := serversWithCustomHandler(t, func(http.ResponseWriter, *http.Request) {
		fmt.Println("Hello")
		<-closing
	})

	for name, s := range svcs {
		t.Run(name, func(t *testing.T) {
			url, closer, h := s(t)
			defer closer()
			of := getTempFilePath(t)
			th := executeTransfer(t, context.Background(), New(h), size, url, of)
			require.NotNil(t, th)
			// close the transfer so context is cancelled
			th.Close()

			evts := waitForTransferComplete(th)
			require.Len(t, evts, 1)
			require.True(t, xerrors.Is(evts[0].Error, context.Canceled))
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

	ht := New(nil, BackOffRetryOpt(50*time.Millisecond, 100*time.Millisecond, 2, 1000))
	of := getTempFilePath(t)
	th := executeTransfer(t, context.Background(), ht, size, svr.URL, of)
	require.NotNil(t, th)

	evts := waitForTransferComplete(th)
	require.NotEmpty(t, evts)
	require.EqualValues(t, size, evts[len(evts)-1].NBytesReceived)

	assertFileContents(t, of, str)

	// assert we had to make multiple connections to the server
	require.True(t, nAttempts.Load() > 10)
}

func TestLibp2pTransferResumption(t *testing.T) {
	// start server with data to send
	size := (100 * readBufferSize) + 30
	str := strings.Repeat("a", size)

	var nAttempts atomic.Int32

	// start http server that always sends 500Kb and disconnects (total file size is greater than 100 Mb)
	svr := newLibp2pHttpServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	defer svr.Close()

	ht := New(svr.clientHost, BackOffRetryOpt(50*time.Millisecond, 100*time.Millisecond, 2, 1000))
	of := getTempFilePath(t)
	th := executeTransfer(t, context.Background(), ht, size, svr.URL, of)
	require.NotNil(t, th)

	evts := waitForTransferComplete(th)
	require.NotEmpty(t, evts)
	require.EqualValues(t, size, evts[len(evts)-1].NBytesReceived)

	assertFileContents(t, of, str)

	// assert we had to make multiple connections to the server
	require.True(t, nAttempts.Load() > 10)
}

func executeTransfer(t *testing.T, ctx context.Context, ht *httpTransport, size int, url string, tmpFile string) transport.Handler {
	dealInfo := &types.TransportDealInfo{
		OutputFile: tmpFile,
		DealSize:   int64(size),
	}
	bz, err := json.Marshal(types.HttpRequest{
		URL: url,
	})
	require.NoError(t, err)

	th, err := ht.Execute(ctx, bz, dealInfo)
	require.NoError(t, err)
	require.NotNil(t, th)

	return th
}

func assertFileContents(t *testing.T, file string, expected string) {
	bz, err := ioutil.ReadFile(file)
	require.NoError(t, err)
	require.EqualValues(t, expected, string(bz))
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

func serversWithRangeHandler(t *testing.T, str string) map[string]func(t *testing.T) (url string, close func(), h host.Host) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		offset := r.Header.Get("Range")
		finalOffset := strings.TrimSuffix(strings.TrimPrefix(offset, "bytes="), "-")
		start, _ := strconv.ParseInt(finalOffset, 10, 64)
		w.WriteHeader(200)
		w.Write([]byte(str[start:]))
	}

	return serversWithCustomHandler(t, handler)
}

func serversWithCustomHandler(t *testing.T, handler http.HandlerFunc) map[string]func(t *testing.T) (url string, close func(), h host.Host) {
	svcs := map[string]func(t *testing.T) (url string, close func(), h host.Host){
		"http": func(t *testing.T) (string, func(), host.Host) {
			svr := httptest.NewServer(handler)
			return svr.URL, svr.Close, nil
		},
		"libp2p-http": func(t *testing.T) (string, func(), host.Host) {
			svr := newLibp2pHttpServer(t, handler)
			return svr.URL, svr.Close, svr.clientHost
		},
	}

	return svcs
}

type lip2pHttpServer struct {
	srvHost    host.Host
	clientHost host.Host
	listener   net.Listener
	URL        string
}

func (l *lip2pHttpServer) Close() {
	l.srvHost.Close()
	l.clientHost.Close()
	l.listener.Close()
}

func newLibp2pHttpServer(t *testing.T, handler func(http.ResponseWriter, *http.Request)) *lip2pHttpServer {
	l := &lip2pHttpServer{}

	m1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	m2, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	l.srvHost = newHost(t, m1)
	l.clientHost = newHost(t, m2)

	l.srvHost.Peerstore().AddAddrs(l.clientHost.ID(), l.clientHost.Addrs(), peerstore.PermanentAddrTTL)
	l.clientHost.Peerstore().AddAddrs(l.srvHost.ID(), l.srvHost.Addrs(), peerstore.PermanentAddrTTL)

	listener, err := gostream.Listen(l.srvHost, p2phttp.DefaultP2PProtocol)
	require.NoError(t, err)
	l.listener = listener

	patt := randSeq(10)

	go func() {
		http.HandleFunc("/"+patt, handler)
		server := &http.Server{}
		server.ConnContext = SaveConnInContext
		server.Serve(listener)
	}()

	l.URL = fmt.Sprintf("libp2p://%s/%s", l.srvHost.ID().Pretty(), patt)
	return l
}

func newHost(t *testing.T, listen multiaddr.Multiaddr) host.Host {
	h, err := libp2p.New(
		context.Background(),
		libp2p.ListenAddrs(listen),
	)
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
