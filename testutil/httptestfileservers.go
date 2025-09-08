package testutil

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// HttpTestFileServer returns a http server that serves files from the given directory with some latency
func HttpTestFileServer(t *testing.T, dir string) (*httptest.Server, error) {
	// start server with data to send
	fileSystem := &SlowFileOpener{Dir: dir}
	handler := http.FileServer(fileSystem)
	svr := httptest.NewServer(handler)
	return svr, nil
}

type HttpTestServer struct {
	*httptest.Server
	lk      sync.Mutex
	working bool
}

func (s *HttpTestServer) SetWorking(working bool) {
	s.lk.Lock()
	defer s.lk.Unlock()
	s.working = working
}

// HttpTestUnstartedFileServer returns a http server that serves files from the given directory
func HttpTestUnstartedFileServer(t *testing.T, dir string) *HttpTestServer {
	var tsrv *HttpTestServer
	handler := http.FileServer(http.Dir(dir))
	svr := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tsrv.lk.Lock()
		working := tsrv.working
		tsrv.lk.Unlock()
		if !working {
			w.WriteHeader(401)
		} else {
			handler.ServeHTTP(w, r)
		}
	}))
	tsrv = &HttpTestServer{Server: svr, working: true}
	return tsrv
}

func addContentLengthHeader(w http.ResponseWriter, l int) {
	w.Header().Add("Content-Length", strconv.Itoa(l))
	w.WriteHeader(200)
}

type unblockInfo struct {
	ch        chan struct{}
	closeOnce sync.Once
}

// BlockingHttpTestServer returns an http server that blocks for a given file until the client unblocks the serving of the file.
type BlockingHttpTestServer struct {
	URL string
	svc *httptest.Server

	mu      sync.Mutex
	unblock map[string]*unblockInfo
}

func NewBlockingHttpTestServer(t *testing.T, dir string) *BlockingHttpTestServer {
	b := &BlockingHttpTestServer{
		unblock: make(map[string]*unblockInfo),
	}

	svc := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fp := path.Clean(r.URL.Path)
		absPath := filepath.Join(dir, fp)

		switch r.Method {
		case http.MethodGet:
			// wait till serving the file is unblocked
			name := path.Clean(strings.TrimPrefix(r.URL.Path, "/"))
			b.mu.Lock()
			ubi := b.unblock[name]
			b.mu.Unlock()
			<-ubi.ch

			// serve the file
			upath := r.URL.Path
			if !strings.HasPrefix(upath, "/") {
				upath = "/" + upath
				r.URL.Path = upath
			}
			http.ServeFile(w, r, absPath)
		case http.MethodHead:
			fp := path.Clean(r.URL.Path)
			absPath := filepath.Join(dir, fp)
			stat, err := os.Stat(absPath)
			if err != nil {
				t.Logf("failed to get file stat: %s", err.Error())
				w.WriteHeader(500)
				return
			}
			addContentLengthHeader(w, int(stat.Size()))
		}

	}))

	b.svc = svc
	return b
}

func (b *BlockingHttpTestServer) AddFile(name string) {
	b.mu.Lock()
	b.unblock[name] = &unblockInfo{ch: make(chan struct{})}
	b.mu.Unlock()
}

func (b *BlockingHttpTestServer) UnblockFile(name string) {
	b.mu.Lock()
	ubi := b.unblock[name]
	b.mu.Unlock()
	ubi.closeOnce.Do(func() {
		close(ubi.ch)
	})
}

func (b *BlockingHttpTestServer) Start() {
	b.svc.Start()
	b.URL = b.svc.URL
}

func (b *BlockingHttpTestServer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ubi := range b.unblock {
		ub := ubi
		ub.closeOnce.Do(func() {
			close(ubi.ch)
		})
	}

	b.svc.CloseClientConnections()
	b.svc.Close()
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

// HttpTestDisconnectingServer returns a test http server that serves files from the given directory but disconnects after sending `afterEvery` bytes
// starting at the start offset mentioned in the Range request.
func HttpTestDisconnectingServer(t *testing.T, dir string, afterEvery int64) *httptest.Server {
	svr := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fp := path.Clean(r.URL.Path)
		absPath := filepath.Join(dir, fp)

		switch r.Method {
		case http.MethodGet:
			// process the start offset
			offset := r.Header.Get("Range")
			startend := strings.Split(strings.TrimPrefix(offset, "bytes="), "-")
			start, _ := strconv.ParseInt(startend[0], 10, 64)
			// only send `afterEvery` bytes and then disconnect
			end := start + afterEvery

			// open the file to serve
			upath := r.URL.Path
			if !strings.HasPrefix(upath, "/") {
				upath = "/" + upath
				r.URL.Path = upath
			}
			f, err := os.Open(absPath)
			if err != nil {
				t.Logf("failed to open file to serve: %s", err)
				w.WriteHeader(500)
				return
			}
			defer func() {
				_ = f.Close()
			}()

			// prevent buffer overflow
			fi, err := f.Stat()
			if err != nil {
				t.Logf("failed to stat file: %s", err)
				w.WriteHeader(500)
				return
			}
			if end > fi.Size() {
				end = fi.Size()
			}

			// read (end-start) bytes from the file starting at the given offset and write them to the response
			bz := make([]byte, end-start)
			n, err := f.ReadAt(bz, start)
			if err != nil {
				t.Logf("failed to read file: %s", err)
				w.WriteHeader(500)
				return
			}
			if int64(n) != (end - start) {
				w.WriteHeader(500)
				return
			}

			w.WriteHeader(200)
			_, err = w.Write(bz)
			if err != nil {
				t.Logf("failed to write file: %s", err)
				w.WriteHeader(500)
				return
			}

			// close the connection so client sees an error while reading the response
			c := GetConn(r)
			_ = c.Close()
		case http.MethodHead:
			stat, err := os.Stat(absPath)
			if err != nil {
				t.Logf("failed to get file stat: %s", err)
				w.WriteHeader(500)
				return
			}
			addContentLengthHeader(w, int(stat.Size()))
		}

	}))
	svr.Config.ConnContext = SaveConnInContext

	return svr
}
