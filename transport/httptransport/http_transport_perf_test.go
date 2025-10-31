package httptransport

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/filecoin-project/boost/transport/types"
	"github.com/stretchr/testify/require"
)

func TestHttpTransportMultistreamPerformance(t *testing.T) {
	// this test runs a tcp proxy in front of the http web server that adds a latency to each tcp packet.
	// This is done to simulate network latency and ensure that multistream transfer is reasonably faster than singlestream.
	// rawSize and latency need to be big enough so that data transfer takes a reasonable proportion of time comparing to writing to the disk.
	ctx := context.Background()
	latency := 100 * time.Millisecond
	rawSize := 100 * readBufferSize // 100 Mib
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
				_, _ = w.Write(st.carBytes[start:])
			} else {
				_, _ = w.Write(st.carBytes[start:end])
			}
		case http.MethodHead:
			addContentLengthHeader(w, len(st.carBytes))
		}
	}

	svr := httptest.NewServer(handler)

	defer svr.Close()

	localAddr := "localhost:26379"
	listener, err := net.Listen("tcp", localAddr)
	require.NoError(t, err)

	go func() {
		for {
			conn, err := listener.Accept()
			require.NoError(t, err)
			go handleConnection(t, conn, strings.TrimPrefix(svr.URL, "http://"), latency)
		}
	}()

	runTransfer := func(chunks int) time.Duration {
		start := time.Now()
		of := getTempFilePath(t)
		th := executeTransfer(t, ctx, New(nil, newDealLogger(t, ctx), NChunksOpt(chunks)), carSize, types.HttpRequest{URL: "http://" + localAddr}, of)
		require.NotNil(t, th)

		evts := waitForTransferComplete(th)
		require.NotEmpty(t, evts)
		require.EqualValues(t, carSize, evts[len(evts)-1].NBytesReceived)
		assertFileContents(t, of, st.carBytes)
		return time.Since(start)
	}

	singleStreamTime := runTransfer(1)
	multiStreamTime := runTransfer(5)
	t.Logf("Single stream: %s", singleStreamTime)
	t.Logf("Multi stream: %s", multiStreamTime)
	// the larger the payload and latency - the faster multistream becomes comparing to singlestream.
	require.True(t, float64(singleStreamTime.Milliseconds())/float64(multiStreamTime.Milliseconds()) > 1)
}

func handleConnection(t *testing.T, localConn net.Conn, remoteAddr string, latency time.Duration) {
	remoteConn, err := net.Dial("tcp", remoteAddr)
	require.NoError(t, err)
	defer func() {
		_ = remoteConn.Close()
	}()

	proxyFunc := func(from, to net.Conn) {
		buf := make([]byte, readBufferSize)
		for {
			// ensures that we don't get blocked on reading multi-chunked payloads
			rerr := from.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			require.NoError(t, rerr)
			numBytesRead, rerr := from.Read(buf)
			if numBytesRead > 0 {
				_, werr := to.Write(buf[0:numBytesRead])
				require.NoError(t, werr)
				time.Sleep(latency)
			}
			if errors.Is(rerr, io.EOF) || errors.Is(rerr, os.ErrDeadlineExceeded) {
				break
			}
			if rerr != nil {
				break
			}
		}
	}

	for {
		proxyFunc(localConn, remoteConn)
		proxyFunc(remoteConn, localConn)
	}
}
