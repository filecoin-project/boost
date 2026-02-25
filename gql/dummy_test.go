package gql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDummyServer(t *testing.T) {
	rq := require.New(t)

	mux := http.NewServeMux()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	rq.NoError(err)

	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	rq.True(ok)

	t.Logf("server listening on %s\n", listener.Addr().String())
	err = serveDummyDeals(mux, tcpAddr.Port)
	rq.NoError(err)

	srv := &http.Server{Handler: mux}

	serveErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
		close(serveErr)
	}()

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, srv.Shutdown(shutdownCtx))

		if err, ok := <-serveErr; ok {
			require.NoError(t, err)
		}
	})

	var sourceBuff bytes.Buffer
	sourceBuff.WriteString(strings.Repeat("test\n", 1024))
	randSource := &sourceBuff
	var randBytes bytes.Buffer
	_, err = randBytes.ReadFrom(randSource)
	rq.NoError(err)

	fileName := fmt.Sprintf("%d-test.car", time.Now().UnixNano())
	filePath := path.Join(DummyDealsDir, fileName)
	err = os.WriteFile(filePath, randBytes.Bytes(), 0o644)
	rq.NoError(err)
	t.Cleanup(func() {
		_ = os.Remove(filePath)
	})

	reqURL := fmt.Sprintf("http://%s/%s/%s", listener.Addr().String(), DummyDealsPrefix, fileName)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, reqURL, nil)
	rq.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	rq.NoError(err)

	defer resp.Body.Close() // nolint
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		rq.Fail(fmt.Errorf("http req failed: code:%d, status:%s", resp.StatusCode, resp.Status).Error())
	}

	var buf bytes.Buffer
	_, err = buf.ReadFrom(resp.Body)
	rq.NoError(err)

	rq.Equal(randBytes.String(), buf.String())

	t.Logf("Read %d bytes", len(buf.Bytes()))
}
