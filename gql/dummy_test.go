package gql

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDummyServer(t *testing.T) {
	rq := require.New(t)
	ctx := context.Background()

	mux := http.NewServeMux()
	port := 8080
	listenAddr := fmt.Sprintf(":%d", port)
	t.Logf("server listening on %s\n", listenAddr)
	err := serveDummyDeals(mux, port)
	rq.NoError(err)

	srv := &http.Server{Addr: listenAddr, Handler: mux}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			rq.Fail(err.Error())
		}
	}()

	var sourceBuff bytes.Buffer
	sourceBuff.WriteString(strings.Repeat("test\n", 1024))
	randSource := &sourceBuff
	var randBytes bytes.Buffer
	_, err = randBytes.ReadFrom(randSource)
	rq.NoError(err)

	fileName := "test.car"
	err = os.WriteFile(path.Join(DummyDealsDir, fileName), randBytes.Bytes(), 0644)
	rq.NoError(err)

	reqUrl := DummyDealsBase + "/" + fileName
	req, err := http.NewRequest("GET", reqUrl, nil)
	rq.NoError(err)

	req = req.WithContext(ctx)
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

	if err := srv.Shutdown(ctx); err != nil {
		rq.Fail(err.Error())
	}

	wg.Wait()
}
