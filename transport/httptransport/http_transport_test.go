package httptransport

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/filecoin-project/boost/transport"

	"github.com/filecoin-project/boost/transport/types"
	"github.com/stretchr/testify/require"
)

func TestSimpleTransfer(t *testing.T) {
	ctx := context.Background()

	// start server with data to send
	size := (100 * readBufferSize) + 30
	str := strings.Repeat("a", size)
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rd := strings.NewReader(str)
		http.ServeContent(w, r, "", time.Now(), rd)
	}))
	defer svr.Close()

	of := getTempFilePath(t)
	th := executeTransfer(t, ctx, &httpTransport{}, size, svr.URL, of)
	require.NotNil(t, th)

	evts := waitForTransferComplete(t, th)
	require.NotEmpty(t, evts)
	require.EqualValues(t, size, evts[len(evts)-1].NBytesReceived)

	assertFileContents(t, of, str)
}

func TestTransportRespectsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Hour)
	}))

	of := getTempFilePath(t)
	th := executeTransfer(t, ctx, &httpTransport{}, 100, svr.URL, of)
	require.NotNil(t, th)

	evts := waitForTransferComplete(t, th)
	require.NotEmpty(t, evts)
	require.Len(t, evts, 1)
	require.Contains(t, evts[0].Error.Error(), "context")
}

func TestConcurrentTransfers(t *testing.T) {
	ctx := context.Background()

	// start server with data to send
	size := (100 * readBufferSize) + 30
	str := strings.Repeat("a", size)
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rd := strings.NewReader(str)
		http.ServeContent(w, r, "", time.Now(), rd)
	}))
	defer svr.Close()
	ht := &httpTransport{}

	var errG errgroup.Group
	for i := 0; i < 10; i++ {
		errG.Go(func() error {
			of := getTempFilePath(t)
			th := executeTransfer(t, ctx, ht, size, svr.URL, of)

			evts := waitForTransferComplete(t, th)
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
}

func TestCompletionOnMultipleAttemptsWithSameFile(t *testing.T) {
	ctx := context.Background()
	of := getTempFilePath(t)

	// start server with data to send
	size := (100 * readBufferSize) + 30
	str := strings.Repeat("a", size)

	end := readBufferSize
	for i := 1; ; i++ {
		svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rangeEnd := readBufferSize * i
			if rangeEnd > size {
				rangeEnd = size
			}
			http.ServeContent(w, r, "", time.Time{}, strings.NewReader(str[:rangeEnd]))
		}))

		th := executeTransfer(t, ctx, &httpTransport{}, size, svr.URL, of)
		require.NotNil(t, th)

		evts := waitForTransferComplete(t, th)

		if len(evts) == 1 {
			require.EqualValues(t, size, evts[0].NBytesReceived)
			break
		}
		require.Contains(t, evts[len(evts)-1].Error.Error(), "mismatch")

		assertFileContents(t, of, str[:end])
		svr.Close()
		end = end + readBufferSize
		if end > size {
			end = size
		}
	}

	// ensure file contents are correct in the end
	assertFileContents(t, of, str)
}

func TestTransferCancellation(t *testing.T) {

	// start server with data to send
	size := (100 * readBufferSize) + 30
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Second)
	}))
	defer svr.Close()

	of := getTempFilePath(t)
	th := executeTransfer(t, context.Background(), &httpTransport{}, size, svr.URL, of)
	require.NotNil(t, th)
	// close the transfer so context is cancelled
	th.Close()

	evts := waitForTransferComplete(t, th)
	require.Len(t, evts, 1)
	require.Contains(t, evts[0].Error.Error(), "context")
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
	f, err := os.Open(file)
	require.NoError(t, err)
	bz, err := ioutil.ReadAll(f)
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

func waitForTransferComplete(t *testing.T, th transport.Handler) []types.TransportEvent {
	var evts []types.TransportEvent

	for evt := range th.Sub() {
		evts = append(evts, evt)
	}

	return evts
}
