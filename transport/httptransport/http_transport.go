package httptransport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"

	"github.com/google/uuid"

	logging "github.com/ipfs/go-log/v2"

	"github.com/filecoin-project/boost/transport/types"

	"github.com/filecoin-project/boost/transport"
)

var log = logging.Logger("http-transport")

const (
	// 1 Mb
	readBufferSize = 1048576
)

var _ transport.Transport = (*httpTransport)(nil)

func New() *httpTransport {
	return &httpTransport{}
}

type httpTransport struct {
	// TODO Construct and inject an http client here ?
}

func (h *httpTransport) Execute(ctx context.Context, transportInfo []byte, dealInfo *types.TransportDealInfo) (th transport.Handler, err error) {
	// de-serialize transport opaque token
	tInfo := &types.HttpRequest{}
	if err := json.Unmarshal(transportInfo, tInfo); err != nil {
		return nil, fmt.Errorf("failed to de-serialize transport info bytes, bytes:%s, err:%w", string(transportInfo), err)
	}
	// check that the outputFile exists
	fi, err := os.Stat(dealInfo.OutputFile)
	if err != nil {
		return nil, fmt.Errorf("output file state error: %w", err)
	}
	// validate req
	if _, err := url.Parse(tInfo.URL); err != nil {
		return nil, fmt.Errorf("failed to parse url: %w", err)
	}
	// do we have more bytes than required already ?
	fileSize := fi.Size()
	if fileSize > dealInfo.DealSize {
		return nil, fmt.Errorf("deal size=%d but file size=%d", dealInfo.DealSize, fileSize)
	}

	// construct the transfer instance that will act as the transfer handler
	tctx, cancel := context.WithCancel(ctx)
	t := &transfer{
		cancel:         cancel,
		tInfo:          tInfo,
		dealInfo:       dealInfo,
		eventCh:        make(chan types.TransportEvent, 256),
		nBytesReceived: fileSize,
	}

	// is the transfer already complete ? we check this by comparing the number of bytes
	// in the output file with the deal size.
	if fileSize == dealInfo.DealSize {
		defer close(t.eventCh)
		defer cancel()

		if err := t.emitEvent(tctx, types.TransportEvent{
			NBytesReceived: fileSize,
		}, dealInfo.DealUuid); err != nil {
			return nil, fmt.Errorf("failed to publish transfer completion event, id: %s, err: %w", t.dealInfo.DealUuid, err)
		}
		return t, nil
	}
	// start executing the transfer
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		defer close(t.eventCh)
		defer t.cancel()

		if err := t.execute(tctx); err != nil {
			if err := t.emitEvent(tctx, types.TransportEvent{
				Error: err,
			}, dealInfo.DealUuid); err != nil {
				log.Errorw("failed to publish transport error", "id", t.dealInfo.DealUuid, "err", err)
			}
		}
	}()
	return t, nil
}

type transfer struct {
	closeOnce sync.Once
	cancel    context.CancelFunc

	eventCh chan types.TransportEvent

	tInfo    *types.HttpRequest
	dealInfo *types.TransportDealInfo
	wg       sync.WaitGroup

	nBytesReceived int64
}

func (t *transfer) emitEvent(ctx context.Context, evt types.TransportEvent, id uuid.UUID) error {
	select {
	case t.eventCh <- evt:
		return nil
	default:
		return fmt.Errorf("dropping event %+v as channel is full for deal id %s", evt, id)
	}
}

func (t *transfer) execute(ctx context.Context) error {
	// construct request
	req, err := http.NewRequest("GET", t.tInfo.URL, nil)
	if err != nil {
		return fmt.Errorf("failed to create http req: %w", err)
	}

	// add range req to start reading from the last byte we have in the output file
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", t.nBytesReceived))
	// init the request with the transfer context
	req = req.WithContext(ctx)
	// open output file in append-only mode for writing
	of, err := os.OpenFile(t.dealInfo.OutputFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open output file: %w", err)
	}
	defer of.Close()

	// start the http transfer
	remaining := t.dealInfo.DealSize - t.nBytesReceived
	if err := t.doHttp(ctx, req, of, remaining); err != nil {
		// do not resume transfer if context has been cancelled
		if ctx.Err() == context.Canceled || ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("transfer context err: %w", ctx.Err())
		}

		//
		// TODO: resumption
		return fmt.Errorf("failed to execute http transfer: %w", err)
	}
	// --- http request finished successfully. see if we got the number of bytes we expected.

	// if the number of bytes we've received is not the same as the deal size, we have a failure.
	if t.nBytesReceived != t.dealInfo.DealSize {
		return fmt.Errorf("mismatch in dealSize vs received bytes, dealSize=%d, received=%d", t.dealInfo.DealSize, t.nBytesReceived)
	}

	return nil
}

func (t *transfer) doHttp(ctx context.Context, req *http.Request, dst io.Writer, toRead int64) error {
	// send http request and validate response
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send  http req: %w", err)
	}
	// we should either get back a 200 or a 206 -> anything else means something has gone wrong and we return an error.
	defer resp.Body.Close() // nolint
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("http req failed: code:%d, status:%s", resp.StatusCode, resp.Status)
	}

	//  start reading the response stream `readBufferSize` at a time using a limit reader so we only read as many bytes as we need to.
	buf := make([]byte, readBufferSize)
	limitR := io.LimitReader(resp.Body, toRead)
	for {
		nr, readErr := limitR.Read(buf)

		// if we read more than zero bytes, write whatever read.
		if nr > 0 {
			nw, writeErr := dst.Write(buf[0:nr])

			// if the number of read and written bytes don't match -> something has gone wrong, abort the http req.
			if nw < 0 || nr != nw {
				if writeErr != nil {
					return fmt.Errorf("failed to write to output file: %w", err)
				}
				return fmt.Errorf("read-write mismatch writing to the output file, read=%d, written=%d", nr, nw)
			}

			t.nBytesReceived = t.nBytesReceived + int64(nw)
			// emit event updating the number of bytes received
			if err := t.emitEvent(ctx, types.TransportEvent{
				NBytesReceived: t.nBytesReceived,
			}, t.dealInfo.DealUuid); err != nil {
				log.Errorw("failed to publish transport event", "id", t.dealInfo.DealUuid, "err", err)
			}
		}
		// the http stream we're reading from has sent us an EOF, nothing to do here.
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("error reading from http response stream: %w", err)
		}
	}
}

// Close shuts down the transfer for the given deal. It is the caller's responsibility to call Close after it no longer needs the transfer.
func (t *transfer) Close() {
	t.closeOnce.Do(func() {
		// cancel the context associated with the transfer
		if t.cancel != nil {
			t.cancel()
		}
		// wait for all go-routines associated with the transfer to return
		t.wg.Wait()
	})

}

func (t *transfer) Sub() chan types.TransportEvent {
	return t.eventCh
}
