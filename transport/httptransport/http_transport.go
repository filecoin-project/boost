package httptransport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/filecoin-project/boost/storagemarket/logs"

	"github.com/filecoin-project/boost/transport"
	"github.com/filecoin-project/boost/transport/types"
	"github.com/google/uuid"
	"github.com/jpillora/backoff"
	"github.com/libp2p/go-libp2p-core/host"
	p2phttp "github.com/libp2p/go-libp2p-http"
	"golang.org/x/xerrors"
)

const (
	// 1 Mib
	readBufferSize = 1048576

	minBackOff           = 1 * time.Minute
	maxBackOff           = 1 * time.Hour
	factor               = 2
	maxReconnectAttempts = 8

	libp2pScheme = "libp2p"
)

var _ transport.Transport = (*httpTransport)(nil)

type Option func(*httpTransport)

func BackOffRetryOpt(minBackoff, maxBackoff time.Duration, factor, maxReconnectAttempts float64) Option {
	return func(h *httpTransport) {
		h.minBackOffWait = minBackoff
		h.maxBackoffWait = maxBackoff
		h.backOffFactor = factor
		h.maxReconnectAttempts = maxReconnectAttempts
	}
}

type httpTransport struct {
	libp2pHost   host.Host
	libp2pClient *http.Client

	minBackOffWait       time.Duration
	maxBackoffWait       time.Duration
	backOffFactor        float64
	maxReconnectAttempts float64

	dl *logs.DealLogger
}

func New(host host.Host, dealLogger *logs.DealLogger, opts ...Option) *httpTransport {
	ht := &httpTransport{
		libp2pHost:           host,
		minBackOffWait:       minBackOff,
		maxBackoffWait:       maxBackOff,
		backOffFactor:        factor,
		maxReconnectAttempts: maxReconnectAttempts,
		dl:                   dealLogger.Subsystem("http-transport"),
	}
	for _, o := range opts {
		o(ht)
	}

	// init a libp2p-http client
	tr := &http.Transport{}
	p2ptr := p2phttp.NewTransport(host, p2phttp.ProtocolOption(types.DataTransferProtocol))
	tr.RegisterProtocol("libp2p", p2ptr)
	ht.libp2pClient = &http.Client{Transport: tr}

	return ht
}

func (h *httpTransport) Execute(ctx context.Context, transportInfo []byte, dealInfo *types.TransportDealInfo) (th transport.Handler, err error) {
	deadline, _ := ctx.Deadline()
	duuid := dealInfo.DealUuid
	h.dl.Infow(duuid, "execute transfer", "deal size", dealInfo.DealSize, "output file", dealInfo.OutputFile,
		"time before context deadline", time.Until(deadline).String())

	// de-serialize transport opaque token
	tInfo := &types.HttpRequest{}
	if err := json.Unmarshal(transportInfo, tInfo); err != nil {
		return nil, fmt.Errorf("failed to de-serialize transport info bytes, bytes:%s, err:%w", string(transportInfo), err)
	}

	if len(tInfo.URL) == 0 {
		return nil, xerrors.New("deal url is empty")
	}

	// parse request URL
	u, err := parseUrl(tInfo.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse request url: %w", err)
	}
	tInfo.URL = u.url

	// check that the outputFile exists
	fi, err := os.Stat(dealInfo.OutputFile)
	if err != nil {
		return nil, fmt.Errorf("output file state error: %w", err)
	}

	// do we have more bytes than required already ?
	fileSize := fi.Size()
	if fileSize > dealInfo.DealSize {
		return nil, fmt.Errorf("deal size=%d but file size=%d", dealInfo.DealSize, fileSize)
	}
	h.dl.Infow(duuid, "existing file size", "file size", fileSize, "deal size", dealInfo.DealSize)

	// construct the transfer instance that will act as the transfer handler
	tctx, cancel := context.WithCancel(ctx)
	t := &transfer{
		cancel:         cancel,
		tInfo:          tInfo,
		dealInfo:       dealInfo,
		eventCh:        make(chan types.TransportEvent, 256),
		nBytesReceived: fileSize,
		backoff: &backoff.Backoff{
			Min:    h.minBackOffWait,
			Max:    h.maxBackoffWait,
			Factor: h.backOffFactor,
			Jitter: true,
		},
		maxReconnectAttempts: h.maxReconnectAttempts,
		dl:                   h.dl,
	}

	// If this is a libp2p URL
	if u.scheme == libp2pScheme {
		// Use the libp2p client
		t.client = h.libp2pClient
		// Add the peer's address to the peerstore so we can dial it
		addrTtl := time.Hour
		if deadline, ok := ctx.Deadline(); ok {
			addrTtl = time.Until(deadline)
		}
		h.libp2pHost.Peerstore().AddAddr(u.peerID, u.multiaddr, addrTtl)
		h.dl.Infow(duuid, "libp2p-http url", "url", tInfo.URL, "peer id", u.peerID, "multiaddr", u.multiaddr)
	} else {
		t.client = http.DefaultClient
		h.dl.Infow(duuid, "http url", "url", tInfo.URL)
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

		h.dl.Infow(duuid, "file size is already equal to deal size, returning")
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
				t.dl.LogError(duuid, "failed to publish transport error", err)
			}
		}
	}()

	h.dl.Infow(duuid, "started async http transfer")
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

	backoff              *backoff.Backoff
	maxReconnectAttempts float64

	client *http.Client
	dl     *logs.DealLogger
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
	duuid := t.dealInfo.DealUuid
	for {
		// construct request
		req, err := http.NewRequest("GET", t.tInfo.URL, nil)
		if err != nil {
			return fmt.Errorf("failed to create http req: %w", err)
		}

		// get the number of bytes already received (the size of the output file)
		st, err := os.Stat(t.dealInfo.OutputFile)
		if err != nil {
			return fmt.Errorf("failed to stat output file: %w", err)
		}
		t.nBytesReceived = st.Size()

		// add request headers
		for name, val := range t.tInfo.Headers {
			req.Header.Set(name, val)
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
		reqErr := t.doHttp(ctx, req, of, remaining)
		if reqErr != nil {
			t.dl.Infow(duuid, "one http req done", "http code", reqErr.code, "outputErr", reqErr.Error())
		} else {
			t.dl.Infow(duuid, "http req finished with no error")
		}

		if reqErr == nil {
			t.dl.Infow(duuid, "http req done without any errors")
			// if there's no error, transfer was successful
			break
		}
		_ = of.Close()

		// check if the error is a 4xx error, meaning there is a problem with
		// the request (eg 401 Unauthorized)
		if reqErr.code/100 == 4 {
			t.dl.LogError(duuid, "terminating http req as got 4xx code", reqErr)
			return reqErr.error
		}

		// do not resume transfer if context has been cancelled or if the context deadline has exceeded
		err = reqErr.error
		if xerrors.Is(err, context.Canceled) || xerrors.Is(err, context.DeadlineExceeded) {
			t.dl.LogError(duuid, "terminating http req as context cancelled or deadline exceeded", err)
			return fmt.Errorf("transfer context err: %w", err)
		}

		// backoff-retry transfer if max number of attempts haven't been exhausted
		nAttempts := t.backoff.Attempt() + 1
		if nAttempts >= t.maxReconnectAttempts {
			t.dl.Errorw(duuid, "terminating http req as exhausted max attempts", "err", err.Error(), "maxAttempts", t.maxReconnectAttempts)
			return fmt.Errorf("could not finish transfer even after %.0f attempts, lastErr: %w", t.maxReconnectAttempts, err)
		}
		duration := t.backoff.Duration()
		bt := time.NewTimer(duration)
		t.dl.Infow(duuid, "retrying http req after waiting", "wait time", duration.String(),
			"nAttempts", nAttempts)
		defer bt.Stop()
		select {
		case <-bt.C:
		case <-ctx.Done():
			t.dl.LogError(duuid, "did not proceed with retry as context cancelled", ctx.Err())
			return fmt.Errorf("transfer context err after %f attempts to finish transfer, lastErr=%s, contextErr=%w", t.backoff.Attempt(), err, ctx.Err())
		}
	}

	// --- http request finished successfully. see if we got the number of bytes we expected.

	// if the number of bytes we've received is not the same as the deal size, we have a failure.
	if t.nBytesReceived != t.dealInfo.DealSize {
		return fmt.Errorf("mismatch in dealSize vs received bytes, dealSize=%d, received=%d", t.dealInfo.DealSize, t.nBytesReceived)
	}
	// if the file size is not equal to the number of bytes received, something has gone wrong
	st, err := os.Stat(t.dealInfo.OutputFile)
	if err != nil {
		return fmt.Errorf("failed to stat output file: %w", err)
	}
	if t.nBytesReceived != st.Size() {
		return fmt.Errorf("mismtach in output file size vs received bytes, fileSize=%d, receivedBytes=%d", st.Size(), t.nBytesReceived)
	}

	t.dl.Infow(duuid, "http req finished successfully", "nBytesReceived", t.nBytesReceived,
		"file size", st.Size())

	return nil
}

func (t *transfer) doHttp(ctx context.Context, req *http.Request, dst io.Writer, toRead int64) *httpError {
	duid := t.dealInfo.DealUuid
	t.dl.Infow(duid, "sending http req", "received", t.nBytesReceived, "remaining",
		toRead, "range-rq", req.Header.Get("Range"))

	// send http request and validate response
	resp, err := t.client.Do(req)
	if err != nil {
		return &httpError{error: fmt.Errorf("failed to send  http req: %w", err)}
	}
	// we should either get back a 200 or a 206 -> anything else means something has gone wrong and we return an error.
	defer resp.Body.Close() // nolint
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return &httpError{
			error: fmt.Errorf("http req failed: code: %d, status: %s", resp.StatusCode, resp.Status),
			code:  resp.StatusCode,
		}
	}

	//  start reading the response stream `readBufferSize` at a time using a limit reader so we only read as many bytes as we need to.
	buf := make([]byte, readBufferSize)
	limitR := io.LimitReader(resp.Body, toRead)
	for {
		if ctx.Err() != nil {
			t.dl.LogError(duid, "not reading http response anymore", ctx.Err())
			return &httpError{error: ctx.Err()}
		}
		nr, readErr := limitR.Read(buf)

		// if we read more than zero bytes, write whatever read.
		if nr > 0 {
			nw, writeErr := dst.Write(buf[0:nr])

			// if the number of read and written bytes don't match -> something has gone wrong, abort the http req.
			if nw < 0 || nr != nw {
				if writeErr != nil {
					return &httpError{error: fmt.Errorf("failed to write to output file: %w", writeErr)}
				}
				return &httpError{error: fmt.Errorf("read-write mismatch writing to the output file, read=%d, written=%d", nr, nw)}
			}

			t.nBytesReceived = t.nBytesReceived + int64(nw)

			// emit event updating the number of bytes received
			if err := t.emitEvent(ctx, types.TransportEvent{
				NBytesReceived: t.nBytesReceived,
			}, t.dealInfo.DealUuid); err != nil {
				t.dl.LogError(duid, "failed to publish transport event", err)
			}
		}
		// the http stream we're reading from has sent us an EOF, nothing to do here.
		if readErr == io.EOF {
			t.dl.Infow(duid, "http server sent EOF", "received", t.nBytesReceived, "deal-size", t.dealInfo.DealSize)
			return nil
		}
		if readErr != nil {
			return &httpError{error: fmt.Errorf("error reading from http response stream: %w", readErr)}
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
