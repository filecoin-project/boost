package httptransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/transport"
	"github.com/filecoin-project/boost/transport/httptransport/util"
	"github.com/filecoin-project/boost/transport/types"
	"github.com/google/uuid"
	"github.com/jpillora/backoff"
	p2phttp "github.com/libp2p/go-libp2p-http"
	"github.com/libp2p/go-libp2p/core/host"
	"golang.org/x/sync/errgroup"
)

const (
	// 1 Mib
	readBufferSize = 1048576

	// 5s, 7s, 11s, 16s, 25s, 38s, 1m, 1m30s, 2m, 3m, 7m, 10m, 10m, 10m, 10m
	minBackOff           = 5 * time.Second
	maxBackOff           = 10 * time.Minute
	factor               = 1.5
	maxReconnectAttempts = 15

	numChunks = 5
)

type httpError struct {
	error
	code int
}

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

func NChunksOpt(nChunks int) Option {
	return func(h *httpTransport) {
		h.nChunks = nChunks
	}
}

func AllowPrivateIPsOpt(b bool) Option {
	return func(h *httpTransport) {
		h.allowPrivateIPs = b
	}
}

type httpTransport struct {
	libp2pHost   host.Host
	libp2pClient *http.Client

	minBackOffWait       time.Duration
	maxBackoffWait       time.Duration
	backOffFactor        float64
	maxReconnectAttempts float64

	nChunks         int
	allowPrivateIPs bool

	dl *logs.DealLogger
}

func New(host host.Host, dealLogger *logs.DealLogger, opts ...Option) *httpTransport {
	ht := &httpTransport{
		libp2pHost:           host,
		minBackOffWait:       minBackOff,
		maxBackoffWait:       maxBackOff,
		backOffFactor:        factor,
		maxReconnectAttempts: maxReconnectAttempts,
		nChunks:              numChunks,
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
		return nil, errors.New("deal url is empty")
	}

	// parse request URL
	u, err := util.ParseUrl(tInfo.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse request url: %w", err)
	}
	tInfo.URL = u.Url

	if !h.allowPrivateIPs {
		pu, err := url.Parse(u.Url)
		if err != nil {
			return nil, fmt.Errorf("failed to parse the request url: %w", err)
		}
		if ip := net.ParseIP(pu.Hostname()); ip != nil && ip.IsPrivate() {
			return nil, fmt.Errorf("downloading from private ip addresses is not allowed")
		}
	}

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

	// default to a single stream for libp2p urls as libp2p server doesn't support range requests
	nChunks := h.nChunks
	if u.Scheme == "libp2p" || dealInfo.DealSize < 10*readBufferSize {
		nChunks = 1
	}

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
		nChunks:              nChunks,
	}

	cleanupFns := []func(){
		cancel,
		func() { t.closeEventChannel(tctx) },
	}
	cleanup := func() {
		for _, fn := range cleanupFns {
			fn()
		}
	}

	// If this is a libp2p URL
	if u.Scheme == util.Libp2pScheme {
		h.dl.Infow(duuid, "libp2p-http url", "url", tInfo.URL, "peer id", u.PeerID, "multiaddr", u.Multiaddr)

		// Use the libp2p client
		t.client = h.libp2pClient

		// Add the peer's address to the peerstore so we can dial it
		addrTtl := time.Hour
		if deadline, ok := ctx.Deadline(); ok {
			addrTtl = time.Until(deadline)
		}
		h.libp2pHost.Peerstore().AddAddr(u.PeerID, u.Multiaddr, addrTtl)

		// Protect the connection for the lifetime of the data transfer
		tag := uuid.New().String()
		h.libp2pHost.ConnManager().Protect(u.PeerID, tag)
		cleanupFns = append(cleanupFns, func() {
			h.libp2pHost.ConnManager().Unprotect(u.PeerID, tag)
		})
	} else {
		// do not follow http redirects for security reasons
		t.client = &http.Client{
			// Custom CheckRedirect function to limit redirects
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 2 { // Limit to 2 redirects
					return http.ErrUseLastResponse
				}
				return nil
			},
		}

		h.dl.Infow(duuid, "http url", "url", tInfo.URL)
	}

	// is the transfer already complete ? we check this by comparing the number of bytes
	// in the output file with the deal size.
	// DealSize might be passed as zero for offline deals.
	if dealInfo.DealSize != 0 && fileSize == dealInfo.DealSize {
		defer cleanup()

		t.emitEvent(types.TransportEvent{NBytesReceived: fileSize})
		h.dl.Infow(duuid, "file size is already equal to deal size, returning")
		return t, nil
	}

	// start executing the transfer
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		defer cleanup()

		if err := t.execute(tctx); err != nil {
			t.emitEvent(types.TransportEvent{Error: err})
		}
	}()

	h.dl.Infow(duuid, "started async http transfer")
	return t, nil
}

type transfer struct {
	closeOnce sync.Once
	cancel    context.CancelFunc

	eventCh chan types.TransportEvent
	lastEvt *types.TransportEvent

	tInfo    *types.HttpRequest
	dealInfo *types.TransportDealInfo
	wg       sync.WaitGroup

	nBytesReceived int64

	backoff              *backoff.Backoff
	maxReconnectAttempts float64

	client *http.Client
	dl     *logs.DealLogger

	nChunks int
	lock    sync.RWMutex
}

func (t *transfer) addBytesReceived(n int64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.nBytesReceived += n
	t.emitEvent(types.TransportEvent{NBytesReceived: t.nBytesReceived})
}

func (t *transfer) getBytesReceived() int64 {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.nBytesReceived
}

func (t *transfer) execute(ctx context.Context) error {
	duuid := t.dealInfo.DealUuid

	outputStats, err := os.Stat(t.dealInfo.OutputFile)
	if err != nil {
		return &httpError{error: fmt.Errorf("failed to get stats of the output file %s: %w", t.dealInfo.OutputFile, err)}
	}
	controlFile := t.dealInfo.OutputFile + "-control"
	controlStats, err := os.Stat(controlFile)

	// Check if the control file exists and create it if it doesn't. Control file captures the number of chunks that the transfer has been started with.
	// If the number of chunks changes half way through, the transfer should continue with the same chunking setting.
	nchunks := t.nChunks
	if errors.Is(err, os.ErrNotExist) {
		// if the output file is not empty, but there is no control file then that must be a continuation of a transfer from before chunking was introduced.
		// in that case set nChunks to one.
		if outputStats.Size() > 0 && controlStats == nil {
			nchunks = 1
		}

		err := t.writeControlFile(controlFile, transferConfig{nchunks})
		if err != nil {
			return &httpError{error: fmt.Errorf("failed to create control file %s: %w", controlFile, err)}
		}
	} else if err != nil {
		return &httpError{error: fmt.Errorf("failed to get stats of control file %s: %w", controlFile, err)}
	} else {
		conf, err := t.readControlFile(controlFile)
		if err != nil {
			return &httpError{error: fmt.Errorf("failed to read control file %s: %w", controlFile, err)}
		}
		nchunks = conf.NChunks
	}

	// Create downloaders. Each downloader must be initialised with the same byte range across restarts in order to resume previous downloads.
	// If the output file contains some data in it already - do not create a downloader for it again.
	// Consider the following scenarios:
	// 1. some chunks have been fully downloaded, some just partially. No chunks have been merged to the main file yet. We need to do nothing for the downloaded chunks
	// 	  and finish downloading for the partial ones;
	// 2. some chunks have been fully downloaded, some just partially. Some of the fully downloaded chunks have been merged into the main
	//    file and their temp files have been deleted. We should not re-download the merged chunks again, finish downloading
	//    the partial ones and proceed with merging.

	// determine deal size from HEAD request and make sure that it matches up with the one from the deal info
	var dealSize int64
	req, err := http.NewRequest("HEAD", t.tInfo.URL, nil)
	if err != nil {
		return &httpError{error: fmt.Errorf("failed to create http HEAD req: %w", err)}
	}
	// add request headers
	for name, val := range t.tInfo.Headers {
		req.Header.Set(name, val)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return &httpError{error: fmt.Errorf("failed to send HEAD http req: %w", err)}
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return &httpError{
			error: fmt.Errorf("http HEAD req failed: code: %d, status: %s", resp.StatusCode, resp.Status),
			code:  resp.StatusCode,
		}
	}
	if s := resp.Header.Get("Content-Length"); len(s) > 0 {
		dealSize, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			return &httpError{
				error: fmt.Errorf("error parsing content-length header: %w", err),
			}
		}
	} else {
		return &httpError{
			error: fmt.Errorf("can't determine deal size from the head request, no header"),
		}
	}

	if t.dealInfo.DealSize != 0 && dealSize != t.dealInfo.DealSize {
		return &httpError{
			error: fmt.Errorf("deal size mismatch: head: %d, dealInfo: %d", dealSize, t.dealInfo.DealSize),
		}
	}

	chunkSize := dealSize / int64(nchunks)
	lastAppendedChunk := int(outputStats.Size() / chunkSize)

	downloaders := make([]*downloader, 0, nchunks-lastAppendedChunk)

	for i := lastAppendedChunk; i < nchunks; i++ {
		rangeStart := int64(i) * chunkSize
		var rangeEnd int64
		if i == nchunks-1 {
			rangeEnd = dealSize
		} else {
			rangeEnd = rangeStart + chunkSize
		}
		// write first chunk directly to the output file
		var chunkFile string
		if i == 0 {
			chunkFile = t.dealInfo.OutputFile
		} else {
			chunkFile = t.dealInfo.OutputFile + "-" + fmt.Sprint(i)
		}
		d := downloader{
			ctx:        ctx,
			transfer:   t,
			chunkFile:  chunkFile,
			outputFile: t.dealInfo.OutputFile,
			rangeStart: rangeStart,
			rangeEnd:   rangeEnd,
			chunkNo:    i,
			dealSize:   dealSize,
		}
		downloaders = append(downloaders, &d)

		// if some chunks have been partially downloaded - add their sizes to nBytesReceived.
		// that doesn't need to be done for the very first chunk as it's already included into the number
		if i == 0 {
			continue
		}
		st, err := os.Stat(d.chunkFile)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return &httpError{error: fmt.Errorf("failed to get stats of the chunk file: %w", err)}
		}
		if st != nil {
			t.nBytesReceived += st.Size()
		}
	}

	for {

		nBytesReceived := t.getBytesReceived()

		group := errgroup.Group{}

		// download chunks into temporary files
		for _, d := range downloaders {
			group.Go(d.doHttp)
		}

		var reqErr *httpError
		if err := group.Wait(); err != nil {
			reqErr = err.(*httpError)
		}

		if reqErr == nil {
			// append chunks one by one to the output file
			// * minimize space overhead by removing the chunk file once it has been appended ot the output successfully
			// * keep in mind restarts, resume writing from the correct chunk / offset
			t.dl.Infow(duuid, "http transfer completed successfully, joining chunks")

			for i := 0; i < len(downloaders); i++ {
				d := downloaders[i]

				if err := d.verify(); err != nil {
					return &httpError{error: err}
				}

				// this is the first chunk, no more job to do as it has already been written to the output file
				if d.chunkNo == 0 {
					continue
				}

				err := d.appendChunkToTheOutput()
				if err != nil {
					return &httpError{error: fmt.Errorf("failed to append chunk to the output: %w", err)}
				}
				err = os.Remove(d.chunkFile)
				if err != nil {
					return &httpError{error: fmt.Errorf("failed to remove the chunk file: %w", err)}
				}
			}
			// delete control file once all chunks have been merged in successfully
			err = os.Remove(controlFile)
			if err != nil {
				t.dl.Infow(duuid, "error deleteing control file %s: %w", controlFile, err)
			}
			// if there's no error, transfer was successful
			break
		}

		t.dl.Infow(duuid, "http request error", "http code", reqErr.code, "outputErr", reqErr.Error())

		// check if the error is a 4xx error, meaning there is a problem with
		// the request (eg 401 Unauthorized)
		if reqErr.code/100 == 4 {
			msg := fmt.Sprintf("terminating http request: received %d response from server", reqErr.code)
			t.dl.LogError(duuid, msg, reqErr)
			return reqErr.error
		}

		// do not resume transfer if context has been cancelled or if the context deadline has exceeded
		err := reqErr.error
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.dl.LogError(duuid, "terminating http transfer: context cancelled or deadline exceeded", err)
			return fmt.Errorf("transfer context canceled err: %w", err)
		}

		// If some data was transferred, reset the back-off count to zero
		if n := t.getBytesReceived(); n > nBytesReceived {
			t.dl.Infow(duuid, "some data was transferred before connection error, so resetting backoff to zero",
				"transferred", n-nBytesReceived)
			t.backoff.Reset()
		}

		// backoff-retry transfer if max number of attempts haven't been exhausted
		nAttempts := t.backoff.Attempt() + 1
		if nAttempts >= t.maxReconnectAttempts {
			t.dl.Errorw(duuid, "terminating http transfer: exhausted max attempts", "err", err.Error(), "maxAttempts", t.maxReconnectAttempts)
			return fmt.Errorf("could not finish transfer even after %.0f attempts, lastErr: %w", t.maxReconnectAttempts, err)
		}
		duration := t.backoff.Duration()
		bt := time.NewTimer(duration)
		t.dl.Infow(duuid, "backing off before retrying http request", "backoff time", duration.String(),
			"attempts", nAttempts)
		defer func() {
			_ = bt.Stop()
		}()
		select {
		case <-bt.C:
			t.dl.Infow(duuid, "back-off complete, retrying http request", "backoff time", duration.String())
		case <-ctx.Done():
			t.dl.LogError(duuid, "did not retry http request: context cancelled", ctx.Err())
			return fmt.Errorf("transfer canceled after %.0f attempts to finish transfer, lastErr=%s, contextErr=%w", t.backoff.Attempt(), err, ctx.Err())
		}
	}

	// --- http request finished successfully. see if we got the number of bytes we expected.

	nBytesReceived := t.getBytesReceived()

	// if the number of bytes we've received is not the same as the deal size, we have a failure.
	if nBytesReceived != dealSize {
		return fmt.Errorf("mismatch in dealSize vs received bytes, dealSize=%d, received=%d", dealSize, nBytesReceived)
	}
	// if the file size is not equal to the number of bytes received, something has gone wrong
	st, err := os.Stat(t.dealInfo.OutputFile)
	if err != nil {
		return fmt.Errorf("failed to stat output file: %w", err)
	}
	if nBytesReceived != st.Size() {
		return fmt.Errorf("mismtach in output file size vs received bytes, fileSize=%d, receivedBytes=%d", st.Size(), nBytesReceived)
	}

	t.dl.Infow(duuid, "http request finished successfully", "nBytesReceived", nBytesReceived,
		"file size", st.Size())

	return nil
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

func (t *transfer) emitEvent(evt types.TransportEvent) {
	t.lastEvt = nil
	select {
	case t.eventCh <- evt:
	default:
		// If it wasn't possible to send the event because the channel is full,
		// save it so that we can ensure it gets sent before the channel is closed.
		// A new event always supersedes an older event, so if there is another
		// event after this one, it will simply over-write this one.
		t.lastEvt = &evt
	}
}

func (t *transfer) closeEventChannel(ctx context.Context) {
	// If there was an event that wasn't sent because the channel was full,
	// ensure that it gets sent before close
	if t.lastEvt != nil {
		select {
		case <-ctx.Done():
		case t.eventCh <- *t.lastEvt:
		}
	}
	close(t.eventCh)
}

type transferConfig struct {
	NChunks int
}

func (t *transfer) readControlFile(cf string) (*transferConfig, error) {
	input, err := os.OpenFile(cf, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening control file for read %s: %w", cf, err)
	}
	defer func() {
		_ = input.Close()
	}()

	data, err := io.ReadAll(input)
	if err != nil {
		return nil, fmt.Errorf("error reading control file %s: %w", cf, err)
	}
	var conf transferConfig
	err = json.Unmarshal(data, &conf)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling control file %s: %w", cf, err)
	}

	return &conf, nil
}

func (t *transfer) writeControlFile(cf string, conf transferConfig) error {
	output, err := os.OpenFile(cf, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening control file for write %s: %w", cf, err)
	}
	defer func() {
		_ = output.Close()
	}()

	data, err := json.Marshal(&conf)
	if err != nil {
		return fmt.Errorf("error marshalling transfer config: %w", err)
	}

	_, err = output.Write(data)
	if err != nil {
		return fmt.Errorf("error writing into control file %s: %w", cf, err)
	}

	return nil
}
