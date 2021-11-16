package httptransport

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"

	"github.com/filecoin-project/boost/transport/types/transferstatus"
	logging "github.com/ipfs/go-log/v2"

	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"

	"github.com/filecoin-project/boost/transport/httptransport/pb"
	"github.com/filecoin-project/boost/transport/types"
	"github.com/gogo/protobuf/proto"

	"github.com/filecoin-project/boost/transport"
)

var log = logging.Logger("http-transport")

const (
	readBufferSize = 4096
)

var _ transport.Transport = (*httpTransport)(nil)

type httpTransport struct {
	// TODO Construct and inject an http client here ?
}

func (h *httpTransport) Execute(ctx context.Context, transportInfo []byte, dealInfo *types.TransportDealInfo) (th *transport.TransportHandler, isComplete bool, err error) {
	tctx, cancel := context.WithCancel(ctx)

	// de-serialize transport opaque token
	tInfo := &pb.HttpReq{}
	if err := proto.Unmarshal(transportInfo, tInfo); err != nil {
		return nil, false, fmt.Errorf("failed to de-serialize transport info bytes: %w", err)
	}
	// check that the outputFile exists
	fi, err := os.Stat(dealInfo.OutputFile)
	if err != nil {
		return nil, false, fmt.Errorf("output file state error: %w", err)
	}
	// validate req
	if _, err := url.Parse(tInfo.URL); err != nil {
		return nil, false, fmt.Errorf("failed to parse url: %w", err)
	}
	// do we have more bytes than required already ?
	fileSize := fi.Size()
	if fileSize > dealInfo.DealSize {
		return nil, false, fmt.Errorf("deal size=%d but file size=%d", dealInfo.DealSize, fileSize)
	}

	// is the transfer already complete ? we check this by comparing the number of bytes
	// in the output file with the deal size.
	if fileSize == dealInfo.DealSize {
		return nil, true, nil
	}

	pub, sub, err := createPubSub(eventbus.NewBus())
	if err != nil {
		return nil, false, fmt.Errorf("failed to create pub-sub: %w", err)
	}
	// emit event with initial state
	if err := pub.Emit(types.TransportEvent{
		Status:         transferstatus.Initiated,
		NBytesReceived: fileSize,
	}); err != nil {
		log.Errorw("failed to publish transport event", "dealUuid", dealInfo.DealUuid, "err", err)
	}

	var wg sync.WaitGroup
	// start executing the transfer
	t := &transfer{
		tInfo:          tInfo,
		dealInfo:       dealInfo,
		pub:            pub,
		nBytesReceived: fileSize,
		wg:             wg,
	}

	t.wg.Add(1)
	go t.execute(tctx)
	return transport.NewHandler(tctx, cancel, dealInfo, sub, wg), false, nil
}

type transfer struct {
	tInfo    *pb.HttpReq
	dealInfo *types.TransportDealInfo
	pub      event.Emitter
	wg       sync.WaitGroup

	nBytesReceived int64
}

func (t *transfer) execute(ctx context.Context) {
	defer t.wg.Done()

	failTransfer := func(err error) {
		if err := t.pub.Emit(types.TransportEvent{
			Status:          transferstatus.Errored,
			IsTerminalState: true,
			Error:           err,
		}); err != nil {
			log.Errorw("failed to publish transport error", "dealUuid", t.dealInfo.DealUuid, "err", err)
		}
	}

	// construct request
	req, err := http.NewRequest("GET", t.tInfo.URL, nil)
	if err != nil {
		failTransfer(fmt.Errorf("failed to create http req: %w", err))
		return
	}

	// add range req to start reading from the last byte we have in the output file
	fi, err := os.Stat(t.dealInfo.OutputFile)
	if err != nil {
		failTransfer(fmt.Errorf("failed to stat output file: %w", err))
		return
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", fi.Size()))
	// init the request with the transfer context
	req = req.WithContext(ctx)
	// open output file in append-only mode for writing
	of, err := os.OpenFile(t.dealInfo.OutputFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		failTransfer(fmt.Errorf("failed to open output file: %w", err))
		return
	}

	// start the http transfer
	remaining := t.dealInfo.DealSize - fi.Size()
	if err := t.doHttp(req, of, remaining); err != nil {
		// do not resume transfer if context has been cancelled
		if ctx.Err() == context.Canceled || ctx.Err() == context.DeadlineExceeded {
			if err := t.pub.Emit(types.TransportEvent{
				Status:          transferstatus.Cancelled,
				IsTerminalState: true,
				Error:           fmt.Errorf("transfer context err: %w", ctx.Err()),
			}); err != nil {
				log.Errorw("failed to publish transport error", "dealUuid", t.dealInfo.DealUuid, "err", err)
			}
			return
		}

		// TODO: resumption
		return
	}

	// --- http request finished successfully. see if we got the number of bytes we expected.

	// if the number of bytes we've received is not the same as the deal size, we have a failure.
	if t.nBytesReceived != t.dealInfo.DealSize {
		failTransfer(fmt.Errorf("mismatch in dealSize vs received bytes, dealSize=%d, received=%d", t.dealInfo.DealSize, t.nBytesReceived))
		return
	}

	// otherwise we're good ! notify the subscriber.
	if err := t.pub.Emit(types.TransportEvent{
		Status:          transferstatus.Finished,
		NBytesReceived:  t.nBytesReceived,
		IsTerminalState: true,
	}); err != nil {
		log.Errorw("failed to publish transport event", "dealUuid", t.dealInfo.DealUuid, "err", err)
	}
}

func (t *transfer) doHttp(req *http.Request, dst io.Writer, toRead int64) error {
	// send http request and validate response
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send  http req: %w", err)
	}
	// we should either get back a 200 or a 206 -> anything else means something has gone wrong and we return an error.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close() // nolint
		return fmt.Errorf("http req failed: %d", resp.StatusCode)
	}
	defer resp.Body.Close()

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
				return fmt.Errorf("read-write mismatch, read=%d, written=%d", nr, nw)
			}
			t.nBytesReceived = t.nBytesReceived + int64(nw)
		}
		// the http stream we're reading from has sent us an EOF, nothing to do here.
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("error reading from http response stream: %w", err)
		}

		// emit event updating the number of bytes received
		if err := t.pub.Emit(types.TransportEvent{
			Status:         transferstatus.DataReceived,
			NBytesReceived: t.nBytesReceived,
		}); err != nil {
			log.Errorw("failed to publish transport event", "dealUuid", t.dealInfo.DealUuid, "err", err)
		}
	}
}

func createPubSub(bus event.Bus) (event.Emitter, event.Subscription, error) {
	emitter, err := bus.Emitter(&types.TransportEvent{}, eventbus.Stateful)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create event emitter: %w", err)
	}
	sub, err := bus.Subscribe(new(types.TransportEvent), eventbus.BufSize(256))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create subscriber: %w", err)
	}

	return emitter, sub, nil
}
