package httptransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

type downloader struct {
	ctx        context.Context
	transfer   *transfer
	outputFile string
	chunkFile  string
	rangeStart int64
	rangeEnd   int64
	chunkNo    int
	dealSize   int64
}

func (d *downloader) doHttp() error {
	// construct request
	req, err := http.NewRequest("GET", d.transfer.tInfo.URL, nil)
	if err != nil {
		return &httpError{error: fmt.Errorf("failed to create http req: %w", err)}
	}

	// add request headers
	for name, val := range d.transfer.tInfo.Headers {
		req.Header.Set(name, val)
	}

	// calculate range boundaries taking into account bytes that have been already downloaded
	fi, err := os.Stat(d.chunkFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return &httpError{error: fmt.Errorf("failed to read size of chunk file %s: %w", d.chunkFile, err)}
	}
	var offset int64
	if fi != nil {
		offset = fi.Size()
	}

	rangeStart := d.rangeStart + offset

	// the file might have been already fully downloaded, nothing to do here
	if rangeStart >= d.rangeEnd {
		return nil
	}

	toRead := d.rangeEnd - rangeStart

	// add range req to start reading from the last byte we have in the output file
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", rangeStart, d.rangeEnd))
	// init the request with the transfer context
	req = req.WithContext(d.ctx)

	duid := d.transfer.dealInfo.DealUuid
	d.transfer.dl.Infow(duid, "sending http request", "toRead", toRead, "range-rq", req.Header.Get("Range"))

	// send http request and validate response
	resp, err := d.transfer.client.Do(req)
	if err != nil {
		return &httpError{error: fmt.Errorf("failed to send http req: %w", err)}
	}
	// we should either get back a 200 or a 206 -> anything else means something has gone wrong and we return an error.
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return &httpError{
			error: fmt.Errorf("http req failed: code: %d, status: %s", resp.StatusCode, resp.Status),
			code:  resp.StatusCode,
		}
	}

	// create chunk file if it doesn't exist
	of, err := os.OpenFile(d.chunkFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return &httpError{error: fmt.Errorf("failed to open chunk file %s for write: %w", d.chunkFile, err)}
	}
	defer func() {
		_ = of.Close()
	}()

	//  start reading the response stream `readBufferSize` at a time using a limit reader so we only read as many bytes as we need to.
	buf := make([]byte, readBufferSize)
	limitR := io.LimitReader(resp.Body, toRead)
	var chunkBytesReceived int64
	for {
		if d.ctx.Err() != nil {
			d.transfer.dl.LogError(duid, "stopped reading http response: context canceled", d.ctx.Err())
			return &httpError{error: d.ctx.Err()}
		}
		nr, readErr := limitR.Read(buf)

		// if we read more than zero bytes, write whatever read.
		if nr > 0 {
			nw, writeErr := of.Write(buf[0:nr])

			// if the number of read and written bytes don't match -> something has gone wrong, abort the http req.
			if nw < 0 || nr != nw {
				if writeErr != nil {
					return &httpError{error: fmt.Errorf("failed to write the chunk file %s: %w", d.chunkFile, writeErr)}
				}
				return &httpError{error: fmt.Errorf("read-write mismatch writing to the chunk file %s: read=%d, written=%d", d.chunkFile, nr, nw)}
			}
			chunkBytesReceived += int64(nw)
			d.transfer.addBytesReceived(int64(nw))
		}
		// the http stream we're reading from has sent us an EOF, nothing to do here.
		if readErr == io.EOF {
			d.transfer.dl.Infow(duid, "http server sent EOF", "toRead", toRead, "received", chunkBytesReceived, "deal-size", d.dealSize)
			return nil
		}
		if readErr != nil {
			return &httpError{error: fmt.Errorf("error reading from http response stream: %w", readErr)}
		}
	}
}

func (d *downloader) appendChunkToTheOutput() error {
	chunk, err := os.OpenFile(d.chunkFile, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening chunk file %s: %w", d.chunkFile, err)
	}
	defer func() {
		_ = chunk.Close()
	}()

	output, err := os.OpenFile(d.outputFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening output file %s: %w", d.outputFile, err)
	}
	defer func() {
		_ = output.Close()
	}()

	outputStats, err := output.Stat()
	if err != nil {
		return fmt.Errorf("error getting output file stats %s: %w", d.outputFile, err)
	}

	outputSize := outputStats.Size()

	if outputSize < d.rangeStart {
		return fmt.Errorf("output file does not have enough bytes for the chunk to be written into it: chunkFile: %s, outputSize: %d rangeStart: %d", d.chunkFile, outputSize, d.rangeStart)
	}

	// the chunk must have been already appended to the output - nothing to do here
	if outputSize >= d.rangeEnd {
		return nil
	}

	// move the write cursor in the case if appending chunk has failed half way through
	offset := outputSize - d.rangeStart

	_, err = chunk.Seek(offset, 0)
	if err != nil {
		return fmt.Errorf("error setting chunk file offset %s: %w", d.chunkFile, err)
	}

	buf := make([]byte, readBufferSize)
	reader := io.Reader(chunk)
	for {
		if d.ctx.Err() != nil {
			return d.ctx.Err()
		}
		nr, readErr := reader.Read(buf)

		if nr > 0 {
			nw, writeErr := output.Write(buf[0:nr])

			if nw < 0 || nr != nw {
				if writeErr != nil {
					return fmt.Errorf("failed to write to the output file from the chunk %s: %w", d.chunkFile, writeErr)
				}
				return fmt.Errorf("read-write mismatch writing to the output file from the chunk %s: read=%d, written=%d", d.chunkFile, nr, nw)
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func (d *downloader) verify() error {
	chunk, err := os.OpenFile(d.chunkFile, os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening chunk file %s: %w", d.chunkFile, err)
	}
	defer func() {
		_ = chunk.Close()
	}()

	chunkStats, err := chunk.Stat()
	if err != nil {
		return fmt.Errorf("error getting chunk file stats %s: %w", d.chunkFile, err)
	}

	expSize := d.rangeEnd - d.rangeStart
	if expSize != chunkStats.Size() {
		return fmt.Errorf("incomplete chunk file %s: expected: %d actual: %d", d.chunkFile, expSize, chunkStats.Size())
	}

	return nil
}
