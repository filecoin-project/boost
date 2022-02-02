package car

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// CarReaderSeeker wraps CarOffsetWriter with a ReadSeeker implementation.
// Note that the Read and Seek methods are not thread-safe. They must not
// be called concurrently with each other, but may be called concurrently
// with Cancel.
type CarReaderSeeker struct {
	ctx    context.Context
	cancel context.CancelFunc
	size   uint64
	offset int64
	cow    *CarOffsetWriter // 🐮

	lk              sync.Mutex
	reader          *io.PipeReader
	writer          *io.PipeWriter
	writeCompleteCh chan struct{}
}

var _ io.ReadSeeker = (*CarReaderSeeker)(nil)

func NewCarReaderSeeker(ctx context.Context, cow *CarOffsetWriter, size uint64) *CarReaderSeeker {
	ctx, cancel := context.WithCancel(ctx)
	return &CarReaderSeeker{
		ctx:    ctx,
		cancel: cancel,
		size:   size,
		cow:    cow,
	}
}

// Read reads data into the buffer. Not thread-safe to call concurrently with Seek.
func (c *CarReaderSeeker) Read(p []byte) (int, error) {
	c.lk.Lock()

	// Check if the CarReadSeeker has been cancelled
	if c.ctx.Err() != nil {
		c.lk.Unlock()
		return 0, c.ctx.Err()
	}

	// Check if the offset is at the end of the file
	if uint64(c.offset) >= c.size {
		defer c.lk.Unlock()

		// If the offset is exactly at the end of the file just return EOF
		if uint64(c.offset) == c.size {
			return 0, io.EOF
		}
		// Otherwise it's an error
		return 0, fmt.Errorf("cannot read from offset %d >= file size %d", c.offset, c.size)
	}

	// Check if there's already a write in progress
	if c.writeCompleteCh == nil {
		// No write in progress, start a new write from the current offset
		// in a go routine
		c.writeCompleteCh = make(chan struct{})

		pr, pw := io.Pipe()
		c.reader = pr
		c.writer = pw

		offset := c.offset
		go func() {
			err := c.cow.Write(c.ctx, pw, uint64(offset))
			pw.CloseWithError(err) //nolint:errcheck

			c.lk.Lock()
			defer c.lk.Unlock()

			// Reset and close the write complete channel
			writeCompleteCh := c.writeCompleteCh
			c.writeCompleteCh = nil
			close(writeCompleteCh)
		}()
	}

	// Don't hold the lock while reading as Read may block
	c.lk.Unlock()

	count, err := c.reader.Read(p)
	c.offset += int64(count)
	return count, err
}

// Seek changes the offset into the stream. Not thread-safe to call concurrently with Read.
func (c *CarReaderSeeker) Seek(offset int64, whence int) (int64, error) {
	// Update the offset
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return 0, fmt.Errorf("invalid offset %d from start: must be positive", offset)
		}
		c.offset = offset
	case io.SeekCurrent:
		if c.offset+offset < 0 {
			return 0, fmt.Errorf("invalid offset %d from current %d: resulting offset is negative", offset, c.offset)
		}
		c.offset += offset
	case io.SeekEnd:
		if int64(c.size)+offset < 0 {
			return 0, fmt.Errorf("invalid offset %d from end: larger than total size %d", offset, c.size)
		}
		c.offset = int64(c.size) + offset
	}

	c.lk.Lock()
	if c.writeCompleteCh == nil {
		// No ongoing write so we can return immediately
		c.lk.Unlock()
		return c.offset, nil
	}

	// There is an ongoing write, so close the pipe and wait for the write to
	// complete before returning. This is so that subsequent reads will be from
	// the updated offset.
	// Note: Closing the reader will cause any subsequent reads / writes on
	// that pipe to fail.
	c.reader.Close() //nolint:errcheck

	// Release the lock while waiting for write to complete
	writeCompleteCh := c.writeCompleteCh
	c.lk.Unlock()

	select {
	case <-c.ctx.Done():
		return 0, c.ctx.Err()
	case <-writeCompleteCh:
	}

	return c.offset, nil
}

// Cancel aborts any read operation: Once Cancel returns, all subsequent calls
// to Read() will return context.Canceled
func (c *CarReaderSeeker) Cancel(ctx context.Context) error {
	c.lk.Lock()

	// Cancel the context
	c.cancel()

	// Check if there's an ongoing write
	writeCompleteCh := c.writeCompleteCh
	if writeCompleteCh != nil {
		// Close the writer. This will cause any subsequent reads to fail.
		c.writer.CloseWithError(context.Canceled) //nolint:errcheck
	}

	c.lk.Unlock()

	// Wait for the write to complete
	if writeCompleteCh != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-writeCompleteCh:
		}
	}
	return nil
}
