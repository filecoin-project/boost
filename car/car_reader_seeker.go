package car

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
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

	reader          *io.PipeReader
	writer          atomic.Value
	writeCompleteCh chan struct{}
}

var _ io.ReadSeeker = (*CarReaderSeeker)(nil)

func NewCarReaderSeeker(ctx context.Context, cow *CarOffsetWriter, size uint64) *CarReaderSeeker {
	ctx, cancel := context.WithCancel(ctx)
	return &CarReaderSeeker{
		ctx:             ctx,
		cancel:          cancel,
		size:            size,
		cow:             cow,
		writeCompleteCh: make(chan struct{}),
	}
}

// Read reads data into the buffer.
// Not thread-safe to call concurrently with Seek or another Read.
// Thread-safe to call with Cancel.
func (c *CarReaderSeeker) Read(p []byte) (int, error) {
	// Check if the CarReadSeeker has been cancelled
	if c.ctx.Err() != nil {
		return 0, c.ctx.Err()
	}

	// Check if the offset is at the end of the file
	if uint64(c.offset) >= c.size {
		// If the offset is exactly at the end of the file just return EOF
		if uint64(c.offset) == c.size {
			return 0, io.EOF
		}
		// Otherwise it's an error
		return 0, fmt.Errorf("cannot read from offset %d >= file size %d", c.offset, c.size)
	}

	// Check if there's already a write in progress
	if c.writer.Load() == nil {
		// No write in progress, start a new write from the current offset
		// in a go routine
		pr, pw := io.Pipe()
		c.reader = pr
		c.writer.Store(pw)

		offset := c.offset
		go func() {
			err := c.cow.Write(c.ctx, pw, uint64(offset))
			pw.CloseWithError(err) //nolint:errcheck
			close(c.writeCompleteCh)
		}()
	}

	count, err := c.reader.Read(p)
	c.offset += int64(count)
	return count, err
}

// Cancel aborts any read operation: Once Cancel returns, all subsequent calls
// to Read() will return context.Canceled
// Thread-safe to call concurrently with Read.
func (c *CarReaderSeeker) Cancel(ctx context.Context) error {
	// Cancel the context
	c.cancel()

	// return if there is no write in progress
	if c.writer.Load() == nil {
		return nil
	}

	pw := c.writer.Load().(*io.PipeWriter)
	pw.CloseWithError(context.Canceled) //nolint:errcheck

	// Wait for the write to complete
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.writeCompleteCh:
	}
	return nil
}

// Seek changes the offset into the stream.
// Not thread-safe to call concurrently with Read, Cancel or another Seek.
// Should only be called once in the beginning before any Read call.
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

	return c.offset, nil
}
