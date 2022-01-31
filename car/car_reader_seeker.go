package car

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// CarReaderSeeker wraps CarOffsetWriter with a ReadSeeker implementation.
type CarReaderSeeker struct {
	parentCtx context.Context
	size      uint64
	offset    int64
	cow       *CarOffsetWriter // 🐮

	lk            sync.Mutex
	reader        *io.PipeReader
	writer        *io.PipeWriter
	writeCancel   context.CancelFunc
	writeComplete chan struct{}
}

var _ io.ReadSeekCloser = (*CarReaderSeeker)(nil)

func NewCarReaderSeeker(ctx context.Context, cow *CarOffsetWriter, size uint64) *CarReaderSeeker {
	return &CarReaderSeeker{
		parentCtx:     ctx,
		size:          size,
		cow:           cow,
		writeComplete: make(chan struct{}, 1),
	}
}

func (c *CarReaderSeeker) Read(p []byte) (n int, err error) {
	c.lk.Lock()
	defer c.lk.Unlock()

	if uint64(c.offset) >= c.size {
		return 0, fmt.Errorf("cannot read from offset %d >= file size %d", c.offset, c.size)
	}

	// Check if there's already a write in progress
	if c.reader == nil {
		// No write in progress, start a new write from the current offset
		// in a go routine
		writeCtx, writeCancel := context.WithCancel(c.parentCtx)
		c.writeCancel = writeCancel
		pr, pw := io.Pipe()
		c.reader = pr
		c.writer = pw

		go func() {
			err := c.cow.Write(writeCtx, pw, uint64(c.offset))
			if err != nil {
				pw.CloseWithError(err) //nolint:errcheck
			} else {
				pw.Close() //nolint:errcheck
			}
			c.writeComplete <- struct{}{}
		}()
	}

	return c.reader.Read(p)
}

func (c *CarReaderSeeker) Seek(offset int64, whence int) (int64, error) {
	c.lk.Lock()
	defer c.lk.Unlock()

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

	// Cancel any ongoing write and wait for it to complete
	if c.reader != nil {
		c.writeCancel()

		c.reader.Close() //nolint:errcheck

		select {
		case <-c.parentCtx.Done():
			return 0, c.parentCtx.Err()
		case <-c.writeComplete:
		}

		c.reader = nil
	}

	return c.offset, nil
}

func (c *CarReaderSeeker) Close() error {
	c.lk.Lock()
	defer c.lk.Unlock()

	if c.writer != nil {
		return c.writer.Close()
	}
	return nil
}

func (c *CarReaderSeeker) CloseWithError(err error) error {
	c.lk.Lock()
	defer c.lk.Unlock()

	if c.writer != nil {
		return c.writer.CloseWithError(err)
	}
	return nil
}
