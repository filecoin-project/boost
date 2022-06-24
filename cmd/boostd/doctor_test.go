package main

import (
	"bufio"
	"context"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/fr32"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/lotus/metrics"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats"
	"golang.org/x/xerrors"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

var tlog = logging.Logger("testread")

func TestRead(t *testing.T) {
	logging.SetLogLevel("testread", "INFO")
	//ctx := context.Background()
	//var pc cid.Cid
	//sector := storage.SectorRef{ID: abi.SectorID{Number: 1}}
	//pieceOffset := storiface.UnpaddedByteIndex(0)
	padded := abi.PaddedPieceSize(34359738368)
	size := padded.Unpadded()
	tlog.Infof("Opening file size: %d (padded: %d)", size, padded)

	////fileName := "/Users/dirk/dev/tmp/unsealed-test/s-t01000-1"
	////fileName := "/Users/dirk/Downloads/s-t01278-1424"
	//fileName := "/tmp/sofiaminer/s-t0127896-741"
	//file, err := os.Open(fileName)
	//require.NoError(t, err)
	//defer file.Close()
	//
	//tlog.Infof("Unpadding file %s", fileName)
	//buf := make([]byte, fr32.BufSize(size.Padded()))
	//upr, err := fr32.NewUnpadReaderBuf(file, size.Padded(), buf)
	//require.NoError(t, err)
	//
	//outFile := "/tmp/unsealed.sofiafile.unpadded"
	outFile := "/tmp/unsealed.sofiafile.unpadded"
	//err = writeFile(upr, outFile)
	////reader, err := unpadReader(ctx, file, pc, sector, pieceOffset, size)
	////require.NoError(t, err)
	//tlog.Infof("Wrote unpadded file to %s", outFile)

	tlog.Infof("Generating index %s", outFile)
	start := time.Now()
	reader, err := os.Open(outFile)
	require.NoError(t, err)
	defer reader.Close()

	idx, err := car.ReadOrGenerateIndex(reader, car.ZeroLengthSectionAsEOF(true), car.StoreIdentityCIDs(true))
	require.NoError(t, err)
	tlog.Infof("Generated index in %s", time.Since(start))

	it, ok := idx.(index.IterableIndex)
	if !ok {
		require.Fail(t, "index is not iterable")
	}

	it.ForEach(func(multihash multihash.Multihash, u uint64) error {
		tlog.Infof("%s: %d", multihash, u)
		return nil
	})
}

func writeFile(upr io.Reader, outFile string) error {
	fo, err := os.Create(outFile)
	if err != nil {
		return err
	}
	defer fo.Close()

	outbuf := make([]byte, 8*1024*1024)
	for {
		// read a chunk
		n, err := upr.Read(outbuf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		// write a chunk
		if _, err := fo.Write(outbuf[:n]); err != nil {
			return err
		}
	}

	return nil
}

func unpadReader(ctx context.Context, reader *os.File, pc cid.Cid, sector storage.SectorRef, pieceOffset storiface.UnpaddedByteIndex, size abi.UnpaddedPieceSize) (io.ReadSeeker, error) {
	ctx, cancel := context.WithCancel(ctx)
	buf := make([]byte, fr32.BufSize(size.Padded()))

	pr, err := (&pieceReader{
		ctx: ctx,
		getReader: func(ctx context.Context, startOffset uint64) (io.ReadCloser, error) {
			startOffsetAligned := storiface.UnpaddedByteIndex(startOffset / 127 * 127) // floor to multiple of 127

			//r, err := rg(startOffsetAligned.Padded())
			//if err != nil {
			//	return nil, xerrors.Errorf("getting reader at +%d: %w", startOffsetAligned, err)
			//}
			r := reader

			upr, err := fr32.NewUnpadReaderBuf(r, size.Padded(), buf)
			if err != nil {
				//r.Close() // nolint
				return nil, xerrors.Errorf("creating unpadded reader: %w", err)
			}

			bir := bufio.NewReaderSize(upr, 127)
			if startOffset > uint64(startOffsetAligned) {
				if _, err := bir.Discard(int(startOffset - uint64(startOffsetAligned))); err != nil {
					//r.Close() // nolint
					return nil, xerrors.Errorf("discarding bytes for startOffset: %w", err)
				}
			}

			return struct {
				io.Reader
				io.Closer
			}{
				Reader: bir,
				Closer: funcCloser(func() error {
					//return r.Close()
					return nil
				}),
			}, nil
		},
		len:      size,
		onClose:  cancel,
		pieceCid: pc,
	}).init()
	if err != nil || pr == nil { // pr == nil to make sure we don't return typed nil
		cancel()
		return nil, err
	}

	return pr, err
}

type funcCloser func() error

func (f funcCloser) Close() error {
	return f()
}

var _ io.Closer = funcCloser(nil)

// For small read skips, it's faster to "burn" some bytes than to setup new sector reader.
// Assuming 1ms stream seek latency, and 1G/s stream rate, we're willing to discard up to 1 MiB.
var MaxPieceReaderBurnBytes int64 = 1 << 20 // 1M
var ReadBuf = 128 * (127 * 8)               // unpadded(128k)

type pieceGetter func(ctx context.Context, offset uint64) (io.ReadCloser, error)

type pieceReader struct {
	ctx       context.Context
	getReader pieceGetter
	pieceCid  cid.Cid
	len       abi.UnpaddedPieceSize
	onClose   context.CancelFunc

	closed bool
	seqAt  int64 // next byte to be read by io.Reader

	mu  sync.Mutex
	r   io.ReadCloser
	br  *bufio.Reader
	rAt int64
}

func (p *pieceReader) init() (_ *pieceReader, err error) {
	stats.Record(p.ctx, metrics.DagStorePRInitCount.M(1))

	p.rAt = 0
	p.r, err = p.getReader(p.ctx, uint64(p.rAt))
	if err != nil {
		return nil, err
	}
	if p.r == nil {
		return nil, nil
	}

	p.br = bufio.NewReaderSize(p.r, ReadBuf)

	return p, nil
}

func (p *pieceReader) check() error {
	if p.closed {
		return xerrors.Errorf("reader closed")
	}

	return nil
}

func (p *pieceReader) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.check(); err != nil {
		return err
	}

	if p.r != nil {
		if err := p.r.Close(); err != nil {
			return err
		}
		if err := p.r.Close(); err != nil {
			return err
		}
		p.r = nil
	}

	p.onClose()

	p.closed = true

	return nil
}

func (p *pieceReader) Read(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.check(); err != nil {
		return 0, err
	}

	n, err := p.readAtUnlocked(b, p.seqAt)
	p.seqAt += int64(n)
	return n, err
}

func (p *pieceReader) Seek(offset int64, whence int) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.check(); err != nil {
		return 0, err
	}

	switch whence {
	case io.SeekStart:
		p.seqAt = offset
	case io.SeekCurrent:
		p.seqAt += offset
	case io.SeekEnd:
		p.seqAt = int64(p.len) + offset
	default:
		return 0, xerrors.Errorf("bad whence")
	}

	return p.seqAt, nil
}

func (p *pieceReader) ReadAt(b []byte, off int64) (n int, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.readAtUnlocked(b, off)
}

func (p *pieceReader) readAtUnlocked(b []byte, off int64) (n int, err error) {
	if err := p.check(); err != nil {
		return 0, err
	}

	stats.Record(p.ctx, metrics.DagStorePRBytesRequested.M(int64(len(b))))

	// 1. Get the backing reader into the correct position

	// if the backing reader is ahead of the offset we want, or more than
	//  MaxPieceReaderBurnBytes behind, reset the reader
	if p.r == nil || p.rAt > off || p.rAt+MaxPieceReaderBurnBytes < off {
		if p.r != nil {
			if err := p.r.Close(); err != nil {
				return 0, xerrors.Errorf("closing backing reader: %w", err)
			}
			p.r = nil
			p.br = nil
		}

		log.Debugw("pieceReader new stream", "piece", p.pieceCid, "at", p.rAt, "off", off-p.rAt, "n", len(b))

		if off > p.rAt {
			stats.Record(p.ctx, metrics.DagStorePRSeekForwardBytes.M(off-p.rAt), metrics.DagStorePRSeekForwardCount.M(1))
		} else {
			stats.Record(p.ctx, metrics.DagStorePRSeekBackBytes.M(p.rAt-off), metrics.DagStorePRSeekBackCount.M(1))
		}

		p.rAt = off
		p.r, err = p.getReader(p.ctx, uint64(p.rAt))
		p.br = bufio.NewReaderSize(p.r, ReadBuf)
		if err != nil {
			return 0, xerrors.Errorf("getting backing reader: %w", err)
		}
	}

	// 2. Check if we need to burn some bytes
	if off > p.rAt {
		stats.Record(p.ctx, metrics.DagStorePRBytesDiscarded.M(off-p.rAt), metrics.DagStorePRDiscardCount.M(1))

		n, err := io.CopyN(io.Discard, p.br, off-p.rAt)
		p.rAt += n
		if err != nil {
			return 0, xerrors.Errorf("discarding read gap: %w", err)
		}
	}

	// 3. Sanity check
	if off != p.rAt {
		return 0, xerrors.Errorf("bad reader offset; requested %d; at %d", off, p.rAt)
	}

	// 4. Read!
	n, err = io.ReadFull(p.br, b)
	if n < len(b) {
		log.Debugw("pieceReader short read", "piece", p.pieceCid, "at", p.rAt, "toEnd", int64(p.len)-p.rAt, "n", len(b), "read", n, "err", err)
	}
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}

	p.rAt += int64(n)
	return n, err
}
