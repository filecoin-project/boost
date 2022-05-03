package retrieval

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/stores"
	"github.com/filecoin-project/lotus/markets/dagstore"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"golang.org/x/xerrors"
)

var log = logging.Logger("retrieval")

type HttpServer struct {
	path       string
	sa         dagstore.SectorAccessor
	pieceStore lotus_dtypes.ProviderPieceStore
	dagst      stores.DAGStoreWrapper

	ctx    context.Context
	cancel context.CancelFunc
	server *http.Server
}

func NewHttpServer(path string, sa dagstore.SectorAccessor, ps lotus_dtypes.ProviderPieceStore, dagst stores.DAGStoreWrapper) *HttpServer {
	return &HttpServer{path: path, sa: sa, pieceStore: ps, dagst: dagst}
}

func (s *HttpServer) pieceBasePath() string {
	return s.path + "/piece/"
}

const httpPort = 7777

func (s *HttpServer) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	listenAddr := fmt.Sprintf(":%d", httpPort)
	handler := http.NewServeMux()
	handler.HandleFunc(s.pieceBasePath(), s.handleByPieceCid)
	s.server = &http.Server{
		Addr:    listenAddr,
		Handler: handler,
		// This context will be the parent of the context associated with all
		// incoming requests
		BaseContext: func(listener net.Listener) context.Context {
			return s.ctx
		},
	}

	fmt.Printf("http retrieval server listening on %s\n", listenAddr)

	go func() {
		if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("http.ListenAndServe(): %v", err)
		}
	}()

	return nil
}

func (s *HttpServer) Stop() error {
	s.cancel()
	return s.server.Close()
}

func (s *HttpServer) handleByPieceCid(w http.ResponseWriter, r *http.Request) {
	prefixLen := len(s.pieceBasePath())
	if len(r.URL.Path) <= prefixLen {
		msg := fmt.Sprintf("path '%s' is missing piece CID", r.URL.Path)
		writeError(w, http.StatusBadRequest, msg)
		return
	}

	fileName := r.URL.Path[prefixLen:]
	pieceCidStr := strings.Replace(fileName, ".car", "", 1)
	pieceCid, err := cid.Parse(pieceCidStr)
	if err != nil {
		msg := fmt.Sprintf("parsing piece CID %s: %s", pieceCidStr, err.Error())
		writeError(w, http.StatusBadRequest, msg)
		return
	}

	// Set the Content-Type header explicitly so that http.ServeContent doesn't
	// try to do it implicitly
	w.Header().Set("Content-Type", "application/car")

	ctx := r.Context()
	content, err := s.getPieceContent(ctx, pieceCid)
	if err != nil {
		if xerrors.Is(err, ErrPieceSealed) {
			msg := fmt.Sprintf("no unsealed CAR file found for piece CID %s", pieceCidStr)
			writeError(w, http.StatusNotFound, msg)
			return
		}
		log.Errorw("getting content for piece", "pieceCid", pieceCid, "error", err)
		msg := fmt.Sprintf("server error getting content for piece CID %s", pieceCidStr)
		writeError(w, http.StatusInternalServerError, msg)
		return
	}

	if r.Method == "HEAD" {
		// For an HTTP HEAD request we don't send any data (just headers)
		http.ServeContent(w, r, "", time.Time{}, content)
		return
	}

	// Send the CAR file
	// http.ServeContent ignores errors when writing to the stream, so we
	// replace the writer with a class that watches for errors
	writeErrWatcher := &writeErrorWatcher{ResponseWriter: w, onError: func(e error) {
		err = e
	}}

	// Send the content
	http.ServeContent(writeErrWatcher, r, "", time.Time{}, content)

	// Check if there was an error during the transfer
	if err != nil {
		log.Infow("failed to send data to client", "error", err, "pieceCid", pieceCid)
		return
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	w.Write([]byte(msg)) //nolint:errcheck
}

func (s *HttpServer) getPieceContent(ctx context.Context, pieceCid cid.Cid) (io.ReadSeeker, error) {
	// Get the deals for the piece
	pieceInfo, err := s.pieceStore.GetPieceInfo(pieceCid)
	if err != nil {
		return nil, err
	}

	// Get the first unsealed deal
	di, err := s.unsealedDeal(ctx, pieceInfo)
	if err != nil {
		return nil, fmt.Errorf("getting CAR for piece %s: %w", pieceCid.String(), ErrPieceSealed)
	}

	// Get the raw piece data from the sector
	pieceReader, err := s.sa.UnsealSectorAt(ctx, di.SectorID, di.Offset.Unpadded(), di.Length.Unpadded())
	if err != nil {
		return nil, fmt.Errorf("getting raw data from sector %d: %w", err)
	}

	it, err := s.dagst.GetIterableIndexForPiece(pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting iterable index for piece %s: %w", pieceCid, err)
	}

	var maxOffset uint64
	var maxMultihash multihash.Multihash
	err = it.ForEach(func(mh multihash.Multihash, offset uint64) error {
		if offset > maxOffset {
			maxOffset = offset
			maxMultihash = mh
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("iterating over CAR index: %w", err)
	}

	_, err = pieceReader.Seek(int64(maxOffset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking to offset %d in CAR: %w", maxOffset, err)
	}

	// A section consists of
	// <size of cid+block><cid><block>

	// Get <size of cid+block>
	cr := &countReader{r: bufio.NewReader(pieceReader)}
	dataLength, err := varint.ReadUvarint(cr)
	if err != nil {
		return nil, fmt.Errorf("reading CAR section length: %w", err)
	}

	// The number of bytes in the uvarint that records <size of cid+block>
	dataLengthUvarSize := cr.count

	// Get the size of the (unpadded) CAR file
	unpaddedCarSize := maxOffset + dataLengthUvarSize + dataLength

	// Seek to the end of the CAR to get its (padded) size
	paddedCarSize, err := pieceReader.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seeking to end of CAR: %w", err)
	}

	// Seek back to the start of the CAR
	_, err = pieceReader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking to start of CAR: %w", err)
	}

	lr := &limitSeekReader{
		Reader:       io.LimitReader(pieceReader, int64(unpaddedCarSize)),
		readSeeker:   pieceReader,
		unpaddedSize: int64(unpaddedCarSize),
		paddedSize:   paddedCarSize,
	}
	return lr, nil
}

type limitSeekReader struct {
	io.Reader
	readSeeker   io.ReadSeeker
	unpaddedSize int64
	paddedSize   int64
}

var _ io.ReadSeeker = (*limitSeekReader)(nil)

func (l *limitSeekReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekEnd {
		offset -= (l.paddedSize - l.unpaddedSize)
	}
	return l.readSeeker.Seek(offset, whence)
}

var ErrPieceSealed = errors.New("piece is in a sealed sector")

func (s *HttpServer) unsealedDeal(ctx context.Context, pieceInfo piecestore.PieceInfo) (*piecestore.DealInfo, error) {
	// Find the first unsealed deal
	for _, di := range pieceInfo.Deals {
		isUnsealed, err := s.sa.IsUnsealed(ctx, di.SectorID, di.Offset.Unpadded(), di.Length.Unpadded())
		if err != nil {
			return nil, err
		}
		if isUnsealed {
			return &di, nil
		}
	}

	return nil, ErrPieceSealed
}

// writeErrorWatcher calls onError if there is an error writing to the writer
type writeErrorWatcher struct {
	http.ResponseWriter
	onError func(err error)
}

func (w *writeErrorWatcher) Write(bz []byte) (int, error) {
	count, err := w.ResponseWriter.Write(bz)
	if err != nil {
		w.onError(err)
	}
	return count, err
}

// countReader just counts the number of bytes read
type countReader struct {
	r     *bufio.Reader
	count uint64
}

func (c *countReader) ReadByte() (byte, error) {
	b, err := c.r.ReadByte()
	if err == nil {
		c.count++
	}
	return b, err
}
