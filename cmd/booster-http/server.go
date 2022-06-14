package main

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

	"github.com/fatih/color"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/multiformats/go-varint"
)

var ErrNotFound = errors.New("not found")

type HttpServer struct {
	path string
	port int
	api  HttpServerApi

	ctx    context.Context
	cancel context.CancelFunc
	server *http.Server
}

type HttpServerApi interface {
	GetMaxPieceOffset(pieceCid cid.Cid) (uint64, error)
	GetPieceInfo(pieceCID cid.Cid) (*piecestore.PieceInfo, error)
	IsUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error)
	UnsealSectorAt(ctx context.Context, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error)
}

func NewHttpServer(path string, port int, api HttpServerApi) *HttpServer {
	return &HttpServer{path: path, port: port, api: api}
}

func (s *HttpServer) pieceBasePath() string {
	return s.path + "/piece/"
}

func (s *HttpServer) Start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)

	listenAddr := fmt.Sprintf(":%d", s.port)
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

	go func() {
		if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("http.ListenAndServe(): %v", err)
		}
	}()
}

func (s *HttpServer) Stop() error {
	s.cancel()
	return s.server.Close()
}

func (s *HttpServer) handleByPieceCid(w http.ResponseWriter, r *http.Request) {
	prefixLen := len(s.pieceBasePath())
	if len(r.URL.Path) <= prefixLen {
		msg := fmt.Sprintf("path '%s' is missing piece CID", r.URL.Path)
		writeError(w, r, http.StatusBadRequest, msg)
		return
	}

	fileName := r.URL.Path[prefixLen:]
	pieceCidStr := strings.Replace(fileName, ".car", "", 1)
	pieceCid, err := cid.Parse(pieceCidStr)
	if err != nil {
		msg := fmt.Sprintf("parsing piece CID %s: %s", pieceCidStr, err.Error())
		writeError(w, r, http.StatusBadRequest, msg)
		return
	}

	// Set the Content-Type header explicitly so that http.ServeContent doesn't
	// try to do it implicitly
	w.Header().Set("Content-Type", "application/vnd.ipld.car")

	ctx := r.Context()
	content, err := s.getPieceContent(ctx, pieceCid)
	if err != nil {
		if isNotFoundError(err) {
			writeError(w, r, http.StatusNotFound, err.Error())
			return
		}
		log.Errorf("getting content for piece %s: %s", pieceCid, err)
		msg := fmt.Sprintf("server error getting content for piece CID %s", pieceCidStr)
		writeError(w, r, http.StatusInternalServerError, msg)
		return
	}

	if r.Method == "HEAD" {
		// For an HTTP HEAD request we don't send any data (just headers)
		http.ServeContent(w, r, "", time.Time{}, content)
		alog("%s\tHEAD %s", color.New(color.FgGreen).Sprintf("%d", http.StatusOK), r.URL)
		return
	}

	// Send the CAR file
	// http.ServeContent ignores errors when writing to the stream, so we
	// replace the writer with a class that watches for errors
	writeErrWatcher := &writeErrorWatcher{ResponseWriter: w, onError: func(e error) {
		err = e
	}}

	// Send the content
	start := time.Now()
	alogAt(start, "%s\tGET %s", color.New(color.FgGreen).Sprintf("%d", http.StatusOK), r.URL)
	http.ServeContent(writeErrWatcher, r, "", time.Time{}, content)

	// Check if there was an error during the transfer
	end := time.Now()
	completeMsg := fmt.Sprintf("GET %s\n%s - %s: %s / %s bytes transferred",
		r.URL, end.Format(timeFmt), start.Format(timeFmt), time.Since(start), addCommas(writeErrWatcher.count))
	if err == nil {
		alogAt(end, "%s\t%s", color.New(color.FgGreen).Sprint("DONE"), completeMsg)
	} else {
		alogAt(end, "%s\t%s\n%s",
			color.New(color.FgRed).Sprint("FAIL"), completeMsg, err)
	}
}

// isNotFoundError falls back to checking the error string for "not found".
// Unfortunately we can't always use errors.Is() because the error might
// have crossed an RPC boundary.
func isNotFoundError(err error) bool {
	if errors.Is(err, ErrNotFound) {
		return true
	}
	if errors.Is(err, datastore.ErrNotFound) {
		return true
	}
	if errors.Is(err, retrievalmarket.ErrNotFound) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}

func writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	w.WriteHeader(status)
	w.Write([]byte("Error: " + msg)) //nolint:errcheck
	alog("%s\tGET %s\n%s",
		color.New(color.FgRed).Sprintf("%d", status), r.URL, msg)
}

func (s *HttpServer) getPieceContent(ctx context.Context, pieceCid cid.Cid) (io.ReadSeeker, error) {
	// Get the deals for the piece
	pieceInfo, err := s.api.GetPieceInfo(pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting sector info for piece %s: %w", pieceCid, err)
	}

	// Get the first unsealed deal
	di, err := s.unsealedDeal(ctx, *pieceInfo)
	if err != nil {
		return nil, fmt.Errorf("getting unsealed CAR file: %w", err)
	}

	// Get the raw piece data from the sector
	pieceReader, err := s.api.UnsealSectorAt(ctx, di.SectorID, di.Offset.Unpadded(), di.Length.Unpadded())
	if err != nil {
		return nil, fmt.Errorf("getting raw data from sector %d: %w", di.SectorID, err)
	}

	maxOffset, err := s.api.GetMaxPieceOffset(pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting max offset for piece %s: %w", pieceCid, err)
	}

	_, err = pieceReader.Seek(int64(maxOffset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking to offset %d in piece data: %w", maxOffset, err)
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

func (s *HttpServer) unsealedDeal(ctx context.Context, pieceInfo piecestore.PieceInfo) (*piecestore.DealInfo, error) {
	// There should always been deals in the PieceInfo, but check just in case
	if len(pieceInfo.Deals) == 0 {
		return nil, fmt.Errorf("there are no deals containing piece %s: %w", pieceInfo.PieceCID, ErrNotFound)
	}

	// The same piece can be in many deals. Find the first unsealed deal.
	sealedCount := 0
	var allErr error
	for _, di := range pieceInfo.Deals {
		isUnsealed, err := s.api.IsUnsealed(ctx, di.SectorID, di.Offset.Unpadded(), di.Length.Unpadded())
		if err != nil {
			allErr = multierror.Append(allErr, err)
			continue
		}
		if isUnsealed {
			return &di, nil
		}
		sealedCount++
	}

	// Try to return an error message with as much useful information as possible
	if allErr == nil {
		deals := make([]string, 0, len(pieceInfo.Deals))
		for _, di := range pieceInfo.Deals {
			deals = append(deals, fmt.Sprintf("Deal %d: Sector %d", di.DealID, di.SectorID))
		}
		noDealsErr := fmt.Errorf("%s: %w", strings.Join(deals, ", "), ErrNotFound)
		return nil, fmt.Errorf("checked unsealed status of %d deals containing piece %s: none are unsealed: %w",
			len(pieceInfo.Deals), pieceInfo.PieceCID, noDealsErr)
	}

	if len(pieceInfo.Deals) == 1 {
		return nil, fmt.Errorf("checking unsealed status of deal %d containing piece %s: %w",
			pieceInfo.Deals[0].DealID, pieceInfo.PieceCID, allErr)
	}

	if sealedCount == 0 {
		return nil, fmt.Errorf("checking unsealed status of %d deals containing piece %s: %w",
			len(pieceInfo.Deals), pieceInfo.PieceCID, allErr)
	}

	return nil, fmt.Errorf("checking unsealed status of %d deals containing piece %s - %d are sealed, %d had errors: %w",
		len(pieceInfo.Deals), pieceInfo.PieceCID, sealedCount, len(pieceInfo.Deals)-sealedCount, allErr)
}

// writeErrorWatcher calls onError if there is an error writing to the writer
type writeErrorWatcher struct {
	http.ResponseWriter
	count   uint64
	onError func(err error)
}

func (w *writeErrorWatcher) Write(bz []byte) (int, error) {
	count, err := w.ResponseWriter.Write(bz)
	if err != nil {
		w.onError(err)
	}
	w.count += uint64(count)
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

const timeFmt = "2006-01-02T15:04:05.000Z0700"

func alog(l string, args ...interface{}) {
	alogAt(time.Now(), l, args...)
}

func alogAt(at time.Time, l string, args ...interface{}) {
	fmt.Printf(at.Format(timeFmt)+"\t"+l+"\n", args...)
}
