package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/fatih/color"
	"github.com/filecoin-project/boost/metrics"
	"github.com/filecoin-project/boost/tracing"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
	"go.opencensus.io/stats"
)

//go:generate go run github.com/golang/mock/mockgen -destination=mocks/mock_booster_http.go -package=mocks_booster_http -source=server.go HttpServerApi,serverApi

var ErrNotFound = errors.New("not found")

// For data served by the endpoints in the HTTP server that never changes
// (eg pieces identified by a piece CID) send a cache header with a constant,
// non-zero last modified time.
var lastModified = time.UnixMilli(1)

const carSuffix = ".car"
const pieceCidParam = "pieceCid"
const payloadCidParam = "payloadCid"

type HttpServer struct {
	path          string
	port          int
	allowIndexing bool
	api           HttpServerApi

	ctx    context.Context
	cancel context.CancelFunc
	server *http.Server
}

type HttpServerApi interface {
	PiecesContainingMultihash(ctx context.Context, mh multihash.Multihash) ([]cid.Cid, error)
	GetMaxPieceOffset(pieceCid cid.Cid) (uint64, error)
	GetPieceInfo(pieceCID cid.Cid) (*piecestore.PieceInfo, error)
	IsUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error)
	UnsealSectorAt(ctx context.Context, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error)
}

func NewHttpServer(path string, port int, allowIndexing bool, api HttpServerApi) *HttpServer {
	return &HttpServer{path: path, port: port, allowIndexing: allowIndexing, api: api}
}

func (s *HttpServer) pieceBasePath() string {
	return s.path + "/piece/"
}

func (s *HttpServer) Start(ctx context.Context) {
	s.ctx, s.cancel = context.WithCancel(ctx)

	listenAddr := fmt.Sprintf(":%d", s.port)
	handler := http.NewServeMux()
	handler.HandleFunc(s.pieceBasePath(), s.handlePieceRequest)
	handler.HandleFunc("/", s.handleIndex)
	handler.HandleFunc("/index.html", s.handleIndex)
	handler.Handle("/metrics", metrics.Exporter("booster_http")) // metrics
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
			log.Fatalf("http.ListenAndServe(): %w", err)
		}
	}()
}

func (s *HttpServer) Stop() error {
	s.cancel()
	return s.server.Close()
}

const idxPage = `
<html>
  <body>
    <h4>Booster HTTP Server</h4>
    Endpoints:
    <table>
      <tbody>
      <tr>
        <td>
          Download a raw piece by payload CID
        </td>
        <td>
          <a href="/piece?payloadCid=bafySomePayloadCid&format=piece" > /piece?payloadCid=<payload cid>&format=piece</a>
        </td>
      </tr>
      <tr>
        <td>
          Download a CAR file by payload CID
        </td>
        <td>
          <a href="/piece?payloadCid=bafySomePayloadCid&format=car" > /piece?payloadCid=<payload cid>&format=car</a>
        </td>
      </tr>
      <tr>
        <td>
          Download a raw piece by piece CID
        </td>
        <td>
          <a href="/piece?pieceCid=bagaSomePieceCID&format=piece" > /piece?pieceCid=<piece cid>&format=piece</a>
        </td>
      </tr>
      <tr>
        <td>
          Download a CAR file by piece CID
        </td>
        <td>
          <a href="/piece?pieceCid=bagaSomePieceCID&format=car" > /piece?payloadCid=<piece cid>&format=car</a>
        </td>
      </tr>
      </tbody>
    </table>
  </body>
</html>
`

func (s *HttpServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(idxPage)) //nolint:errcheck
}

func (s *HttpServer) handlePieceRequest(w http.ResponseWriter, r *http.Request) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		msg := fmt.Sprintf("parsing query: %s", err.Error())
		writeError(w, r, http.StatusBadRequest, msg)
		return
	}

	isCar := false

	if len(q["format"]) == 1 {
		if q["format"][0] == "car" { // Check if format value is car
			isCar = true
		} else if q["format"][0] != "piece" { // Check if format value is not piece
			writeError(w, r, http.StatusBadRequest, "incorrect `format` query parameter")
			return
		}
	} else if len(q["format"]) == 0 {
		isCar = false
	} else { // Error if more than 1 format value
		writeError(w, r, http.StatusBadRequest, "single `format` query parameter is allowed")
		return
	}

	// Check provided cid and format and redirect the request appropriately
	if len(q[payloadCidParam]) == 1 {
		payloadCid, err := cid.Parse(q[payloadCidParam][0])
		if err != nil {
			msg := fmt.Sprintf("parsing payload CID '%s': %s", q[payloadCidParam][0], err.Error())
			writeError(w, r, http.StatusBadRequest, msg)
			stats.Record(r.Context(), metrics.HttpPayloadByCidRequestCount.M(1))
			return
		}
		s.handleByPayloadCid(payloadCid, isCar, w, r)
	} else if len(q[pieceCidParam]) == 1 {
		pieceCid, err := cid.Parse(q[pieceCidParam][0])
		if err != nil {
			msg := fmt.Sprintf("parsing piece CID '%s': %s", q[pieceCidParam][0], err.Error())
			writeError(w, r, http.StatusBadRequest, msg)
			stats.Record(r.Context(), metrics.HttpPieceByCidRequestCount.M(1))
			return
		}
		s.handleByPieceCid(pieceCid, isCar, w, r)
	} else {
		writeError(w, r, http.StatusBadRequest, "unsupported query")
	}
}

func (s *HttpServer) handleByPayloadCid(payloadCid cid.Cid, isCar bool, w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	ctx, span := tracing.Tracer.Start(r.Context(), "http.payload_cid")
	defer span.End()

	stats.Record(ctx, metrics.HttpPayloadByCidRequestCount.M(1))

	// Find all the pieces that contain the payload cid
	pieces, err := s.api.PiecesContainingMultihash(ctx, payloadCid.Hash())
	if err != nil {
		if isNotFoundError(err) {
			msg := fmt.Sprintf("getting piece that contains payload CID '%s': %s", payloadCid, err.Error())
			writeError(w, r, http.StatusNotFound, msg)
			stats.Record(ctx, metrics.HttpPayloadByCid404ResponseCount.M(1))
			return
		}
		log.Errorf("getting piece that contains payload CID '%s': %s", payloadCid, err)
		msg := fmt.Sprintf("server error getting piece that contains payload CID '%s'", payloadCid)
		writeError(w, r, http.StatusInternalServerError, msg)
		stats.Record(ctx, metrics.HttpPayloadByCid500ResponseCount.M(1))
		return
	}

	// Just get the content of the first piece returned (if the client wants a
	// different piece they can just call the /piece endpoint)
	pieceCid := pieces[0]
	content, err := s.getPieceContent(ctx, pieceCid)
	if err == nil && isCar {
		content, err = s.getCarContent(pieceCid, content)
	}
	if err != nil {
		if isNotFoundError(err) {
			msg := fmt.Sprintf("getting content for payload CID %s in piece %s: %s", payloadCid, pieceCid, err)
			writeError(w, r, http.StatusNotFound, msg)
			stats.Record(ctx, metrics.HttpPayloadByCid404ResponseCount.M(1))
			return
		}
		log.Errorf("getting content for payload CID %s in piece %s: %s", payloadCid, pieceCid, err)
		msg := fmt.Sprintf("server error getting content for payload CID %s in piece %s", payloadCid, pieceCid)
		writeError(w, r, http.StatusInternalServerError, msg)
		stats.Record(ctx, metrics.HttpPayloadByCid500ResponseCount.M(1))
		return
	}

	// Set an Etag based on the piece cid
	etag := pieceCid.String()
	if isCar {
		etag += carSuffix
	}
	w.Header().Set("Etag", etag)

	serveContent(w, r, content, getContentType(isCar))

	stats.Record(ctx, metrics.HttpPayloadByCid200ResponseCount.M(1))
	stats.Record(ctx, metrics.HttpPayloadByCidRequestDuration.M(float64(time.Since(startTime).Milliseconds())))
}

func (s *HttpServer) handleByPieceCid(pieceCid cid.Cid, isCar bool, w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	ctx, span := tracing.Tracer.Start(r.Context(), "http.piece_cid")
	defer span.End()
	stats.Record(ctx, metrics.HttpPieceByCidRequestCount.M(1))

	// Get a reader over the piece
	content, err := s.getPieceContent(ctx, pieceCid)
	if err == nil && isCar {
		content, err = s.getCarContent(pieceCid, content)
	}
	if err != nil {
		if isNotFoundError(err) {
			writeError(w, r, http.StatusNotFound, err.Error())
			stats.Record(ctx, metrics.HttpPieceByCid404ResponseCount.M(1))
			return
		}
		log.Errorf("getting content for piece %s: %s", pieceCid, err)
		msg := fmt.Sprintf("server error getting content for piece CID %s", pieceCid)
		writeError(w, r, http.StatusInternalServerError, msg)
		stats.Record(ctx, metrics.HttpPieceByCid500ResponseCount.M(1))
		return
	}

	// Set an Etag based on the piece cid
	etag := pieceCid.String()
	if isCar {
		etag += carSuffix
	}
	w.Header().Set("Etag", etag)

	serveContent(w, r, content, getContentType(isCar))

	stats.Record(ctx, metrics.HttpPieceByCid200ResponseCount.M(1))
	stats.Record(ctx, metrics.HttpPieceByCidRequestDuration.M(float64(time.Since(startTime).Milliseconds())))
}

func getContentType(isCar bool) string {
	if isCar {
		return "application/vnd.ipld.car"
	}
	return "application/piece"
}

func serveContent(w http.ResponseWriter, r *http.Request, content io.ReadSeeker, contentType string) {
	// Set the Content-Type header explicitly so that http.ServeContent doesn't
	// try to do it implicitly
	w.Header().Set("Content-Type", contentType)

	// http.ServeContent ignores errors when writing to the stream, so we
	// replace the writer with a class that watches for errors
	var err error
	writeErrWatcher := &writeErrorWatcher{ResponseWriter: w, onError: func(e error) {
		err = e
	}}

	// Note that the last modified time is a constant value because the data
	// in a piece identified by a cid will never change.
	start := time.Now()
	alogAt(start, "%s\tGET %s", color.New(color.FgGreen).Sprintf("%d", http.StatusOK), r.URL)
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		// If Accept-Encoding header contains gzip then send a gzipped response

		gzipwriter := &gziphandler.GzipResponseWriter{
			ResponseWriter: writeErrWatcher,
		}
		// Set the Content-Encoding header to gzip
		w.Header().Set("Content-Encoding", "gzip")
		if r.Method == "HEAD" {
			// For an HTTP HEAD request ServeContent doesn't send any data (just headers)
			http.ServeContent(w, r, "", time.Time{}, content)
			alog("%s\tHEAD %s", color.New(color.FgGreen).Sprintf("%d", http.StatusOK), r.URL)
			return
		}

		// Send the content
		http.ServeContent(gzipwriter, r, "", lastModified, content)
	} else {
		if r.Method == "HEAD" {
			// For an HTTP HEAD request ServeContent doesn't send any data (just headers)
			http.ServeContent(w, r, "", time.Time{}, content)
			alog("%s\tHEAD %s", color.New(color.FgGreen).Sprintf("%d", http.StatusOK), r.URL)
			return
		}

		// Send the content
		http.ServeContent(writeErrWatcher, r, "", lastModified, content)
	}

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

	return pieceReader, nil
}

func (s *HttpServer) getCarContent(pieceCid cid.Cid, pieceReader io.ReadSeeker) (io.ReadSeeker, error) {
	maxOffset, err := s.api.GetMaxPieceOffset(pieceCid)
	if err != nil {
		if s.allowIndexing {
			// If it's not possible to get the max piece offset it may be because
			// the CAR file hasn't been indexed yet. So try to index it in real time.
			alog("%s\tbuilding index for %s", color.New(color.FgBlue).Sprintf("INFO"), pieceCid)
			maxOffset, err = getMaxPieceOffset(pieceReader)
		}
		if err != nil {
			return nil, fmt.Errorf("getting max offset for piece %s: %w", pieceCid, err)
		}
	}

	// Seek to the max offset
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

// getMaxPieceOffset generates a CAR file index from the reader, and returns
// the maximum offset in the index
func getMaxPieceOffset(reader io.ReadSeeker) (uint64, error) {
	idx, err := car.GenerateIndex(reader, car.ZeroLengthSectionAsEOF(true), car.StoreIdentityCIDs(true))
	if err != nil {
		return 0, fmt.Errorf("generating CAR index: %w", err)
	}

	itidx, ok := idx.(index.IterableIndex)
	if !ok {
		return 0, fmt.Errorf("could not cast CAR file index %t to an IterableIndex", idx)
	}

	var maxOffset uint64
	err = itidx.ForEach(func(m multihash.Multihash, offset uint64) error {
		if offset > maxOffset {
			maxOffset = offset
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("getting max offset: %w", err)
	}

	return maxOffset, nil
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
			// Found a deal with an unsealed piece, so return the deal info
			return &di, nil
		}
		sealedCount++
	}

	// It wasn't possible to find a deal with the piece cid that is unsealed.
	// Try to return an error message with as much useful information as possible
	dealSectors := make([]string, 0, len(pieceInfo.Deals))
	for _, di := range pieceInfo.Deals {
		dealSectors = append(dealSectors, fmt.Sprintf("Deal %d: Sector %d", di.DealID, di.SectorID))
	}

	if allErr == nil {
		dealSectorsErr := fmt.Errorf("%s: %w", strings.Join(dealSectors, ", "), ErrNotFound)
		return nil, fmt.Errorf("checked unsealed status of %d deals containing piece %s: none are unsealed: %w",
			len(pieceInfo.Deals), pieceInfo.PieceCID, dealSectorsErr)
	}

	if len(pieceInfo.Deals) == 1 {
		return nil, fmt.Errorf("checking unsealed status of deal %d (sector %d) containing piece %s: %w",
			pieceInfo.Deals[0].DealID, pieceInfo.Deals[0].SectorID, pieceInfo.PieceCID, allErr)
	}

	if sealedCount == 0 {
		return nil, fmt.Errorf("checking unsealed status of %d deals containing piece %s: %s: %w",
			len(pieceInfo.Deals), pieceInfo.PieceCID, dealSectors, allErr)
	}

	return nil, fmt.Errorf("checking unsealed status of %d deals containing piece %s - %d are sealed, %d had errors: %s: %w",
		len(pieceInfo.Deals), pieceInfo.PieceCID, sealedCount, len(pieceInfo.Deals)-sealedCount, dealSectors, allErr)
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
