package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/boostd-data/model"
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
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/multiformats/go-multihash"
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
	path string
	port int
	api  HttpServerApi

	ctx    context.Context
	cancel context.CancelFunc
	server *http.Server
}

type HttpServerApi interface {
	PiecesContainingMultihash(ctx context.Context, mh multihash.Multihash) ([]cid.Cid, error)
	GetCarSize(ctx context.Context, pieceCid cid.Cid) (uint64, error)
	GetPieceDeals(ctx context.Context, pieceCID cid.Cid) ([]model.DealInfo, error)
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
		content, err = s.getCarContent(ctx, pieceCid, content)
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
		content, err = s.getCarContent(ctx, pieceCid, content)
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

	var writer http.ResponseWriter

	// http.ServeContent ignores errors when writing to the stream, so we
	// replace the writer with a class that watches for errors
	var err error
	writeErrWatcher := &writeErrorWatcher{ResponseWriter: w, onError: func(e error) {
		err = e
	}}

	writer = writeErrWatcher //Need writeErrWatcher to be of type writeErrorWatcher for addCommas()

	// Note that the last modified time is a constant value because the data
	// in a piece identified by a cid will never change.
	start := time.Now()
	alogAt(start, "%s\tGET %s", color.New(color.FgGreen).Sprintf("%d", http.StatusOK), r.URL)
	isGzipped := strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")
	if isGzipped {
		// If Accept-Encoding header contains gzip then send a gzipped response

		gzwriter := gziphandler.GzipResponseWriter{
			ResponseWriter: writeErrWatcher,
		}
		// Close the writer to flush buffer
		defer gzwriter.Close()
		writer = &gzwriter
	}

	if r.Method == "HEAD" {
		// For an HTTP HEAD request ServeContent doesn't send any data (just headers)
		http.ServeContent(writer, r, "", time.Time{}, content)
		alog("%s\tHEAD %s", color.New(color.FgGreen).Sprintf("%d", http.StatusOK), r.URL)
		return
	}

	// Send the content
	http.ServeContent(writer, r, "", lastModified, content)

	// Write a line to the log
	end := time.Now()
	completeMsg := fmt.Sprintf("GET %s\n%s - %s: %s / %s bytes transferred",
		r.URL, end.Format(timeFmt), start.Format(timeFmt), time.Since(start), addCommas(writeErrWatcher.count))
	if isGzipped {
		completeMsg += " (gzipped)"
	}
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
	pieceDeals, err := s.api.GetPieceDeals(ctx, pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting sector info for piece %s: %w", pieceCid, err)
	}

	// Get the first unsealed deal
	di, err := s.unsealedDeal(ctx, pieceCid, pieceDeals)
	if err != nil {
		return nil, fmt.Errorf("getting unsealed CAR file: %w", err)
	}

	// Get the raw piece data from the sector
	pieceReader, err := s.api.UnsealSectorAt(ctx, di.SectorID, di.PieceOffset.Unpadded(), di.PieceLength.Unpadded())
	if err != nil {
		return nil, fmt.Errorf("getting raw data from sector %d: %w", di.SectorID, err)
	}

	return pieceReader, nil
}

func (s *HttpServer) getCarContent(ctx context.Context, pieceCid cid.Cid, pieceReader io.ReadSeeker) (io.ReadSeeker, error) {
	// Get the CAR size from the piece directory
	unpaddedCarSize, err := s.api.GetCarSize(ctx, pieceCid)
	if err != nil {
		return nil, fmt.Errorf("getting unpadded CAR size: %w", err)
	}

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

func (s *HttpServer) unsealedDeal(ctx context.Context, pieceCid cid.Cid, pieceDeals []model.DealInfo) (*model.DealInfo, error) {
	// There should always been deals in the PieceInfo, but check just in case
	if len(pieceDeals) == 0 {
		return nil, fmt.Errorf("there are no deals containing piece %s: %w", pieceCid, ErrNotFound)
	}

	// The same piece can be in many deals. Find the first unsealed deal.
	sealedCount := 0
	var allErr error
	for _, di := range pieceDeals {
		isUnsealed, err := s.api.IsUnsealed(ctx, di.SectorID, di.PieceOffset.Unpadded(), di.PieceLength.Unpadded())
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
	dealSectors := make([]string, 0, len(pieceDeals))
	for _, di := range pieceDeals {
		dealSectors = append(dealSectors, fmt.Sprintf("Deal %d: Sector %d", di.ChainDealID, di.SectorID))
	}

	if allErr == nil {
		dealSectorsErr := fmt.Errorf("%s: %w", strings.Join(dealSectors, ", "), ErrNotFound)
		return nil, fmt.Errorf("checked unsealed status of %d deals containing piece %s: none are unsealed: %w",
			len(pieceDeals), pieceCid, dealSectorsErr)
	}

	if len(pieceDeals) == 1 {
		return nil, fmt.Errorf("checking unsealed status of deal %d (sector %d) containing piece %s: %w",
			pieceDeals[0].ChainDealID, pieceDeals[0].SectorID, pieceCid, allErr)
	}

	if sealedCount == 0 {
		return nil, fmt.Errorf("checking unsealed status of %d deals containing piece %s: %s: %w",
			len(pieceDeals), pieceCid, dealSectors, allErr)
	}

	return nil, fmt.Errorf("checking unsealed status of %d deals containing piece %s - %d are sealed, %d had errors: %s: %w",
		len(pieceDeals), pieceCid, sealedCount, len(pieceDeals)-sealedCount, dealSectors, allErr)
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

const timeFmt = "2006-01-02T15:04:05.000Z0700"

func alog(l string, args ...interface{}) {
	alogAt(time.Now(), l, args...)
}

func alogAt(at time.Time, l string, args ...interface{}) {
	fmt.Printf(at.Format(timeFmt)+"\t"+l+"\n", args...)
}
