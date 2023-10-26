package main

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/tracing"
	"github.com/filecoin-project/boost/metrics"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/frisbii"
	"go.opencensus.io/stats"
)

func (s *HttpServer) pieceHandler() http.HandlerFunc {
	var pieceHandler http.Handler = http.HandlerFunc(s.handleByPieceCid)
	if s.opts.CompressionLevel != gzip.NoCompression {
		gzipWrapper := gziphandler.MustNewGzipLevelHandler(s.opts.CompressionLevel)
		pieceHandler = gzipWrapper(pieceHandler)
		log.Debugf("enabling compression with a level of %d", s.opts.CompressionLevel)
	}
	return pieceHandler.ServeHTTP
}

func (s *HttpServer) handleByPieceCid(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	ctx, span := tracing.Tracer.Start(r.Context(), "http.piece_cid")
	defer span.End()
	stats.Record(ctx, metrics.HttpPieceByCidRequestCount.M(1))

	// Remove the path up to the piece cid
	prefixLen := len(s.pieceBasePath())
	if len(r.URL.Path) <= prefixLen {
		writeError(w, r, http.StatusBadRequest, fmt.Errorf("path '%s' is missing piece CID", r.URL.Path))
		stats.Record(ctx, metrics.HttpPieceByCid400ResponseCount.M(1))
		return
	}

	pieceCidStr := r.URL.Path[prefixLen:]
	pieceCid, err := cid.Parse(pieceCidStr)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, fmt.Errorf("parsing piece CID '%s': %s", pieceCidStr, err.Error()))
		stats.Record(ctx, metrics.HttpPieceByCid400ResponseCount.M(1))
		return
	}

	// Get a reader over the piece
	content, err := s.getPieceContent(ctx, pieceCid)
	if err != nil {
		if isNotFoundError(err) {
			writeError(w, r, http.StatusNotFound, err)
			stats.Record(ctx, metrics.HttpPieceByCid404ResponseCount.M(1))
			return
		}
		writeError(w, r, http.StatusInternalServerError, fmt.Errorf("server error getting content for piece CID %s: %s", pieceCid, err.Error()))
		stats.Record(ctx, metrics.HttpPieceByCid500ResponseCount.M(1))
		return
	}

	setHeaders(w, pieceCid)
	serveContent(w, r, content)

	stats.Record(ctx, metrics.HttpPieceByCid200ResponseCount.M(1))
	stats.Record(ctx, metrics.HttpPieceByCidRequestDuration.M(float64(time.Since(startTime).Milliseconds())))
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
	pieceReader, err := s.api.UnsealSectorAt(ctx, di.MinerAddr, di.SectorID, di.PieceOffset.Unpadded(), di.PieceLength.Unpadded())
	if err != nil {
		return nil, fmt.Errorf("getting raw data from sector %d: %w", di.SectorID, err)
	}

	return pieceReader, nil
}

func isGzipped(res http.ResponseWriter) bool {
	switch res.(type) {
	case *gziphandler.GzipResponseWriter, gziphandler.GzipResponseWriterWithCloseNotify:
		// there are conditions where we may have a GzipResponseWriter but the
		// response will not be compressed, but they are related to very small
		// response sizes so this shouldn't matter (much)
		return true
	}
	return false
}

func setHeaders(w http.ResponseWriter, pieceCid cid.Cid) {
	w.Header().Set("Vary", "Accept-Encoding")
	etag := `"` + pieceCid.String() + `"` // must be quoted
	if isGzipped(w) {
		etag = etag[:len(etag)-1] + ".gz\""
	}
	w.Header().Set("Etag", etag)
	w.Header().Set("Content-Type", "application/piece")
	w.Header().Set("Cache-Control", "public, max-age=29030400, immutable")
}

func serveContent(res http.ResponseWriter, req *http.Request, content io.ReadSeeker) {
	// http.ServeContent ignores errors when writing to the stream, so we
	// replace the writer with a class that watches for errors
	res = newPieceAccountingWriter(res, toLoggingResponseWriter(res))

	// Note that the last modified time is a constant value because the data
	// in a piece identified by a cid will never change.

	if req.Method == http.MethodHead {
		// For an HTTP HEAD request ServeContent doesn't send any data (just headers)
		http.ServeContent(res, req, "", time.Time{}, content)
		return
	}

	// Send the content
	http.ServeContent(res, req, "", lastModified, content)
}

// isNotFoundError falls back to checking the error string for "not found".
// Unfortunately we can't always use errors.Is() because the error might
// have crossed an RPC boundary.
func isNotFoundError(err error) bool {
	switch {
	case errors.Is(err, ErrNotFound),
		errors.Is(err, datastore.ErrNotFound),
		errors.Is(err, retrievalmarket.ErrNotFound),
		strings.Contains(strings.ToLower(err.Error()), "not found"):
		return true
	default:
		return false
	}
}

func writeError(w http.ResponseWriter, r *http.Request, status int, msg error) {
	log.Warnf("error handling request [%s]: %s", r.URL.String(), msg.Error())
	if lrw := toLoggingResponseWriter(w); lrw != nil {
		lrw.LogError(status, msg) // will log the lowest wrapped error, so %w errors are isolated
	} else {
		log.Error("no logging response writer to report to")
		http.Error(w, msg.Error(), status)
	}
}

func (s *HttpServer) unsealedDeal(ctx context.Context, pieceCid cid.Cid, pieceDeals []model.DealInfo) (*model.DealInfo, error) {
	// There should always be deals in the PieceInfo, but check just in case
	if len(pieceDeals) == 0 {
		return nil, fmt.Errorf("there are no deals containing piece %s: %w", pieceCid, ErrNotFound)
	}

	// The same piece can be in many deals. Find the first unsealed deal.
	sealedCount := 0
	var allErr error
	for _, di := range pieceDeals {
		isUnsealed, err := s.api.IsUnsealed(ctx, di.MinerAddr, di.SectorID, di.PieceOffset.Unpadded(), di.PieceLength.Unpadded())
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
		if di.IsDirectDeal {
			dealSectors = append(dealSectors, fmt.Sprintf("Allocation %d: Sector %d", di.ChainDealID, di.SectorID))
		} else {
			dealSectors = append(dealSectors, fmt.Sprintf("Deal %d: Sector %d", di.ChainDealID, di.SectorID))
		}
	}

	if allErr == nil {
		dealSectorsErr := fmt.Errorf("%s: %w", strings.Join(dealSectors, ", "), ErrNotFound)
		return nil, fmt.Errorf("checked unsealed status of %d deals containing piece %s: none are unsealed: %w",
			len(pieceDeals), pieceCid, dealSectorsErr)
	}

	if len(pieceDeals) == 1 {
		if pieceDeals[0].IsDirectDeal {
			return nil, fmt.Errorf("checking unsealed status of allocation %d (sector %d) containing piece %s: %w",
				pieceDeals[0].ChainDealID, pieceDeals[0].SectorID, pieceCid, allErr)
		}
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

func toLoggingResponseWriter(res http.ResponseWriter) *frisbii.LoggingResponseWriter {
	switch lrw := res.(type) {
	case *frisbii.LoggingResponseWriter:
		return lrw
	case *gziphandler.GzipResponseWriter:
		if lrw, ok := lrw.ResponseWriter.(*frisbii.LoggingResponseWriter); ok {
			return lrw
		}
	}
	return nil
}

// pieceAccountingWriter reports the number of bytes written to a
// LoggingResponseWriter so the compression ratio can be calculated.
type pieceAccountingWriter struct {
	http.ResponseWriter
	lrw *frisbii.LoggingResponseWriter
}

func newPieceAccountingWriter(
	w http.ResponseWriter,
	lrw *frisbii.LoggingResponseWriter,
) *pieceAccountingWriter {
	return &pieceAccountingWriter{ResponseWriter: w, lrw: lrw}
}

func (w *pieceAccountingWriter) Write(bz []byte) (int, error) {
	count, err := w.ResponseWriter.Write(bz)
	if w.lrw != nil {
		w.lrw.WroteBytes(count)
	}
	return count, err
}
