package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/fatih/color"
	"github.com/filecoin-project/boost-gfm/piecestore"
	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/boost/metrics"
	"github.com/filecoin-project/boostd-data/shared/tracing"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	"go.opencensus.io/stats"
)

//go:generate go run github.com/golang/mock/mockgen -destination=mocks/mock_booster_http.go -package=mocks_booster_http -source=server.go HttpServerApi,serverApi

var ErrNotFound = errors.New("not found")

// For data served by the endpoints in the HTTP server that never changes
// (eg pieces identified by a piece CID) send a cache header with a constant,
// non-zero last modified time.
var lastModified = time.UnixMilli(1)

type apiVersion struct {
	Version string `json:"Version"`
}

type HttpServer struct {
	path    string
	port    int
	api     HttpServerApi
	opts    HttpServerOptions
	idxPage string

	ctx    context.Context
	cancel context.CancelFunc
	server *http.Server
}

type HttpServerApi interface {
	GetPieceInfo(pieceCID cid.Cid) (*piecestore.PieceInfo, error)
	IsUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error)
	UnsealSectorAt(ctx context.Context, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error)
}

type HttpServerOptions struct {
	Blockstore               blockstore.Blockstore
	ServePieces              bool
	SupportedResponseFormats []string
}

func NewHttpServer(path string, port int, api HttpServerApi, opts *HttpServerOptions) *HttpServer {
	if opts == nil {
		opts = &HttpServerOptions{ServePieces: true}
	}
	return &HttpServer{path: path, port: port, api: api, opts: *opts, idxPage: parseTemplate(*opts)}
}

func (s *HttpServer) pieceBasePath() string {
	return s.path + "/piece/"
}

func (s *HttpServer) ipfsBasePath() string {
	return s.path + "/ipfs/"
}

func (s *HttpServer) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)
	handler := http.NewServeMux()

	if s.opts.ServePieces {
		handler.HandleFunc(s.pieceBasePath(), s.handleByPieceCid)
	}

	if s.opts.Blockstore != nil {
		blockService := blockservice.New(s.opts.Blockstore, offline.Exchange(s.opts.Blockstore))
		gw, err := NewBlocksGateway(blockService, nil)
		if err != nil {
			return fmt.Errorf("creating blocks gateway: %w", err)
		}
		handler.Handle(s.ipfsBasePath(), newGatewayHandler(gw, s.opts.SupportedResponseFormats))
	}

	handler.HandleFunc("/", s.handleIndex)
	handler.HandleFunc("/index.html", s.handleIndex)
	handler.HandleFunc("/info", s.handleInfo)
	handler.Handle("/metrics", metrics.Exporter("booster_http")) // metrics
	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
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

	return nil
}

func (s *HttpServer) Stop() error {
	s.cancel()
	return s.server.Close()
}

func (s *HttpServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(s.idxPage)) //nolint:errcheck
}

func (s *HttpServer) handleInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	v := apiVersion{
		Version: "0.3.0",
	}
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func (s *HttpServer) handleByPieceCid(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()
	ctx, span := tracing.Tracer.Start(r.Context(), "http.piece_cid")
	defer span.End()
	stats.Record(ctx, metrics.HttpPieceByCidRequestCount.M(1))

	// Remove the path up to the piece cid
	prefixLen := len(s.pieceBasePath())
	if len(r.URL.Path) <= prefixLen {
		msg := fmt.Sprintf("path '%s' is missing piece CID", r.URL.Path)
		writeError(w, r, http.StatusBadRequest, msg)
		stats.Record(ctx, metrics.HttpPieceByCid400ResponseCount.M(1))
		return
	}

	pieceCidStr := r.URL.Path[prefixLen:]
	pieceCid, err := cid.Parse(pieceCidStr)
	if err != nil {
		msg := fmt.Sprintf("parsing piece CID '%s': %s", pieceCidStr, err.Error())
		writeError(w, r, http.StatusBadRequest, msg)
		stats.Record(ctx, metrics.HttpPieceByCid400ResponseCount.M(1))
		return
	}

	// Get a reader over the piece
	content, err := s.getPieceContent(ctx, pieceCid)
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
	setEtag(w, pieceCid.String())

	serveContent(w, r, content)

	stats.Record(ctx, metrics.HttpPieceByCid200ResponseCount.M(1))
	stats.Record(ctx, metrics.HttpPieceByCidRequestDuration.M(float64(time.Since(startTime).Milliseconds())))
}

func serveContent(w http.ResponseWriter, r *http.Request, content io.ReadSeeker) {
	// Set the Content-Type header explicitly so that http.ServeContent doesn't
	// try to do it implicitly
	w.Header().Set("Content-Type", "application/piece")

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

func setEtag(w http.ResponseWriter, etag string) {
	// Note: the etag must be surrounded by "quotes"
	w.Header().Set("Etag", `"`+etag+`"`)
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

func (s *HttpServer) unsealedDeal(ctx context.Context, pieceInfo piecestore.PieceInfo) (*piecestore.DealInfo, error) {
	// There should always be deals in the PieceInfo, but check just in case
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

const timeFmt = "2006-01-02T15:04:05.000Z0700"

func alog(l string, args ...interface{}) {
	alogAt(time.Now(), l, args...)
}

func alogAt(at time.Time, l string, args ...interface{}) {
	fmt.Printf(at.Format(timeFmt)+"\t"+l+"\n", args...)
}
