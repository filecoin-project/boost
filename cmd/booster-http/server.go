package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/filecoin-project/boost-graphsync/storeutil"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/metrics"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	"github.com/ipld/frisbii"
	"github.com/rs/cors"
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
	path       string
	listenAddr string
	port       int
	api        HttpServerApi
	opts       HttpServerOptions
	idxPage    string

	ctx    context.Context
	cancel context.CancelFunc
	server *http.Server
}

type HttpServerApi interface {
	GetPieceDeals(ctx context.Context, pieceCID cid.Cid) ([]model.DealInfo, error)
	IsUnsealed(ctx context.Context, minerAddr address.Address, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error)
	UnsealSectorAt(ctx context.Context, minerAddr address.Address, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error)
}

type HttpServerOptions struct {
	Blockstore       blockstore.Blockstore
	ServePieces      bool
	ServeTrustless   bool
	CompressionLevel int
	LogWriter        io.Writer          // for a standardised log write format
	LogHandler       frisbii.LogHandler // for more granular control over log output
}

func NewHttpServer(path string, listenAddr string, port int, api HttpServerApi, opts *HttpServerOptions) *HttpServer {
	if opts == nil {
		opts = &HttpServerOptions{ServePieces: true, ServeTrustless: false, CompressionLevel: gzip.NoCompression}
	}
	return &HttpServer{path: path, listenAddr: listenAddr, port: port, api: api, opts: *opts, idxPage: parseTemplate(*opts)}
}

func (s *HttpServer) pieceBasePath() string {
	return s.path + "/piece/"
}

func (s *HttpServer) ipfsBasePath() string {
	return s.path + "/ipfs/"
}

func newCors() *cors.Cors {
	options := cors.Options{
		AllowedHeaders: []string{"*"},
	}
	c := cors.New(options)
	return c
}

func (s *HttpServer) Start(ctx context.Context) error {
	if !s.opts.ServePieces && !s.opts.ServeTrustless {
		return errors.New("no content to serve")
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	c := newCors()
	handler := http.NewServeMux()

	if s.opts.ServePieces {
		handler.HandleFunc(s.pieceBasePath(), s.pieceHandler())
	}

	if s.opts.ServeTrustless {
		if s.opts.Blockstore == nil {
			return errors.New("no blockstore provided for trustless gateway")
		}
		lsys := storeutil.LinkSystemForBlockstore(s.opts.Blockstore)
		handler.Handle(
			s.ipfsBasePath(),
			frisbii.NewHttpIpfs(ctx, lsys, frisbii.WithCompressionLevel(s.opts.CompressionLevel)),
		)
	}

	handler.HandleFunc("/", s.handleIndex)
	handler.HandleFunc("/index.html", s.handleIndex)
	handler.HandleFunc("/info", s.handleInfo)
	handler.Handle("/metrics", metrics.Exporter("booster_http")) // metrics
	s.server = &http.Server{
		Addr: fmt.Sprintf("%s:%d", s.listenAddr, s.port),
		Handler: c.Handler(
			frisbii.NewLogMiddleware(handler, frisbii.WithLogWriter(s.opts.LogWriter), frisbii.WithLogHandler(s.opts.LogHandler)),
		),
		// This context will be the parent of the context associated with all
		// incoming requests
		BaseContext: func(listener net.Listener) context.Context {
			return s.ctx
		},
	}

	go func() {
		if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("http.ListenAndServe(): %v", err.Error())
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
