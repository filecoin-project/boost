package frisbii

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	trustlessutils "github.com/ipld/go-trustless-utils"
	trustlesshttp "github.com/ipld/go-trustless-utils/http"
)

var _ http.Handler = (*HttpIpfs)(nil)

type ErrorLogger interface {
	LogError(status int, err error)
}

// HttpIpfs is an http.Handler that serves IPLD data via HTTP according to the
// Trustless Gateway specification.
type HttpIpfs struct {
	ctx  context.Context
	lsys linking.LinkSystem
	cfg  *httpOptions
}

type httpOptions struct {
	MaxResponseDuration time.Duration
	MaxResponseBytes    int64
}

type HttpOption func(*httpOptions)

// WithMaxResponseDuration sets the maximum duration for a response to be
// streamed before the connection is closed. This allows a server to limit the
// amount of time a client can hold a connection open; and also restricts the
// ability to serve very large DAGs.
//
// A value of 0 will disable the limitation. This is the default.
func WithMaxResponseDuration(d time.Duration) HttpOption {
	return func(o *httpOptions) {
		o.MaxResponseDuration = d
	}
}

// WithMaxResponseBytes sets the maximum number of bytes that will be streamed
// before the connection is closed. This allows a server to limit the amount of
// data a client can request; and also restricts the ability to serve very large
// DAGs.
//
// A value of 0 will disable the limitation. This is the default.
func WithMaxResponseBytes(b int64) HttpOption {
	return func(o *httpOptions) {
		o.MaxResponseBytes = b
	}
}

func NewHttpIpfs(
	ctx context.Context,
	lsys linking.LinkSystem,
	opts ...HttpOption,
) *HttpIpfs {
	cfg := &httpOptions{}
	for _, opt := range opts {
		opt(cfg)
	}

	return &HttpIpfs{
		ctx:  ctx,
		lsys: lsys,
		cfg:  cfg,
	}
}

func (hi *HttpIpfs) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	ctx := hi.ctx
	if hi.cfg.MaxResponseDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, hi.cfg.MaxResponseDuration)
		defer cancel()
	}

	logError := func(status int, err error) {
		res.WriteHeader(status)
		_, _ = res.Write([]byte(err.Error()))
		if lrw, ok := res.(ErrorLogger); ok {
			lrw.LogError(status, err)
		} else {
			logger.Debugf("Error handling request from [%s] for [%s] status=%d, msg=%s", req.RemoteAddr, req.URL, status, err.Error())
		}
	}

	// filter out everything but GET requests
	switch req.Method {
	case http.MethodGet:
		break
	default:
		res.Header().Add("Allow", http.MethodGet)
		logError(http.StatusMethodNotAllowed, errors.New("method not allowed"))
		return
	}

	path := datamodel.ParsePath(req.URL.Path)
	_, path = path.Shift() // remove /ipfs

	// check if CID path param is missing
	if path.Len() == 0 {
		// not a valid path to hit
		logError(http.StatusNotFound, errors.New("not found"))
		return
	}

	includeDupes, err := trustlesshttp.CheckFormat(req)
	if err != nil {
		logError(http.StatusBadRequest, err)
		return
	}

	fileName, err := trustlesshttp.ParseFilename(req)
	if err != nil {
		logError(http.StatusBadRequest, err)
		return
	}

	// validate CID path parameter
	var cidSeg datamodel.PathSegment
	cidSeg, path = path.Shift()
	rootCid, err := cid.Parse(cidSeg.String())
	if err != nil {
		logError(http.StatusBadRequest, errors.New("failed to parse CID path parameter"))
		return
	}

	dagScope, err := trustlesshttp.ParseScope(req)
	if err != nil {
		logError(http.StatusBadRequest, err)
		return
	}

	byteRange, err := trustlesshttp.ParseByteRange(req)
	if err != nil {
		logError(http.StatusBadRequest, err)
		return
	}

	request := trustlessutils.Request{
		Root:       rootCid,
		Path:       path.String(),
		Scope:      dagScope,
		Bytes:      byteRange,
		Duplicates: includeDupes,
	}

	if fileName == "" {
		fileName = fmt.Sprintf("%s%s", rootCid.String(), trustlesshttp.FilenameExtCar)
	}

	bytesWrittenCh := make(chan struct{})
	writer := newIpfsResponseWriter(res, hi.cfg.MaxResponseBytes, func() {
		// called once we start writing blocks into the CAR (on the first Put())
		res.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileName))
		res.Header().Set("Cache-Control", trustlesshttp.ResponseCacheControlHeader)
		res.Header().Set("Content-Type", trustlesshttp.ResponseContentTypeHeader(includeDupes))
		res.Header().Set("Etag", request.Etag())
		res.Header().Set("X-Content-Type-Options", "nosniff")
		res.Header().Set("X-Ipfs-Path", "/"+datamodel.ParsePath(req.URL.Path).String())
		close(bytesWrittenCh)
	})

	if err := StreamCar(ctx, hi.lsys, writer, request); err != nil {
		logError(http.StatusInternalServerError, err)
		select {
		case <-bytesWrittenCh:
			logger.Debugw("unclean close", "cid", rootCid, "err", err)
			if err := closeWithUnterminatedChunk(res); err != nil {
				logger.Infow("unable to send early termination", "err", err)
			}
			return
		default:
		}
		logger.Debugw("error streaming CAR", "cid", rootCid, "err", err)
	}
}

var _ io.Writer = (*ipfsResponseWriter)(nil)

type ipfsResponseWriter struct {
	w         io.Writer
	fn        func()
	byteCount int
	once      sync.Once
	maxBytes  int64
}

func newIpfsResponseWriter(w io.Writer, maxBytes int64, fn func()) *ipfsResponseWriter {
	return &ipfsResponseWriter{
		w:        w,
		maxBytes: maxBytes,
		fn:       fn,
	}
}

func (w *ipfsResponseWriter) Write(p []byte) (int, error) {
	w.once.Do(w.fn)
	w.byteCount += len(p)
	if w.maxBytes > 0 && int64(w.byteCount) > w.maxBytes {
		return 0, fmt.Errorf("response too large: %d bytes", w.byteCount)
	}
	return w.w.Write(p)
}

// closeWithUnterminatedChunk attempts to take control of the the http conn and terminate the stream early
//
// (copied from github.com/filecoin-project/lassie/pkg/server/http/ipfs.go)
func closeWithUnterminatedChunk(res http.ResponseWriter) error {
	hijacker, ok := res.(http.Hijacker)
	if !ok {
		return errors.New("unable to access hijack interface")
	}
	conn, buf, err := hijacker.Hijack()
	if err != nil {
		return fmt.Errorf("unable to access conn through hijack interface: %w", err)
	}
	if _, err := buf.Write(trustlesshttp.ResponseChunkDelimeter); err != nil {
		return fmt.Errorf("writing response chunk delimiter: %w", err)
	}
	if err := buf.Flush(); err != nil {
		return fmt.Errorf("flushing buff: %w", err)
	}
	// attempt to close just the write side
	if err := conn.Close(); err != nil {
		return fmt.Errorf("closing write conn: %w", err)
	}
	return nil
}
