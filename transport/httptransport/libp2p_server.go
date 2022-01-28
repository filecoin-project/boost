package httptransport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/filecoin-project/boost/car"
	"github.com/filecoin-project/boost/transport/types"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/libp2p/go-libp2p-core/host"
	gostream "github.com/libp2p/go-libp2p-gostream"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"
)

// Libp2pCarServer serves deal data by matching an auth token to the root CID
// of a DAG in a blockstore, and serving the data as a CAR
type Libp2pCarServer struct {
	h      host.Host
	auth   *AuthTokenDB
	bstore blockstore.Blockstore
	cfg    ServerConfig
	bicm   car.BlockInfoCacheManager

	ctx         context.Context
	cancel      context.CancelFunc
	server      *http.Server
	netListener net.Listener

	eventListenersLk sync.Mutex
	eventListeners   map[*EventListenerFn]struct{}

	transfersLk sync.RWMutex
	transfers   map[cid.Cid]*Libp2pTransfer
}

type ServerConfig struct {
	AnnounceAddr          multiaddr.Multiaddr
	BlockInfoCacheManager car.BlockInfoCacheManager
}

func NewLibp2pCarServer(h host.Host, auth *AuthTokenDB, bstore blockstore.Blockstore, cfg ServerConfig) *Libp2pCarServer {
	bcim := cfg.BlockInfoCacheManager
	if bcim == nil {
		bcim = car.NewRefCountBICM()
	}
	return &Libp2pCarServer{
		h:              h,
		auth:           auth,
		bstore:         bstore,
		cfg:            cfg,
		bicm:           bcim,
		eventListeners: make(map[*EventListenerFn]struct{}),
		transfers:      make(map[cid.Cid]*Libp2pTransfer),
	}
}

func (s *Libp2pCarServer) Start() error {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	// Listen on HTTP over libp2p
	listener, err := gostream.Listen(s.h, types.DataTransferProtocol)
	if err != nil {
		return fmt.Errorf("starting gostream listener: %w", err)
	}

	s.netListener = listener

	handler := http.NewServeMux()
	handler.HandleFunc("/", s.handler)
	s.server = &http.Server{
		Handler: handler,
		// This context will be the parent of the context associated with all
		// incoming requests
		BaseContext: func(listener net.Listener) context.Context {
			return s.ctx
		},
	}
	go s.server.Serve(listener) //nolint:errcheck

	return nil
}

func (s *Libp2pCarServer) Stop() error {
	s.cancel()
	lerr := s.netListener.Close()
	serr := s.server.Close()

	if lerr != nil {
		return lerr
	}
	return serr
}

// authValue is the data associated with an auth token in the auth DB
type authValue struct {
	ProposalCid cid.Cid
	PayloadCid  cid.Cid
	Size        uint64
	DBID        uint
}

type DealTransfer struct {
	MultiAddress multiaddr.Multiaddr
	AuthToken    string
}

// PrepareForDataRequest creates an auth token and saves some parameters
// in the datastore, in preparation for a request with that auth token.
func (s *Libp2pCarServer) PrepareForDataRequest(ctx context.Context, dbID uint, proposalCid cid.Cid, payloadCid cid.Cid, size uint64) (*DealTransfer, error) {
	authValueJson, err := json.Marshal(&authValue{
		ProposalCid: proposalCid,
		PayloadCid:  payloadCid,
		Size:        size,
		DBID:        dbID,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling auth value JSON: %w", err)
	}

	authToken, err := s.auth.Put(ctx, authValueJson)
	if err != nil {
		return nil, fmt.Errorf("adding new auth token: %w", err)
	}

	dt := &DealTransfer{
		MultiAddress: s.cfg.AnnounceAddr,
		AuthToken:    authToken,
	}
	return dt, nil
}

// CleanupPreparedRequest is called when a request cannot be fulfilled,
// for example because the provider rejected the deal proposal
func (s *Libp2pCarServer) CleanupPreparedRequest(ctx context.Context, authToken string, proposalCid cid.Cid) error {
	// Clean up any transfer that was created
	s.transfersLk.Lock()
	xfer, ok := s.transfers[proposalCid]
	if ok {
		delete(s.transfers, proposalCid)
		xfer.Cancel(fmt.Errorf("cancelled"))
	}
	s.transfersLk.Unlock()

	// Delete the auth token for the request
	return s.auth.Delete(ctx, authToken)
}

func (s *Libp2pCarServer) handler(w http.ResponseWriter, r *http.Request) {
	err := s.handleNewReq(w, r)
	if err != nil {
		log.Infow("data transfer request failed", "code", err.code, "err", err)
		w.WriteHeader(err.code)
	}
}

func (s *Libp2pCarServer) handleNewReq(w http.ResponseWriter, r *http.Request) *httpError {
	ctx := r.Context()

	// Get auth token from Authorization header
	_, authToken, ok := r.BasicAuth()
	if !ok {
		return &httpError{
			error: errors.New("rejected request with no Authorization header"),
			code:  401,
		}
	}

	// Get proposal CID from auth datastore
	authValueJson, err := s.auth.Get(ctx, authToken)
	if xerrors.Is(err, ErrTokenNotFound) {
		return &httpError{
			error: errors.New("rejected unrecognized auth token"),
			code:  401,
		}
	} else if err != nil {
		return &httpError{
			error: fmt.Errorf("getting key from datastore: %w", err),
			code:  500,
		}
	}

	var val authValue
	err = json.Unmarshal(authValueJson, &val)
	if err != nil {
		return &httpError{
			error: fmt.Errorf("unmarshaling json from datastore: %w", err),
			code:  500,
		}
	}

	// Get a block info cache for the CarOffsetWriter
	bic := s.bicm.Get(val.PayloadCid)
	defer s.bicm.Unref(val.PayloadCid)

	// Create a CarOffsetWriter and a reader for it
	cow := car.NewCarOffsetWriter(val.PayloadCid, s.bstore, bic)
	content := car.NewCarReaderSeeker(ctx, cow, val.Size)

	// Set the Content-Type header explicitly so that http.ServeContent doesn't
	// try to do it implicitly
	w.Header().Set("Content-Type", "application/car")

	if r.Method == "HEAD" {
		// For an HTTP HEAD request we don't send any data (just headers)
		http.ServeContent(w, r, "", time.Time{}, content)
		return nil
	}

	// Send the CAR file
	err = s.sendCar(r, w, val, authToken, content)
	if err != nil {
		return &httpError{
			error: err,
			code:  500,
		}
	}

	return nil
}

func (s *Libp2pCarServer) sendCar(r *http.Request, w http.ResponseWriter, val authValue, authToken string, content *car.CarReaderSeeker) error {
	// Create a new transfer
	s.transfersLk.Lock()
	_, isRetry := s.transfers[val.ProposalCid]
	xfer := &Libp2pTransfer{
		CreatedAt:  time.Now(),
		DBID:       val.DBID,
		AuthToken:  authToken,
		LocalAddr:  s.h.ID().String(),
		RemoteAddr: r.RemoteAddr,
		content:    content,
	}
	s.transfers[val.ProposalCid] = xfer
	s.transfersLk.Unlock()

	if isRetry {
		log.Infow("restarting transfer", "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid)
	} else {
		log.Infow("starting transfer", "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid)
	}

	// Fire event before starting transfer
	xfer.onStarted()
	s.fireEvent(val.DBID, xfer.State())

	// Fire progress events during transfer.
	// We fire a progress event each time bytes are read from the CAR.
	// Note that we can't fire events when bytes are written to the HTTP stream
	// because there may be some transformation first (eg adding headers etc).
	readEmitter := &readEmitter{rs: content, emit: func(totalRead uint64, err error) {
		if err != nil {
			xfer.onError(err)
			// Note: The error event gets fired at the end of the method
			return
		}
		xfer.onSent(totalRead)
		s.fireEvent(val.DBID, xfer.State())
	}}

	// http.ServeContent ignores errors when writing to the stream, so we
	// replace the writer with a class that watches for errors
	writeErrWatcher := &writeErrorWatcher{ResponseWriter: w, onError: xfer.onError}

	// Send the content
	http.ServeContent(writeErrWatcher, r, "", time.Time{}, readEmitter)

	// Check if there was an error during the transfer
	if err := xfer.error(); err != nil {
		// There was an error, fire an error event
		log.Infow("transfer failed", "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid, "error", err)
		s.fireEvent(val.DBID, xfer.State())

		return nil
	}

	// Fire complete event
	log.Infow("completed serving request", "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid)
	xfer.onCompleted()
	s.fireEvent(val.DBID, xfer.State())

	return nil
}

type EventListenerFn func(dbid uint, st types.TransferState)
type UnsubFn func()

func (s *Libp2pCarServer) fireEvent(dbid uint, state types.TransferState) {
	s.eventListenersLk.Lock()
	for l := range s.eventListeners {
		(*l)(dbid, state)
	}
	s.eventListenersLk.Unlock()
}

func (s *Libp2pCarServer) Subscribe(cb EventListenerFn) UnsubFn {
	s.eventListenersLk.Lock()
	s.eventListeners[&cb] = struct{}{}
	s.eventListenersLk.Unlock()

	return func() {
		s.eventListenersLk.Lock()
		delete(s.eventListeners, &cb)
		s.eventListenersLk.Unlock()
	}
}

// ForEach is called once for each active data transfer
func (s *Libp2pCarServer) ForEach(cb func(xfer *Libp2pTransfer) error) error {
	return s.Filter(func(xfer *Libp2pTransfer) (bool, error) {
		return true, cb(xfer)
	})
}

// Filter is called once for each active data transfer. If the callback
// function returns false, the transfer is removed.
func (s *Libp2pCarServer) Filter(cb func(xfer *Libp2pTransfer) (bool, error)) error {
	s.transfersLk.RLock()
	defer s.transfersLk.RUnlock()

	for propcid, xfer := range s.transfers {
		keep, err := cb(xfer)
		if err != nil {
			return err
		}
		if !keep {
			delete(s.transfers, propcid)
		}
	}
	return nil
}

// Libp2pTransfer keeps track of a data transfer
type Libp2pTransfer struct {
	CreatedAt  time.Time
	DBID       uint
	AuthToken  string
	LocalAddr  string
	RemoteAddr string
	content    *car.CarReaderSeeker

	lk        sync.RWMutex
	cancelled bool
	err       error
	sent      uint64
	status    types.TransferStatus
}

func (t *Libp2pTransfer) error() error {
	t.lk.RLock()
	defer t.lk.RUnlock()

	return t.err
}

// onError is called when a transfer error occurs
func (t *Libp2pTransfer) onError(err error) {
	t.lk.Lock()
	defer t.lk.Unlock()

	if t.err != nil {
		return
	}
	t.err = err
	t.status = types.TransferStatusFailed
}

// onStarted is called when a transfer starts / restarts
func (t *Libp2pTransfer) onStarted() {
	t.lk.Lock()
	defer t.lk.Unlock()

	// If the transfer has been permanently cancelled, it cannot be restarted.
	// Note: if there was a transfer error it can be restarted.
	if t.cancelled {
		return
	}

	t.err = nil
	t.status = types.TransferStatusStarted
}

// onSent is called when some bytes are sent
func (t *Libp2pTransfer) onSent(sent uint64) {
	t.lk.Lock()
	defer t.lk.Unlock()

	// If there was a data transfer error, ignore any subsequent sent events
	if t.status == types.TransferStatusFailed {
		return
	}

	t.sent = sent
	t.status = types.TransferStatusOngoing
}

// onCompleted is called when the transfer completes successfully
func (t *Libp2pTransfer) onCompleted() {
	t.lk.Lock()
	defer t.lk.Unlock()

	// If there was a data transfer error, ignore the completed event
	if t.status == types.TransferStatusFailed {
		return
	}

	t.status = types.TransferStatusCompleted
}

func (t *Libp2pTransfer) State() types.TransferState {
	t.lk.RLock()
	defer t.lk.RUnlock()

	msg := ""
	if t.err != nil {
		msg = t.err.Error()
	} else {
		msg = fmt.Sprintf("transferred %d bytes", t.sent)
		if t.sent == 0 {
			msg = "pull data transfer queued"
		}
	}
	return types.TransferState{
		LocalAddr:  t.LocalAddr,
		RemoteAddr: t.RemoteAddr,
		Status:     t.status,
		Sent:       t.sent,
		Message:    msg,
	}
}

// Cancel permanently fails the transfer
func (t *Libp2pTransfer) Cancel(err error) {
	t.lk.Lock()
	defer t.lk.Unlock()

	if t.cancelled {
		return
	}

	t.content.CloseWithError(err) //nolint:errcheck
	t.cancelled = true
	t.err = err
	t.status = types.TransferStatusFailed
}

// readEmitter emits an event with the current offset into the read stream
// each time there is a read
type readEmitter struct {
	rs     io.ReadSeeker
	emit   func(count uint64, err error)
	offset uint64
}

func (e *readEmitter) Seek(offset int64, whence int) (int64, error) {
	newOffset, err := e.rs.Seek(offset, whence)
	e.offset = uint64(newOffset)
	return newOffset, err
}

func (e *readEmitter) Read(p []byte) (n int, err error) {
	count, err := e.rs.Read(p)
	e.offset += uint64(count)
	e.emit(e.offset, err)
	return count, err
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
