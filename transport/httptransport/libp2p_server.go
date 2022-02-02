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

	xferMgr *transfersMgr
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
		h:       h,
		auth:    auth,
		bstore:  bstore,
		cfg:     cfg,
		bicm:    bcim,
		xferMgr: newTransfersManager(),
	}
}

func (s *Libp2pCarServer) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Start up the transfers manager
	go s.xferMgr.run(s.ctx)

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

func (s *Libp2pCarServer) Stop(ctx context.Context) error {
	bicmerr := s.bicm.Close()
	s.cancel()
	lerr := s.netListener.Close()
	serr := s.server.Close()

	// Wait for all events to be processed
	s.xferMgr.awaitStop(ctx)

	if lerr != nil {
		return lerr
	}
	if serr != nil {
		return serr
	}
	return bicmerr
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
func (s *Libp2pCarServer) CleanupPreparedRequest(ctx context.Context, authToken string) error {
	// Delete the auth token for the request
	aterr := s.auth.Delete(ctx, authToken)

	// Cancel the transfer
	xfererr := s.xferMgr.cancel(ctx, authToken)

	if aterr != nil {
		return aterr
	}
	return xfererr
}

// handler is called by the http library to handle an incoming HTTP request
func (s *Libp2pCarServer) handler(w http.ResponseWriter, r *http.Request) {
	// Check authentication
	authToken, authVal, herr := s.checkAuth(r)
	if herr != nil {
		log.Infow("data transfer request failed", "code", herr.code, "err", herr.error)
		w.WriteHeader(herr.code)
		return
	}

	// Get a block info cache for the CarOffsetWriter
	bic := s.bicm.Get(authVal.PayloadCid)
	err := s.serveContent(w, r, authToken, authVal, bic)
	s.bicm.Unref(authVal.PayloadCid, err)
}

func (s *Libp2pCarServer) checkAuth(r *http.Request) (string, *authValue, *httpError) {
	ctx := r.Context()

	// Get auth token from Authorization header
	_, authToken, ok := r.BasicAuth()
	if !ok {
		return "", nil, &httpError{
			error: errors.New("rejected request with no Authorization header"),
			code:  401,
		}
	}

	// Get proposal CID from auth datastore
	authValueJson, err := s.auth.Get(ctx, authToken)
	if xerrors.Is(err, ErrTokenNotFound) {
		return "", nil, &httpError{
			error: errors.New("rejected unrecognized auth token"),
			code:  401,
		}
	} else if err != nil {
		return "", nil, &httpError{
			error: fmt.Errorf("getting key from datastore: %w", err),
			code:  500,
		}
	}

	var val authValue
	err = json.Unmarshal(authValueJson, &val)
	if err != nil {
		return "", nil, &httpError{
			error: fmt.Errorf("unmarshaling json from datastore: %w", err),
			code:  500,
		}
	}

	return authToken, &val, nil
}

func (s *Libp2pCarServer) serveContent(w http.ResponseWriter, r *http.Request, authToken string, val *authValue, bic *car.BlockInfoCache) error {
	ctx := r.Context()

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
	return s.sendCar(r, w, val, authToken, content)
}

func (s *Libp2pCarServer) sendCar(r *http.Request, w http.ResponseWriter, val *authValue, authToken string, content *car.CarReaderSeeker) error {
	// Create transfer
	xfer, fireEvent, err := s.xferMgr.newTransfer(val, authToken, s.h.ID().String(), r.RemoteAddr, content)
	if err != nil {
		return fmt.Errorf("creating new transfer: %w", err)
	}

	// Fire transfer started event
	log.Infow("starting transfer", "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid)
	fireEvent()

	// Fire progress events during transfer.
	// We fire a progress event each time bytes are read from the CAR.
	// Note that we can't fire events when bytes are written to the HTTP stream
	// because there may be some transformation first (eg adding headers etc).
	readEmitter := &readEmitter{rs: content, emit: func(totalRead uint64, err error) {
		if err != nil {
			// Note that the error event is fired at the end of sendCar so that
			// it is fired after the read has completed
			xfer.onError(err)
			return
		}
		xfer.onSent(totalRead)
		fireEvent()
	}}

	// http.ServeContent ignores errors when writing to the stream, so we
	// replace the writer with a class that watches for errors
	writeErrWatcher := &writeErrorWatcher{ResponseWriter: w, onError: xfer.onError}

	// Send the content
	http.ServeContent(writeErrWatcher, r, "", time.Time{}, readEmitter)

	// Check if there was an error during the transfer
	if err := xfer.error(); err != nil {
		// There was an error, fire a transfer error event
		log.Infow("transfer failed", "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid, "error", err)
		fireEvent()

		return err
	}

	// Fire transfer complete event
	log.Infow("completed serving request", "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid)
	xfer.onCompleted()
	fireEvent()

	return nil
}

type EventListenerFn func(dbid uint, st types.TransferState)
type UnsubFn func()

func (s *Libp2pCarServer) Subscribe(cb EventListenerFn) UnsubFn {
	return s.xferMgr.subscribe(cb)
}

type MatchFn func(xfer *Libp2pTransfer) (bool, error)

// Matching returns all transfers selected by the match function
func (s *Libp2pCarServer) Matching(match MatchFn) ([]*Libp2pTransfer, error) {
	return s.xferMgr.matching(func(xfer *Libp2pTransfer) (bool, error) {
		return match(xfer)
	})
}

// transfersMgr keeps a list of active transfers.
// It provides methods to subscribe to and fire events, and runs a
// go-routine to process new transfers and transfer events.
type transfersMgr struct {
	transfersLk sync.RWMutex
	transfers   map[string]*Libp2pTransfer

	eventListenersLk sync.Mutex
	eventListeners   map[*EventListenerFn]struct{}

	ctx     context.Context
	actions chan *xferAction
	done    chan struct{}
}

func newTransfersManager() *transfersMgr {
	return &transfersMgr{
		transfers:      make(map[string]*Libp2pTransfer),
		eventListeners: make(map[*EventListenerFn]struct{}),
		// Keep some buffer in the channel so that it doesn't get blocked when
		// there is a burst of events to process.
		actions: make(chan *xferAction, 1024),
		done:    make(chan struct{}),
	}
}

// cancel removes the transfer from the list of active transfers and cancels
// the transfer (closing its read / write pipe)
func (m *transfersMgr) cancel(ctx context.Context, authToken string) error {
	// Remove the transfer from the list of active transfers
	m.transfersLk.Lock()
	xfer, ok := m.transfers[authToken]
	if ok {
		delete(m.transfers, authToken)
	}
	m.transfersLk.Unlock()

	if ok {
		// Cancel the transfer
		return xfer.cancel(ctx, fmt.Errorf("cancelled"))
	}
	return nil
}

// matching returns the transfers selected by the match function
func (m *transfersMgr) matching(match MatchFn) ([]*Libp2pTransfer, error) {
	m.transfersLk.RLock()
	defer m.transfersLk.RUnlock()

	matching := make([]*Libp2pTransfer, 0)
	for _, xfer := range m.transfers {
		matches, err := match(xfer)
		if err != nil {
			return nil, err
		}
		if matches {
			matching = append(matching, xfer)
		}
	}

	return matching, nil
}

// newTransfer creates a new transfer and adds it to the list of active transfers.
// It returns the transfer and a function that fires an event with the current
// state of the transfer. The function is guaranteed to fire the event after the
// transfer has been added to the list of active transfers.
func (m *transfersMgr) newTransfer(val *authValue, authToken string, localAddr string, remoteAddr string, content *car.CarReaderSeeker) (*Libp2pTransfer, func(), error) {
	xfer := &Libp2pTransfer{
		CreatedAt:  time.Now(),
		DBID:       val.DBID,
		AuthToken:  authToken,
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
		content:    content,
		status:     types.TransferStatusStarted,
	}

	// Queue up an action that adds the transfer to the map of transfers
	err := m.enqueueAction(&xferAction{authToken: authToken, xfer: xfer})
	if err != nil {
		return nil, nil, err
	}

	// fireEvent is called when the state changes (eg data is sent, or there's an error)
	fireEvent := func() {
		// Queue up an action that fires an event with the current transfer state.
		// Note: enqueueAction returns an error only if the context is cancelled,
		// which we can safely ignore.
		st := xfer.State()
		_ = m.enqueueAction(&xferAction{dbID: val.DBID, transferState: &st}) //nolint:errcheck
	}

	return xfer, fireEvent, nil
}

type xferAction struct {
	authToken     string
	xfer          *Libp2pTransfer
	dbID          uint
	transferState *types.TransferState
}

func (m *transfersMgr) enqueueAction(evt *xferAction) error {
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case m.actions <- evt:
		return nil
	}
}

func (m *transfersMgr) run(ctx context.Context) {
	defer close(m.done)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	m.ctx = ctx

	processAction := func(action *xferAction) {
		// There are two types of action:

		// 1. Add a new transfer to the list of active transfers
		if action.xfer != nil {
			m.transfersLk.Lock()
			if existing, ok := m.transfers[action.authToken]; ok {
				// Cancel any existing transfer with the same auth token
				go existing.cancel(ctx, errors.New("transfer restarted")) //nolint:errcheck
			}
			m.transfers[action.authToken] = action.xfer
			m.transfersLk.Unlock()
			return
		}

		// 2. Publish an event
		m.publishEvent(action.dbID, action.transferState)
	}

	// When the event queue is shut down, drain the remaining events
	drainEvents := func() {
		for {
			select {
			case action := <-m.actions:
				processAction(action)
			default:
				return
			}
		}
	}
	defer drainEvents()

	// Process events until the event queue is shut down
	for {
		select {
		case <-m.ctx.Done():
			return
		case action := <-m.actions:
			processAction(action)
		}
	}
}

// awaitStop blocks until the context expires or the event queue has completed
// and drained
func (m *transfersMgr) awaitStop(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-m.done:
	}
}

func (m *transfersMgr) publishEvent(dbid uint, state *types.TransferState) {
	m.eventListenersLk.Lock()
	for l := range m.eventListeners {
		(*l)(dbid, *state)
	}
	m.eventListenersLk.Unlock()
}

func (m *transfersMgr) subscribe(cb EventListenerFn) UnsubFn {
	m.eventListenersLk.Lock()
	m.eventListeners[&cb] = struct{}{}
	m.eventListenersLk.Unlock()

	return func() {
		m.eventListenersLk.Lock()
		delete(m.eventListeners, &cb)
		m.eventListenersLk.Unlock()
	}
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
func (t *Libp2pTransfer) cancel(ctx context.Context, err error) error {
	t.lk.Lock()

	if t.cancelled {
		t.lk.Unlock()
		return nil
	}

	t.cancelled = true
	t.err = err
	t.status = types.TransferStatusFailed

	t.lk.Unlock()

	return t.content.Cancel(ctx)
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
