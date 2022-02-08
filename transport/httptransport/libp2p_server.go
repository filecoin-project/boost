package httptransport

import (
	"context"
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
	"github.com/libp2p/go-libp2p-core/peer"
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

	*transfersMgr
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
		h:            h,
		auth:         auth,
		bstore:       bstore,
		cfg:          cfg,
		bicm:         bcim,
		transfersMgr: newTransfersManager(),
	}
}

func (s *Libp2pCarServer) ID() peer.ID {
	return s.h.ID()
}

func (s *Libp2pCarServer) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Start up the transfers manager
	go s.transfersMgr.run(s.ctx)

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
	s.transfersMgr.awaitStop(ctx)

	if lerr != nil {
		return lerr
	}
	if serr != nil {
		return serr
	}
	return bicmerr
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

func (s *Libp2pCarServer) checkAuth(r *http.Request) (string, *AuthValue, *httpError) {
	ctx := r.Context()

	// Get auth token from Authorization header
	_, authToken, ok := r.BasicAuth()
	if !ok {
		return "", nil, &httpError{
			error: errors.New("rejected request with no Authorization header"),
			code:  401,
		}
	}

	// Get auth value from auth datastore
	val, err := s.auth.Get(ctx, authToken)
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

	return authToken, val, nil
}

func (s *Libp2pCarServer) serveContent(w http.ResponseWriter, r *http.Request, authToken string, val *AuthValue, bic *car.BlockInfoCache) error {
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

func (s *Libp2pCarServer) sendCar(r *http.Request, w http.ResponseWriter, val *AuthValue, authToken string, content *car.CarReaderSeeker) error {
	// Create transfer
	xfer := newLibp2pTransfer(val, authToken, s.h.ID().String(), r.RemoteAddr, content)

	// Add transfer to the list of active transfers
	fireEvent, err := s.transfersMgr.add(xfer)
	if err != nil {
		return fmt.Errorf("creating new transfer: %w", err)
	}

	// Fire transfer started event
	logParams := []interface{}{"id", val.ID, "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid, "size", val.Size}
	log.Infow("starting transfer", logParams...)
	fireEvent(xfer.State())

	// Fire progress events during transfer.
	// We fire a progress event each time bytes are read from the CAR.
	// Note that we can't fire events when bytes are written to the HTTP stream
	// because there may be some headers written first.
	var errLk sync.Mutex
	readEmitter := &readEmitter{rs: content, emit: func(totalRead uint64, e error) {
		errLk.Lock()
		defer errLk.Unlock()

		if e != nil {
			err = e
		}
		// Stop firing sent events after an error occurs
		if err == nil {
			st := xfer.setSent(totalRead)
			fireEvent(st)
		}
	}}

	// http.ServeContent ignores errors when writing to the stream, so we
	// replace the writer with a class that watches for errors
	writeErrWatcher := &writeErrorWatcher{ResponseWriter: w, onError: func(e error) {
		errLk.Lock()
		defer errLk.Unlock()
		err = e
	}}

	// Send the content
	http.ServeContent(writeErrWatcher, r, "", time.Time{}, readEmitter)

	// Check if there was an error during the transfer
	if err != nil {
		log.Infow("transfer failed", append(logParams, "error", err))
	} else {
		log.Infow("completed serving request", logParams)
	}

	st := xfer.setComplete(err)
	fireEvent(st)

	return nil
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
		actions: make(chan *xferAction, 256),
		done:    make(chan struct{}),
	}
}

var ErrTransferNotFound = errors.New("transfer not found")

// Get gets a transfer by id.
// Returns ErrTransferNotFound if there is no active transfer with that id.
func (m *transfersMgr) Get(id string) (*Libp2pTransfer, error) {
	m.transfersLk.RLock()
	defer m.transfersLk.RUnlock()

	xfer, ok := m.transfers[id]
	if !ok {
		return nil, ErrTransferNotFound
	}
	return xfer, nil
}

// CancelTransfer cancels the transfer with the given id (closing its
// read / write pipe), and waits for the error or completed event to be
// fired before returning.
// It returns the state that the transfer is in after being canceled (either
// completed or errored out).
func (m *transfersMgr) CancelTransfer(ctx context.Context, id string) (*types.TransferState, error) {
	// Remove the transfer from the list of active transfers
	m.transfersLk.Lock()
	xfer, ok := m.transfers[id]
	if ok {
		delete(m.transfers, id)
	}
	m.transfersLk.Unlock()

	if !ok {
		return nil, ErrTransferNotFound
	}

	// Cancel the transfer
	return xfer.cancel(ctx)
}

type MatchFn func(xfer *Libp2pTransfer) (bool, error)

// Matching returns all transfers selected by the match function
func (m *transfersMgr) Matching(match MatchFn) ([]*Libp2pTransfer, error) {
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

// Adds the transfer to the list of active transfers.
// It returns a function that fires an event with the current state of the
// transfer. The function is guaranteed to fire the event after the transfer
// has been added to the list of active transfers.
func (m *transfersMgr) add(xfer *Libp2pTransfer) (func(types.TransferState), error) {
	// Queue up an action that adds the transfer to the map of transfers
	err := m.enqueueAction(&xferAction{xfer: xfer})
	if err != nil {
		return nil, err
	}

	// fireEvent is called when the state changes (eg data is sent, or there's an error)
	fireEvent := func(st types.TransferState) {
		// Queue up an action that fires an event with the current transfer state.
		// Note: enqueueAction returns an error only if the context is cancelled,
		// which we can safely ignore.
		_ = m.enqueueAction(&xferAction{xfer: xfer, transferState: &st}) //nolint:errcheck
	}

	return fireEvent, nil
}

type xferAction struct {
	xfer          *Libp2pTransfer
	transferState *types.TransferState
}

// enqueueAction adds an action to the queue, or returns context.Canceled if
// the transfersMgr has been shut down
func (m *transfersMgr) enqueueAction(evt *xferAction) error {
	select {
	case <-m.ctx.Done():
		return m.ctx.Err()
	case m.actions <- evt:
		return nil
	}
}

// run processes actions on the queue until the context is cancelled
func (m *transfersMgr) run(ctx context.Context) {
	defer close(m.done)

	m.ctx = ctx

	processAction := func(action *xferAction) {
		xfer := action.xfer
		// There are two types of action:

		// 1. Add a new transfer to the list of active transfers
		if action.transferState == nil {
			m.transfersLk.Lock()
			if existing, ok := m.transfers[xfer.ID]; ok {
				xfer.isRestart = true
				// Close any existing transfer with the same id and prevent
				// it from firing any more events
				existing.close(ctx)
			}
			m.transfers[xfer.ID] = xfer
			m.transfersLk.Unlock()
			return
		}

		// 2. Publish an event
		if xfer.isRestart && action.transferState.Status == types.TransferStatusStarted {
			// If this transfer replaces an event with the same id, it's a
			// restart
			action.transferState.Status = types.TransferStatusRestarted
		}
		if !xfer.closed {
			m.publishEvent(xfer.ID, action.transferState)
			xfer.eventPublished(action.transferState.Status)
		}
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

func (m *transfersMgr) publishEvent(id string, state *types.TransferState) {
	m.eventListenersLk.Lock()
	for l := range m.eventListeners {
		(*l)(id, *state)
	}
	m.eventListenersLk.Unlock()
}

type EventListenerFn func(id string, st types.TransferState)
type UnsubFn func()

func (m *transfersMgr) Subscribe(cb EventListenerFn) UnsubFn {
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
	ID          string
	PayloadCid  cid.Cid
	ProposalCid cid.Cid
	CreatedAt   time.Time
	AuthToken   string
	LocalAddr   string
	RemoteAddr  string
	content     *car.CarReaderSeeker
	// indicates whether this transfer replaces a previous transfer with the
	// same id
	isRestart bool

	lk     sync.RWMutex
	err    error
	sent   uint64
	status types.TransferStatus

	// Set when the transfer is replaced with a new transfer with the same id.
	// This prevents the replaced transfer from firing any more events.
	closed bool

	// When the error or complete event has been fired
	eventsDrained chan struct{}
}

func newLibp2pTransfer(val *AuthValue, authToken string, localAddr string, remoteAddr string, content *car.CarReaderSeeker) *Libp2pTransfer {
	return &Libp2pTransfer{
		ID:            val.ID,
		PayloadCid:    val.PayloadCid,
		ProposalCid:   val.ProposalCid,
		CreatedAt:     time.Now(),
		AuthToken:     authToken,
		LocalAddr:     localAddr,
		RemoteAddr:    remoteAddr,
		content:       content,
		status:        types.TransferStatusStarted,
		eventsDrained: make(chan struct{}),
	}
}

// setSent is called when some bytes are sent.
// Returns the state of the transfer after the sent amount is updated.
func (t *Libp2pTransfer) setSent(sent uint64) types.TransferState {
	t.lk.Lock()
	defer t.lk.Unlock()

	t.sent = sent
	t.status = types.TransferStatusOngoing

	return t.state()
}

// setComplete is called when the transfer completes or there is an error
// Returns the state of the transfer after the complete status is applied.
func (t *Libp2pTransfer) setComplete(err error) types.TransferState {
	t.lk.Lock()
	defer t.lk.Unlock()

	t.err = err
	if t.err == nil {
		t.status = types.TransferStatusCompleted
	} else {
		t.status = types.TransferStatusFailed
	}

	return t.state()
}

func (t *Libp2pTransfer) State() types.TransferState {
	t.lk.RLock()
	defer t.lk.RUnlock()

	return t.state()
}

func (t *Libp2pTransfer) state() types.TransferState {
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
		PayloadCid: t.PayloadCid,
	}
}

// cancel permanently fails the transfer, and waits for the error or completed
// event to be fired before returning.
// It returns the state that the transfer is in after being canceled (either
// completed or errored out).
func (t *Libp2pTransfer) cancel(ctx context.Context) (*types.TransferState, error) {
	// Cancel the read / write stream
	err := t.content.Cancel(ctx)
	if err != nil {
		return nil, err
	}

	// Wait for the error or completed event to be fired
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.eventsDrained:
		t.lk.Lock()
		defer t.lk.Unlock()
		st := t.state()
		return &st, nil
	}
}

func (t *Libp2pTransfer) close(ctx context.Context) {
	t.closed = true

	// Cancel the read / write stream
	go t.content.Cancel(ctx) //nolint:errcheck
}

// eventPublished is called when an event for this transfer is emitted
func (t *Libp2pTransfer) eventPublished(status types.TransferStatus) {
	// Close the eventsDrained channel when the transfer completes or errors out
	// so that any select against the channel is notified.
	if status == types.TransferStatusFailed || status == types.TransferStatusCompleted {
		close(t.eventsDrained)
	}
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
