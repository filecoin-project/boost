package httptransport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/filecoin-project/boost/transport/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/v2"
	"github.com/libp2p/go-libp2p-core/host"
	gostream "github.com/libp2p/go-libp2p-gostream"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"
)

// Libp2pCarServer serves deal data by matching an auth token to the root CID
// of a DAG in a blockstore, and serving the data as a CAR
type Libp2pCarServer struct {
	h      host.Host
	authds datastore.Batching
	bstore blockstore.Blockstore
	cfg    ServerConfig

	ctx         context.Context
	cancel      context.CancelFunc
	server      *http.Server
	netListener net.Listener

	eventListenersLk sync.Mutex
	eventListeners   map[*EventListenerFn]struct{}

	transfersLk sync.Mutex
	transfers   map[cid.Cid]*transferTimer
}

type ServerConfig struct {
	AnnounceAddr multiaddr.Multiaddr
	// If there is an error transferring data, wait for the data receiver to
	// retry the transfer for up to RetryTimeout before giving up
	RetryTimeout time.Duration
}

func NewLibp2pCarServer(h host.Host, ds datastore.Batching, bstore blockstore.Blockstore, cfg ServerConfig) *Libp2pCarServer {
	authds := namespace.Wrap(ds, datastore.NewKey("/libp2p-car-server-auth"))
	return &Libp2pCarServer{
		h:              h,
		authds:         authds,
		bstore:         bstore,
		cfg:            cfg,
		eventListeners: make(map[*EventListenerFn]struct{}),
		transfers:      make(map[cid.Cid]*transferTimer),
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
	s.server = &http.Server{Handler: handler}
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

// PrepareForDataRequest creates an auth token and saves some parameters
// in the datastore, in preparation for a request with that auth token.
func (s *Libp2pCarServer) PrepareForDataRequest(ctx context.Context, dbID uint, proposalCid cid.Cid, payloadCid cid.Cid, size uint64) (*DealTransfer, error) {
	// Create a new auth token and add it to the datastore
	authTokenBuff := make([]byte, 1024)
	if _, err := rand.Read(authTokenBuff); err != nil {
		return nil, fmt.Errorf("generating auth token: %w", err)
	}
	authToken := hex.EncodeToString(authTokenBuff)

	authValueJson, err := json.Marshal(&authValue{
		CreatedAt:   time.Now(),
		ProposalCid: proposalCid,
		PayloadCid:  payloadCid,
		Size:        size,
		DBID:        dbID,
	})
	if err != nil {
		return nil, fmt.Errorf("marshaling auth value JSON: %w", err)
	}

	authTokenKey := datastore.NewKey(authToken)
	err = s.authds.Put(ctx, authTokenKey, authValueJson)
	if err != nil {
		return nil, fmt.Errorf("adding auth token to datastore: %w", err)
	}

	dt := &DealTransfer{
		MultiAddress: s.cfg.AnnounceAddr,
		AuthToken:    authToken,
	}
	return dt, nil
}

// Cleanup removes an auth token from the datastore
func (s *Libp2pCarServer) Cleanup(authToken string) error {
	return s.authds.Delete(s.ctx, datastore.NewKey(authToken))
}

func (s *Libp2pCarServer) handler(w http.ResponseWriter, r *http.Request) {
	err := s.handleNewReq(w, r)
	if err != nil {
		log.Infof(err.Error())
		w.WriteHeader(err.code)
	}
}

func (s *Libp2pCarServer) handleNewReq(w http.ResponseWriter, r *http.Request) *httpError {
	// Get auth token from Authorization header
	_, authToken, ok := r.BasicAuth()
	if !ok {
		return &httpError{
			error: fmt.Errorf("rejected request with no Authorization header"),
			code:  401,
		}
	}

	// Get proposal CID from auth datastore
	authValueJson, err := s.authds.Get(s.ctx, datastore.NewKey(authToken))
	if xerrors.Is(err, datastore.ErrNotFound) {
		return &httpError{
			error: fmt.Errorf("rejected unrecognized auth token"),
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

	w.Header().Set("Content-Type", "application/car")
	content := car.NewCarReaderSeeker(s.ctx, val.PayloadCid, s.bstore, val.Size)
	if r.Method == "HEAD" {
		http.ServeContent(w, r, "", time.Time{}, content)
		return nil
	}

	s.sendCar(r, w, authToken, val, content)
	return nil
}

func (s *Libp2pCarServer) sendCar(r *http.Request, w http.ResponseWriter, authToken string, val authValue, content *car.CarReaderSeeker) {
	var transferred uint64
	local := s.h.ID().String()
	remote := r.RemoteAddr // TODO: Check that RemoteAddr is the remote Peer ID
	fireEvent := func(transferred uint64, status types.TransferStatus, msg string) {
		evt := transferEvent(local, remote, transferred, status, msg)
		s.eventListenersLk.Lock()
		for l := range s.eventListeners {
			(*l)(val.DBID, evt)
		}
		s.eventListenersLk.Unlock()
	}

	s.transfersLk.Lock()
	existing, ok := s.transfers[val.ProposalCid]
	if ok {
		log.Infow("restarting transfer", "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid)
		existing.cancelTimeout()
	} else {
		log.Infof("starting transfer", "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid)
	}
	xfer := &transferTimer{
		authValue:    &val,
		retryTimeout: s.cfg.RetryTimeout,
		onTimeout: func(orig error) {
			s.transfersLk.Lock()
			delete(s.transfers, val.ProposalCid)
			s.transfersLk.Unlock()

			log.Infof("failed transfer after timeout", "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid,
				"error", orig, "timeout", s.cfg.RetryTimeout)
			_ = s.Cleanup(authToken)

			msg := fmt.Sprintf("timed out waiting %s for data receiver to restart transfer ", s.cfg.RetryTimeout)
			msg += "after error " + orig.Error()
			msg += "\nfor proposal CID " + val.ProposalCid.String() + ", payloadCID " + val.PayloadCid.String()
			fireEvent(transferred, types.TransferStatusFailed, msg)
		},
	}
	s.transfers[val.ProposalCid] = xfer
	s.transfersLk.Unlock()

	// Fire event before starting transfer
	fireEvent(0, types.TransferStatusStarted, "")

	// Fire progress events during transfer.
	// We fire a progress event each time bytes are read from the CAR.
	// Note that we can't fire events when bytes are written to the HTTP stream
	// because there may be some transformation first (eg adding headers etc).
	readEmitter := &readEmitter{ReadSeeker: content, emit: func(count int, err error) {
		if err != nil {
			xfer.startRetryTimer(err)
			return
		}
		transferred += uint64(count)
		fireEvent(transferred, types.TransferStatusOngoing, "")
	}}

	// http.ServeContent ignores errors when writing to the stream, so we
	// replace the writer with a class that watches for errors
	writeErrWatcher := &writeErrorWatcher{ResponseWriter: w, onError: xfer.startRetryTimer}

	http.ServeContent(writeErrWatcher, r, "", time.Time{}, readEmitter)

	// Check if there was an error during the transfer
	if xfer.errored() {
		// There was an error, just wait for the data receiver to retry
		return
	}

	s.transfersLk.Lock()
	delete(s.transfers, val.ProposalCid)
	s.transfersLk.Unlock()

	// Fire event at the end of the transfer
	fireEvent(transferred, types.TransferStatusCompleted, "")

	log.Infow("completed serving request", "proposalCID", val.ProposalCid, "payloadCID", val.PayloadCid)
}

type EventListenerFn func(dbid uint, st types.TransferState)
type UnsubFn func()

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

type transferTimer struct {
	*authValue
	retryTimeout time.Duration
	onTimeout    func(orig error)
	retryTimer   *time.Timer
	lk           sync.Mutex
	err          error
}

func (t *transferTimer) startRetryTimer(err error) {
	t.lk.Lock()
	defer t.lk.Unlock()

	if t.err != nil {
		return
	}
	t.err = err

	msg := "error in transfer for proposal CID %s / payload CID %s: %s"
	msg += "\nwaiting %s for data receiver to retry"
	log.Infof(msg, t.ProposalCid, t.PayloadCid, err, t.retryTimeout)
	t.retryTimer = time.AfterFunc(t.retryTimeout, func() {
		t.onTimeout(err)
	})
}

func (t *transferTimer) cancelTimeout() {
	if t.retryTimer != nil {
		t.retryTimer.Stop()
	}
}

func (t *transferTimer) errored() bool {
	t.lk.Lock()
	defer t.lk.Unlock()

	return t.err != nil
}

type readEmitter struct {
	io.ReadSeeker
	emit func(count int, err error)
}

func (e *readEmitter) Read(p []byte) (n int, err error) {
	count, err := e.ReadSeeker.Read(p)
	e.emit(count, err)
	return count, err
}

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

func transferEvent(localAddr string, remoteAddr string, transferred uint64, status types.TransferStatus, msg string) types.TransferState {
	if msg == "" {
		msg = fmt.Sprintf("transferred %d bytes", transferred)
		if transferred == 0 {
			msg = "pull data transfer queued"
		}
	}
	return types.TransferState{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
		Status:     status,
		Sent:       transferred,
		Message:    msg,
	}
}

type DealTransfer struct {
	MultiAddress multiaddr.Multiaddr
	AuthToken    string
}

type authValue struct {
	CreatedAt   time.Time
	ProposalCid cid.Cid
	PayloadCid  cid.Cid
	Size        uint64
	DBID        uint
}
