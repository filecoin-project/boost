package retrieve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/filecoin-project/boost/transport/httptransport"
	boosttypes "github.com/filecoin-project/boost/transport/types"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/xerrors"
)

// libp2pCarServer is mocked out by the tests
type libp2pCarServer interface {
	ID() peer.ID
	Start(ctx context.Context) error
	Get(id string) (*httptransport.Libp2pTransfer, error)
	Subscribe(f httptransport.EventListenerFn) httptransport.UnsubFn
	CancelTransfer(ctx context.Context, id string) (*boosttypes.TransferState, error)
	Matching(f httptransport.MatchFn) ([]*httptransport.Libp2pTransfer, error)
}

// libp2pTransferManager watches events for a libp2p data transfer
type libp2pTransferManager struct {
	dtServer  libp2pCarServer
	authDB    *httptransport.AuthTokenDB
	dtds      datastore.Batching
	opts      libp2pTransferManagerOpts
	ctx       context.Context
	cancelCtx context.CancelFunc
	ticker    *time.Ticker

	listenerLk sync.Mutex
	listener   func(uint, ChannelState)
	unsub      func()
}

type libp2pTransferManagerOpts struct {
	xferTimeout     time.Duration
	authCheckPeriod time.Duration
}

func newLibp2pTransferManager(dtServer libp2pCarServer, ds datastore.Batching, authDB *httptransport.AuthTokenDB, opts libp2pTransferManagerOpts) *libp2pTransferManager {
	ds = namespace.Wrap(ds, datastore.NewKey("/data-transfers"))
	return &libp2pTransferManager{
		dtServer: dtServer,
		dtds:     ds,
		authDB:   authDB,
		opts:     opts,
	}
}

func (m *libp2pTransferManager) Stop() {
	m.ticker.Stop()
	m.cancelCtx()
	m.unsub()
}

func (m *libp2pTransferManager) Start(ctx context.Context) error {
	m.ctx, m.cancelCtx = context.WithCancel(ctx)

	// subscribe to notifications from the data transfer server
	m.unsub = m.subscribe()

	// Every tick, check transfers to see if any have expired
	m.ticker = time.NewTicker(m.opts.authCheckPeriod)
	go func() {
		for range m.ticker.C {
			m.checkTransferExpiry(ctx)
		}
	}()

	return m.dtServer.Start(ctx)
}

func (m *libp2pTransferManager) checkTransferExpiry(ctx context.Context) {
	// Delete expired auth tokens from the auth DB
	expired, err := m.authDB.DeleteExpired(ctx, time.Now().Add(-m.opts.xferTimeout))
	if err != nil {
		log.Errorw("deleting expired tokens from auth DB", "err", err)
		return
	}

	// For each expired auth token
	for _, val := range expired {
		// Get the active transfer associated with the auth token
		activeXfer, err := m.dtServer.Get(val.ID)
		if err != nil && !errors.Is(err, httptransport.ErrTransferNotFound) {
			log.Errorw("getting transfer", "id", val.ID, "err", err)
			continue
		}

		// Note that there may not be an active transfer (it may have not
		// started, errored out or completed)
		if activeXfer != nil {
			// Cancel the transfer
			_, err := m.dtServer.CancelTransfer(ctx, val.ID)
			if err != nil && !errors.Is(err, httptransport.ErrTransferNotFound) {
				log.Errorw("canceling transfer", "id", val.ID, "err", err)
				continue
			}
		}

		// Check if the transfer already completed
		completedXfer, err := m.getCompletedTransfer(val.ID)
		if err != nil && !errors.Is(err, datastore.ErrNotFound) {
			log.Errorw("getting completed transfer", "id", val.ID, "err", err)
			continue
		}
		// If the transfer had already completed, nothing more to do
		if completedXfer != nil && completedXfer.Status == boosttypes.TransferStatusCompleted {
			continue
		}

		// The transfer didn't start, or was canceled or errored out before
		// completing, so fire a transfer error event
		dbid, err := strconv.ParseUint(val.ID, 10, 64)
		if err != nil {
			log.Errorw("parsing dbid in libp2p transfer manager event", "id", val.ID, "err", err)
			continue
		}

		st := boosttypes.TransferState{
			ID:        val.ID,
			LocalAddr: m.dtServer.ID().String(),
		}

		if activeXfer != nil {
			st = activeXfer.State()
		}
		st.Status = boosttypes.TransferStatusFailed
		if st.Message == "" {
			st.Message = fmt.Sprintf("timed out waiting %s for transfer to complete", m.opts.xferTimeout)
		}

		if completedXfer == nil {
			// Save the transfer status in persistent storage
			err = m.saveCompletedTransfer(val.ID, st)
			if err != nil {
				log.Errorf("saving completed transfer: %s", err)
				continue
			}
		}

		// Fire transfer error event
		m.listenerLk.Lock()
		if m.listener != nil {
			m.listener(uint(dbid), m.toDTState(st))
		}
		m.listenerLk.Unlock()
	}
}

// PrepareForDataRequest prepares to receive a data transfer request with the
// given auth token
func (m *libp2pTransferManager) PrepareForDataRequest(ctx context.Context, id uint, authToken string, proposalCid cid.Cid, payloadCid cid.Cid, size uint64) error {
	err := m.authDB.Put(ctx, authToken, httptransport.AuthValue{
		ID:          fmt.Sprintf("%d", id),
		ProposalCid: proposalCid,
		PayloadCid:  payloadCid,
		Size:        size,
	})
	if err != nil {
		return fmt.Errorf("adding new auth token: %w", err)
	}
	return nil
}

// CleanupPreparedRequest is called to remove the auth token for a request
// when the request is no longer expected (eg because the provider rejected
// the deal proposal)
func (m *libp2pTransferManager) CleanupPreparedRequest(ctx context.Context, dbid uint, authToken string) error {
	// Delete the auth token for the request
	delerr := m.authDB.Delete(ctx, authToken)

	// Cancel any related transfer
	dbidstr := fmt.Sprintf("%d", dbid)
	_, cancelerr := m.dtServer.CancelTransfer(ctx, dbidstr)
	if cancelerr != nil && xerrors.Is(cancelerr, httptransport.ErrTransferNotFound) {
		// Ignore transfer not found error
		cancelerr = nil
	}

	if delerr != nil {
		return delerr
	}
	return cancelerr
}

// Subscribe to state change events from the libp2p server
func (m *libp2pTransferManager) Subscribe(listener func(dbid uint, st ChannelState)) (func(), error) {
	m.listenerLk.Lock()
	defer m.listenerLk.Unlock()

	if m.listener != nil {
		// Only need one for the current use case, we can add more if needed later
		return nil, fmt.Errorf("only one listener allowed")
	}

	m.listener = listener

	unsub := func() {
		m.listenerLk.Lock()
		defer m.listenerLk.Unlock()

		m.listener = nil
	}
	return unsub, nil
}

// Convert from libp2p server events to data transfer events
func (m *libp2pTransferManager) subscribe() (unsub func()) {
	return m.dtServer.Subscribe(func(dbidstr string, st boosttypes.TransferState) {
		dbid, err := strconv.ParseUint(dbidstr, 10, 64)
		if err != nil {
			log.Errorf("cannot parse dbid '%s' in libp2p transfer manager event: %s", dbidstr, err)
			return
		}
		if st.Status == boosttypes.TransferStatusFailed {
			// If the transfer fails, don't fire the failure event yet.
			// Wait until the transfer timeout (so that the data receiver has
			// a chance to make another request)
			log.Infow("libp2p data transfer error", "dbid", dbid, "remote", st.RemoteAddr, "message", st.Message)
			return
		}
		if st.Status == boosttypes.TransferStatusCompleted {
			if err := m.saveCompletedTransfer(dbidstr, st); err != nil {
				log.Errorf("saving completed transfer: %s", err)
			}
		}

		m.listenerLk.Lock()
		defer m.listenerLk.Unlock()
		if m.listener != nil {
			m.listener(uint(dbid), m.toDTState(st))
		}
	})
}

// All returns all active and completed transfers
func (m *libp2pTransferManager) All() (map[string]ChannelState, error) {
	// Get all active transfers
	all := make(map[string]ChannelState)
	_, err := m.dtServer.Matching(func(xfer *httptransport.Libp2pTransfer) (bool, error) {
		all[xfer.ID] = m.toDTState(xfer.State())
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	// Get all persisted (completed) transfers
	err = m.forEachCompletedTransfer(func(id string, st boosttypes.TransferState) bool {
		all[id] = m.toDTState(st)
		return true
	})
	return all, err
}

func (m *libp2pTransferManager) byId(id string) (*ChannelState, error) {
	// Check active transfers
	xfer, err := m.dtServer.Get(id)
	if err == nil {
		st := m.toDTState(xfer.State())
		return &st, nil
	}
	if !xerrors.Is(err, httptransport.ErrTransferNotFound) {
		return nil, err
	}

	// Check completed transfers
	boostSt, err := m.getCompletedTransfer(id)
	if err != nil {
		return nil, err
	}
	st := m.toDTState(*boostSt)
	return &st, nil
}

// byRemoteAddrAndPayloadCid returns the transfer with the matching remote
// address and payload cid
func (m *libp2pTransferManager) byRemoteAddrAndPayloadCid(remoteAddr string, payloadCid cid.Cid) (*ChannelState, error) {
	// Check active transfers
	matches, err := m.dtServer.Matching(func(xfer *httptransport.Libp2pTransfer) (bool, error) {
		match := xfer.RemoteAddr == remoteAddr && xfer.PayloadCid == payloadCid
		return match, nil
	})
	if err != nil {
		return nil, err
	}

	if len(matches) > 0 {
		st := m.toDTState(matches[0].State())
		return &st, nil
	}

	// Check completed transfers
	var chanst *ChannelState
	err = m.forEachCompletedTransfer(func(id string, st boosttypes.TransferState) bool {
		if st.RemoteAddr == remoteAddr && st.PayloadCid == payloadCid {
			dtst := m.toDTState(st)
			chanst = &dtst
			return true
		}
		return false
	})
	return chanst, err
}

func (m *libp2pTransferManager) toDTState(st boosttypes.TransferState) ChannelState {
	status := datatransfer.Ongoing
	switch st.Status {
	case boosttypes.TransferStatusStarted:
		status = datatransfer.Requested
	case boosttypes.TransferStatusCompleted:
		status = datatransfer.Completed
	case boosttypes.TransferStatusFailed:
		status = datatransfer.Failed
	}

	// The datatransfer channel ID is no longer used, but fill in
	// its fields anyway so that the parts of the UI that depend on it
	// still work
	xferid, _ := strconv.ParseUint(st.ID, 10, 64) //nolint:errcheck
	remoteAddr := st.RemoteAddr
	if remoteAddr == "" {
		// If the remote peer never tried to connect to us we don't have
		// a remote peer ID. However datatransfer.ChannelID requires a
		// remote peer ID. So just fill it in with the local peer ID so
		// that it doesn't fail JSON parsing when sent in an RPC.
		remoteAddr = st.LocalAddr
	}
	chid := datatransfer.ChannelID{
		Initiator: parsePeerID(st.LocalAddr),
		Responder: parsePeerID(remoteAddr),
		ID:        datatransfer.TransferID(xferid),
	}

	return ChannelState{
		SelfPeer:     parsePeerID(st.LocalAddr),
		RemotePeer:   parsePeerID(remoteAddr),
		Status:       status,
		StatusStr:    datatransfer.Statuses[status],
		Sent:         st.Sent,
		Received:     st.Received,
		Message:      st.Message,
		BaseCid:      st.PayloadCid.String(),
		ChannelID:    chid,
		TransferID:   st.ID,
		TransferType: BoostTransfer,
	}
}

func (m *libp2pTransferManager) getCompletedTransfer(id string) (*boosttypes.TransferState, error) {
	data, err := m.dtds.Get(m.ctx, datastore.NewKey(id))
	if err != nil {
		if xerrors.Is(err, datastore.ErrNotFound) {
			return nil, fmt.Errorf("getting transfer status for id '%s': %w", id, err)
		}
		return nil, fmt.Errorf("getting transfer status for id '%s' from datastore: %w", id, err)
	}

	var st boosttypes.TransferState
	err = json.Unmarshal(data, &st)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling transfer status: %w", err)
	}

	return &st, nil
}

func (m *libp2pTransferManager) saveCompletedTransfer(id string, st boosttypes.TransferState) error {
	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("json marshalling transfer status: %w", err)
	}

	err = m.dtds.Put(m.ctx, datastore.NewKey(id), data)
	if err != nil {
		return fmt.Errorf("storing transfer status to datastore: %w", err)
	}

	return nil
}

func (m *libp2pTransferManager) forEachCompletedTransfer(cb func(string, boosttypes.TransferState) bool) error {
	qres, err := m.dtds.Query(m.ctx, query.Query{})
	if err != nil {
		return xerrors.Errorf("query error: %w", err)
	}
	defer qres.Close() //nolint:errcheck

	for r := range qres.Next() {
		var st boosttypes.TransferState
		err = json.Unmarshal(r.Value, &st)
		if err != nil {
			return fmt.Errorf("unmarshaling json from datastore: %w", err)
		}

		ok := cb(removeLeadingSlash(r.Key), st)
		if !ok {
			return nil
		}
	}

	return nil
}

func removeLeadingSlash(key string) string {
	if key[0] == '/' {
		return key[1:]
	}
	return key
}

func parsePeerID(pidstr string) peer.ID {
	pid, err := peer.Decode(pidstr)
	if err != nil {
		log.Warnf("couldnt decode pid '%s': %s", pidstr, err)
		return ""
	}
	return pid
}
