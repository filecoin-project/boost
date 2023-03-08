package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/encoding"
	"github.com/filecoin-project/go-data-transfer/message"
	"github.com/filecoin-project/go-data-transfer/network"
	"github.com/filecoin-project/go-data-transfer/registry"
	"github.com/filecoin-project/go-data-transfer/transport/graphsync/extension"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	retrievalimpl "github.com/filecoin-project/go-fil-markets/retrievalmarket/impl"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/migrations"
	"github.com/filecoin-project/go-fil-markets/stores"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/hannahhoward/go-pubsub"
	"github.com/ipfs/go-graphsync"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"
)

var log = logging.Logger("boostgs")

var incomingReqExtensions = []graphsync.ExtensionName{
	extension.ExtensionIncomingRequest1_1,
	extension.ExtensionDataTransfer1_1,
}

// Uniquely identify a request (requesting peer + data transfer id)
type reqId struct {
	p  peer.ID
	id datatransfer.TransferID
}

// GraphsyncUnpaidRetrieval intercepts incoming requests to Graphsync.
// If the request is for a paid retrieval, it is forwarded to the existing
// Graphsync implementation.
// If the request is a simple unpaid retrieval, it is handled by this class.
type GraphsyncUnpaidRetrieval struct {
	graphsync.GraphExchange
	peerID     peer.ID
	dtnet      network.DataTransferNetwork
	decoder    *registry.Registry
	validator  *requestValidator
	pubSubDT   *pubsub.PubSub
	pubSubMkts *pubsub.PubSub

	activeRetrievalsLk sync.RWMutex
	activeRetrievals   map[reqId]*retrievalState

	ctx context.Context

	// Used by the tests
	outgoingBlockHook func(*retrievalState)
}

var _ graphsync.GraphExchange = (*GraphsyncUnpaidRetrieval)(nil)

var defaultExtensions = []graphsync.ExtensionName{
	extension.ExtensionDataTransfer1_1,
}

type AskGetter interface {
	GetAsk() *retrievalmarket.Ask
}

type ValidationDeps struct {
	DealDecider    retrievalimpl.DealDecider
	DagStore       stores.DAGStoreWrapper
	PieceStore     piecestore.PieceStore
	SectorAccessor retrievalmarket.SectorAccessor
	AskStore       AskGetter
}

func NewGraphsyncUnpaidRetrieval(peerID peer.ID, gs graphsync.GraphExchange, dtnet network.DataTransferNetwork, vdeps ValidationDeps) (*GraphsyncUnpaidRetrieval, error) {
	typeRegistry := registry.NewRegistry()
	err := typeRegistry.Register(&retrievalmarket.DealProposal{}, nil)
	if err != nil {
		return nil, err
	}
	err = typeRegistry.Register(&migrations.DealProposal0{}, nil)
	if err != nil {
		return nil, err
	}

	return &GraphsyncUnpaidRetrieval{
		GraphExchange:    gs,
		peerID:           peerID,
		dtnet:            dtnet,
		decoder:          typeRegistry,
		pubSubDT:         pubsub.New(eventDispatcherDT),
		pubSubMkts:       pubsub.New(eventDispatcherMkts),
		validator:        newRequestValidator(vdeps),
		activeRetrievals: make(map[reqId]*retrievalState),
	}, nil
}

func (g *GraphsyncUnpaidRetrieval) Start(ctx context.Context) {
	g.ctx = ctx
	g.validator.ctx = ctx
}

// Called when a new request is received
func (g *GraphsyncUnpaidRetrieval) trackTransfer(p peer.ID, id datatransfer.TransferID, state *retrievalState) {
	// Record the transfer as an active retrieval so we can distinguish between
	// retrievals intercepted by this class, and those passed through to the
	// paid retrieval implementation.
	g.activeRetrievalsLk.Lock()
	g.activeRetrievals[reqId{p: p, id: id}] = state
	g.activeRetrievalsLk.Unlock()

	// Protect the connection so that it doesn't get reaped by the
	// connection manager before the transfer has completed
	g.dtnet.Protect(p, fmt.Sprintf("%d", id))
}

// Called when a request completes (either successfully or in failure)
// TODO: Make sure that untrackTransfer is always called eventually
// (may need to add a timeout)
func (g *GraphsyncUnpaidRetrieval) untrackTransfer(p peer.ID, id datatransfer.TransferID) {
	g.activeRetrievalsLk.Lock()
	delete(g.activeRetrievals, reqId{p: p, id: id})
	g.activeRetrievalsLk.Unlock()

	g.dtnet.Unprotect(p, fmt.Sprintf("%d", id))
}

func (g *GraphsyncUnpaidRetrieval) CancelTransfer(ctx context.Context, id datatransfer.TransferID, p *peer.ID) error {
	didCancel := false

	// If peer is set we can cancel more efficiently
	if p != nil {
		state, ok := g.activeRetrievals[reqId{p: *p, id: id}]
		if ok {
			g.failTransfer(state, errors.New("transfer cancelled by provider"))
		}
		return nil
	}

	// Peer was not given so we need to iterate over active retrievals
	for _, state := range g.activeRetrievals {
		if state.cs.transferID == id {
			didCancel = true
			g.failTransfer(state, errors.New("transfer cancelled by provider"))
			// Dont break, transferID might not be unique, so we have to keep iterating
		}
	}

	if !didCancel {
		return fmt.Errorf("no transfer with id %d", id)
	}

	return nil
}

func (g *GraphsyncUnpaidRetrieval) List() []retrievalState {
	values := make([]retrievalState, 0, len(g.activeRetrievals))

	for _, value := range g.activeRetrievals {
		values = append(values, *value)
	}

	return values
}

func (g *GraphsyncUnpaidRetrieval) RegisterIncomingRequestQueuedHook(hook graphsync.OnIncomingRequestQueuedHook) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterIncomingRequestQueuedHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.RequestQueuedHookActions) {
		interceptRtvl, err := g.interceptRetrieval(p, request)
		if err != nil {
			log.Errorw("incoming request failed", "request", request, "error", err)
			return
		}

		if !interceptRtvl {
			hook(p, request, hookActions)
			return
		}
	})
}

func (g *GraphsyncUnpaidRetrieval) interceptRetrieval(p peer.ID, request graphsync.RequestData) (bool, error) {
	// Extract the request message from the extension data
	msg, err := extension.GetTransferData(request, defaultExtensions)
	if err != nil {
		log.Errorw("failed to extract message from request", "request", request, "err", err)
		return false, nil
	}
	// Extension not found, ignore
	if msg == nil {
		return false, nil
	}

	// When a data transfer request comes in on graphsync, the remote peer
	// initiated a pull request for data. If it's not a request, ignore it.
	if !msg.IsRequest() {
		return false, nil
	}

	dtRequest := msg.(datatransfer.Request)
	if !dtRequest.IsNew() && !dtRequest.IsRestart() {
		// The request is not for a new retrieval (it's a cancel etc).
		// If this message is for an existing unpaid retrieval it will already
		// be in our map (because we must have already processed the new
		// retrieval request)
		_, ok := g.isActiveUnpaidRetrieval(reqId{p: p, id: msg.TransferID()})
		return ok, nil
	}

	// The request is for a new transfer / restart transfer, so check if it's
	// for an unpaid retrieval
	voucher, decodeErr := g.decodeVoucher(dtRequest, g.decoder)
	if decodeErr != nil {
		return false, fmt.Errorf("decoding new request voucher: %w", decodeErr)
	}

	proposal, ok := voucher.(*retrievalmarket.DealProposal)
	if !ok {
		legacyProposal, ok := voucher.(*migrations.DealProposal0)
		if !ok {
			return false, errors.New("wrong voucher type")
		}
		newProposal := migrations.MigrateDealProposal0To1(*legacyProposal)
		proposal = &newProposal
	}

	// If it's a paid retrieval, do not intercept it
	if !proposal.UnsealPrice.IsZero() || !proposal.PricePerByte.IsZero() {
		return false, nil
	}

	// It's for an unpaid retrieval. Initialize the channel state.
	selBytes, err := encoding.Encode(request.Selector())
	if err != nil {
		return true, fmt.Errorf("encoding selector: %w", err)
	}
	cs := &channelState{
		selfPeer:   g.peerID,
		transferID: msg.TransferID(),
		baseCid:    request.Root(),
		selector:   &cbg.Deferred{Raw: selBytes},
		sender:     g.peerID,
		recipient:  p,
		status:     datatransfer.Requested,
		isPull:     true,
	}

	mktsState := &retrievalmarket.ProviderDealState{
		DealProposal:  *proposal,
		ChannelID:     &datatransfer.ChannelID{ID: msg.TransferID(), Initiator: p, Responder: g.peerID},
		Status:        retrievalmarket.DealStatusNew,
		Receiver:      p,
		FundsReceived: abi.NewTokenAmount(0),
	}
	state := &retrievalState{
		cs:   cs,
		mkts: mktsState,
	}

	// Record the data transfer ID so that we can intercept future
	// events for this transfer
	g.trackTransfer(p, msg.TransferID(), state)

	// Fire transfer queued event
	g.publishDTEvent(datatransfer.TransferRequestQueued, "", cs)

	// This is an unpaid retrieval, so this class is responsible for
	// handling it
	return true, nil
}

func (g *GraphsyncUnpaidRetrieval) RegisterIncomingRequestHook(hook graphsync.OnIncomingRequestHook) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterIncomingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		msg, state, intercept := g.isRequestForActiveUnpaidRetrieval(p, request)
		if !intercept {
			hook(p, request, hookActions)
			return
		}

		g.publishDTEvent(datatransfer.Open, "unpaid", state.cs)
		g.publishMktsEvent(retrievalmarket.ProviderEventOpen, *state.mkts)

		err := func() error {
			voucher, decodeErr := g.decodeVoucher(msg, g.decoder)
			if decodeErr != nil {
				return fmt.Errorf("decoding new request voucher: %w", decodeErr)
			}

			// Validate the request
			res, validateErr := g.validator.validatePullRequest(msg.IsRestart(), p, voucher, request.Root(), request.Selector())
			isAccepted := validateErr == nil
			const isPaused = false // There are no payments required, so never pause
			resultType := datatransfer.EmptyTypeIdentifier
			if res != nil {
				resultType = res.Type()
			}
			respMsg, msgErr := message.NewResponse(msg.TransferID(), isAccepted, isPaused, resultType, res)
			if msgErr != nil {
				return fmt.Errorf("creating accept response message: %w", msgErr)
			}

			// Send an accept message / validation failed message as an extension
			// (a go-data-transfer protocol message that gets embedded in a
			// graphsync message)
			if respMsg != nil {
				// This hook uses a unique extension name so it can be attached
				// to a graphsync message with data from a different hook.
				// incomingReqExtensions also includes the default extension
				// name to maintain compatibility with previous data-transfer
				// protocol versions.
				extensions, extensionErr := extension.ToExtensionData(respMsg, incomingReqExtensions)
				if extensionErr != nil {
					return fmt.Errorf("building extension data: %w", extensionErr)
				}
				for _, ext := range extensions {
					hookActions.SendExtensionData(ext)
				}
			}

			return validateErr
		}()

		if err != nil {
			hookActions.TerminateWithError(err)
			g.failTransfer(state, err)
			return
		}

		// Mark the request as valid
		hookActions.ValidateRequest()

		// Fire events
		state.cs.status = datatransfer.Ongoing
		g.publishDTEvent(datatransfer.Accept, "", state.cs)
		state.mkts.Status = retrievalmarket.DealStatusUnsealing
		g.publishMktsEvent(retrievalmarket.ProviderEventDealAccepted, *state.mkts)
		state.mkts.Status = retrievalmarket.DealStatusUnsealed
		g.publishMktsEvent(retrievalmarket.ProviderEventUnsealComplete, *state.mkts)
	})
}

func (g *GraphsyncUnpaidRetrieval) RegisterOutgoingBlockHook(hook graphsync.OnOutgoingBlockHook) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterOutgoingBlockHook(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
		_, state, intercept := g.isRequestForActiveUnpaidRetrieval(p, request)
		if !intercept {
			hook(p, request, block, hookActions)
			return
		}

		if g.outgoingBlockHook != nil {
			g.outgoingBlockHook(state)
		}
	})
}

func (g *GraphsyncUnpaidRetrieval) RegisterCompletedResponseListener(listener graphsync.OnResponseCompletedListener) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterCompletedResponseListener(func(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
		msg, state, intercept := g.isRequestForActiveUnpaidRetrieval(p, request)
		if !intercept {
			listener(p, request, status)
			return
		}

		// Check that it's an incoming response from the other peer
		// (not an outgoing response)
		if p == g.peerID {
			return
		}

		defer g.untrackTransfer(p, msg.TransferID())

		// Request was cancelled, nothing further to do
		if status == graphsync.RequestCancelled {
			return
		}

		if status != graphsync.RequestCompletedFull {
			completeErr := fmt.Errorf("graphsync response to peer %s did not complete: response status code %s", p, status)
			g.failTransfer(state, completeErr)
			return
		}

		// Fire markets blocks completed event
		state.mkts.Status = retrievalmarket.DealStatusBlocksComplete
		g.publishMktsEvent(retrievalmarket.ProviderEventBlocksCompleted, *state.mkts)

		// Include a markets protocol Completed message in the response
		voucher := &retrievalmarket.DealResponse{
			ID:     state.mkts.DealProposal.ID,
			Status: retrievalmarket.DealStatusCompleted,
		}

		const isAccepted = true
		const isPaused = false
		respMsg, err := message.CompleteResponse(msg.TransferID(), isAccepted, isPaused, voucher.Type(), voucher)
		if err != nil {
			g.failTransfer(state, fmt.Errorf("getting complete response: %w", err))
			return
		}

		// Send the other peer a message that the transfer has completed
		if err := g.dtnet.SendMessage(g.ctx, p, respMsg); err != nil {
			err := fmt.Errorf("failed to send completion message to requestor %s: %w", p, err)
			g.failTransfer(state, err)
			return
		}

		state.cs.status = datatransfer.Completed
		g.publishDTEvent(datatransfer.Complete, "", state.cs)
		// Fire markets blocks completed event
		state.mkts.Status = retrievalmarket.DealStatusCompleted
		g.publishMktsEvent(retrievalmarket.ProviderEventComplete, *state.mkts)

		log.Infow("successfully sent completion message to requestor", "peer", p)
	})
}

func (g *GraphsyncUnpaidRetrieval) RegisterRequestorCancelledListener(listener graphsync.OnRequestorCancelledListener) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterRequestorCancelledListener(func(p peer.ID, request graphsync.RequestData) {
		_, state, intercept := g.isRequestForActiveUnpaidRetrieval(p, request)
		if !intercept {
			listener(p, request)
			return
		}

		state.cs.status = datatransfer.Cancelled
		g.publishDTEvent(datatransfer.Cancel, "client cancelled", state.cs)
		state.mkts.Status = retrievalmarket.DealStatusCancelled
		g.publishMktsEvent(retrievalmarket.ProviderEventCancelComplete, *state.mkts)

		g.untrackTransfer(p, state.cs.transferID)
	})
}

func (g *GraphsyncUnpaidRetrieval) RegisterBlockSentListener(listener graphsync.OnBlockSentListener) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterBlockSentListener(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData) {
		_, state, intercept := g.isRequestForActiveUnpaidRetrieval(p, request)
		if !intercept {
			listener(p, request, block)
			return
		}

		// When a data transfer is restarted, the requester sends a list of CIDs
		// that it already has. Graphsync calls the sent hook for all blocks even
		// if they are in the list (meaning, they aren't actually sent over the
		// wire). So here we check if the block was actually sent
		// over the wire before firing the data sent event.
		if block.BlockSizeOnWire() == 0 {
			return
		}

		// Fire block sent event
		state.cs.sent += block.BlockSizeOnWire()
		g.publishDTEvent(datatransfer.DataSent, "", state.cs)
		state.mkts.TotalSent += block.BlockSizeOnWire()
	})
}

func (g *GraphsyncUnpaidRetrieval) RegisterNetworkErrorListener(listener graphsync.OnNetworkErrorListener) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterNetworkErrorListener(func(p peer.ID, request graphsync.RequestData, err error) {
		_, state, intercept := g.isRequestForActiveUnpaidRetrieval(p, request)
		if !intercept {
			listener(p, request, err)
			return
		}

		// Consider network errors as fatal, clients can sent a new request if they wish

		// Cancel the graphsync retrieval
		cancelErr := g.GraphExchange.Cancel(g.ctx, request.ID())
		if cancelErr != nil {
			log.Errorf("cancelling graphsync response after network error: %w", cancelErr)
		}

		// Fail the transfer
		g.failTransfer(state, err)
	})
}

func (g *GraphsyncUnpaidRetrieval) failTransfer(state *retrievalState, err error) {
	state.cs.status = datatransfer.Failed
	state.cs.message = err.Error()
	g.publishDTEvent(datatransfer.Error, err.Error(), state.cs)
	state.mkts.Status = retrievalmarket.DealStatusErrored
	g.publishMktsEvent(retrievalmarket.ProviderEventDataTransferError, *state.mkts)

	g.untrackTransfer(state.cs.recipient, state.cs.transferID)
	log.Infow("transfer failed", "transfer id", state.cs.transferID, "peer", state.cs.recipient, "err", err)
}

func (g *GraphsyncUnpaidRetrieval) decodeVoucher(request datatransfer.Request, registry *registry.Registry) (datatransfer.Voucher, error) {
	vtypStr := request.VoucherType()
	decoder, has := registry.Decoder(vtypStr)
	if !has {
		return nil, fmt.Errorf("unknown voucher type: %s", vtypStr)
	}
	encodable, err := request.Voucher(decoder)
	if err != nil {
		return nil, err
	}
	return encodable.(datatransfer.Registerable), nil
}

func (g *GraphsyncUnpaidRetrieval) isRequestForActiveUnpaidRetrieval(p peer.ID, request graphsync.RequestData) (datatransfer.Request, *retrievalState, bool) {
	// Extract the data transfer message from the Graphsync request
	msg, err := extension.GetTransferData(request, defaultExtensions)
	if err != nil {
		log.Errorw("failed to extract message from request", "request", request, "err", err)
		return nil, nil, false
	}
	// Extension not found, ignore
	if msg == nil {
		return nil, nil, false
	}

	// Check it's a request (not a response)
	if !msg.IsRequest() {
		return nil, nil, false
	}

	dtRequest := msg.(datatransfer.Request)
	state, ok := g.isActiveUnpaidRetrieval(reqId{p: p, id: msg.TransferID()})
	return dtRequest, state, ok
}

func (g *GraphsyncUnpaidRetrieval) isActiveUnpaidRetrieval(id reqId) (*retrievalState, bool) {
	g.activeRetrievalsLk.RLock()
	defer g.activeRetrievalsLk.RUnlock()

	state, ok := g.activeRetrievals[id]
	return state, ok
}

func (g *GraphsyncUnpaidRetrieval) SubscribeToValidationEvents(sub retrievalmarket.ProviderValidationSubscriber) retrievalmarket.Unsubscribe {
	return g.validator.Subscribe(sub)
}
