package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	graphsync "github.com/filecoin-project/boost-graphsync"
	"github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/encoding"
	"github.com/filecoin-project/boost/datatransfer/message"
	"github.com/filecoin-project/boost/datatransfer/network"
	"github.com/filecoin-project/boost/datatransfer/registry"
	"github.com/filecoin-project/boost/datatransfer/transport/graphsync/extension"
	"github.com/filecoin-project/boost/metrics"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes/migrations"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/hannahhoward/go-pubsub"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.opencensus.io/stats"
)

var (
	log                  = logging.Logger("boostgs")
	ErrRetrievalNotFound = fmt.Errorf("no transfer found")
)

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

type RetrievalAskGetter interface {
	GetAsk() *legacyretrievaltypes.Ask
}

type retrievalAskGetter struct {
	ask legacyretrievaltypes.Ask
}

func (rag *retrievalAskGetter) GetAsk() *legacyretrievaltypes.Ask {
	return &rag.ask
}

func NewRetrievalAskGetter() RetrievalAskGetter {
	return &retrievalAskGetter{
		ask: legacyretrievaltypes.Ask{
			PricePerByte: abi.NewTokenAmount(0),
			UnsealPrice:  abi.NewTokenAmount(0),
		},
	}
}

type ValidationDeps struct {
	DealDecider    DealDecider
	PieceDirectory *piecedirectory.PieceDirectory
	SectorAccessor SectorAccessor
	AskStore       RetrievalAskGetter
}

func NewGraphsyncUnpaidRetrieval(peerID peer.ID, gs graphsync.GraphExchange, dtnet network.DataTransferNetwork, vdeps ValidationDeps) (*GraphsyncUnpaidRetrieval, error) {
	typeRegistry := registry.NewRegistry()
	err := typeRegistry.Register(&legacyretrievaltypes.DealProposal{}, nil)
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

func (g *GraphsyncUnpaidRetrieval) Start(ctx context.Context) error {
	g.ctx = ctx
	g.validator.ctx = ctx

	return nil
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
	g.activeRetrievalsLk.Lock()

	var state *retrievalState
	if p != nil {
		state = g.activeRetrievals[reqId{p: *p, id: id}]
	}

	if state == nil {
		for _, st := range g.activeRetrievals {
			if st.cs.transferID == id {
				state = st
				break
			}
		}
	}

	if state == nil {
		g.activeRetrievalsLk.Unlock()
		return fmt.Errorf("failed to cancel with id %d: %w", id, ErrRetrievalNotFound)
	}

	rcpt := state.cs.recipient
	tID := state.cs.transferID
	gsRequestID := state.gsReq
	g.activeRetrievalsLk.Unlock()

	// tell GraphSync to cancel the request
	if (gsRequestID != graphsync.RequestID{}) {
		err := g.Cancel(ctx, gsRequestID)
		if err != nil {
			log.Info("unable to force close graphsync request %s: %s", tID, err)
		}
	}

	// send a message on data transfer
	err := g.dtnet.SendMessage(ctx, rcpt, message.CancelResponse(tID))
	g.failTransfer(state, errors.New("transfer cancelled by provider"))

	if err != nil {
		return fmt.Errorf("cancelling request for transfer %d: %w", id, err)
	}

	return nil
}

func (g *GraphsyncUnpaidRetrieval) List() []retrievalState {
	g.activeRetrievalsLk.Lock()
	defer g.activeRetrievalsLk.Unlock()

	values := make([]retrievalState, 0, len(g.activeRetrievals))

	for _, value := range g.activeRetrievals {
		values = append(values, *value)
	}

	return values
}

// Called when a transfer is received by graphsync and queued for processing
func (g *GraphsyncUnpaidRetrieval) RegisterIncomingRequestQueuedHook(hook graphsync.OnIncomingRequestQueuedHook) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterIncomingRequestQueuedHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.RequestQueuedHookActions) {
		log.Debugw("incoming request queued", "request", request)
		stats.Record(g.ctx, metrics.GraphsyncRequestQueuedCount.M(1))

		interceptRtvl, err := g.interceptRetrieval(p, request)
		if err != nil {
			log.Errorw("incoming request failed", "request", request, "error", err)
			return
		}

		if !interceptRtvl {
			hook(p, request, hookActions)
			stats.Record(g.ctx, metrics.GraphsyncRequestQueuedPaidCount.M(1))
			return
		}
		stats.Record(g.ctx, metrics.GraphsyncRequestQueuedUnpaidCount.M(1))
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
		log.Debugw("no extension found", "request", request)
		return false, nil
	}

	// When a data transfer request comes in on graphsync, the remote peer
	// initiated a pull request for data. If it's not a request, ignore it.
	if !msg.IsRequest() {
		log.Debugw("ignoring non-request message", "request", request)
		return false, nil
	}

	dtRequest := msg.(datatransfer.Request)
	if !dtRequest.IsNew() && !dtRequest.IsRestart() {
		// The request is not for a new retrieval (it's a cancel etc).
		// If this message is for an existing unpaid retrieval it will already
		// be in our map (because we must have already processed the new
		// retrieval request)
		_, ok := g.isActiveUnpaidRetrieval(reqId{p: p, id: msg.TransferID()})
		log.Debugw("ignoring non-new request", "request", request, "isActiveUnpaidRetrieval", ok)
		return ok, nil
	}

	// The request is for a new transfer / restart transfer, so check if it's
	// for an unpaid retrieval. We are explicitly checking for voucher type to be
	// legacyretrievaltypes.DealProposal{}. Rest are all rejected at this stage.
	voucher, decodeErr := g.decodeVoucher(dtRequest, g.decoder)
	if decodeErr != nil {
		// If we don't recognize the voucher, don't intercept the retrieval.
		// Instead it will be passed through to the legacy code for processing.
		log.Debugw("decoding new request voucher", "request", request, "err", decodeErr)
		if !errors.Is(decodeErr, ErrUnknownVoucher) {
			return false, fmt.Errorf("decoding new request voucher: %w", decodeErr)
		}
	}
	switch v := voucher.(type) {
	case *legacyretrievaltypes.DealProposal:
		// This is a retrieval deal
		proposal := *v
		log.Debugw("intercepting retrieval deal", "proposal", proposal)
		return g.handleRetrievalDeal(p, msg, proposal, request, RetrievalTypeDeal)
	case *migrations.DealProposal0:
		// This is a retrieval deal with an older format
		proposal := migrations.MigrateDealProposal0To1(*v)
		log.Debugw("intercepting retrieval deal v1", "proposal", proposal)
		return g.handleRetrievalDeal(p, msg, proposal, request, RetrievalTypeDeal)
	}

	log.Debugw("ignoring request", "request", request)
	return false, nil
}

func (g *GraphsyncUnpaidRetrieval) handleRetrievalDeal(peerID peer.ID, msg datatransfer.Message, proposal legacyretrievaltypes.DealProposal, request graphsync.RequestData, retType RetrievalType) (bool, error) {
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
		recipient:  peerID,
		status:     datatransfer.Requested,
		isPull:     true,
	}

	mktsState := &legacyretrievaltypes.ProviderDealState{
		DealProposal:  proposal,
		ChannelID:     &datatransfer.ChannelID{ID: msg.TransferID(), Initiator: peerID, Responder: g.peerID},
		Status:        legacyretrievaltypes.DealStatusNew,
		Receiver:      peerID,
		FundsReceived: abi.NewTokenAmount(0),
	}
	state := &retrievalState{
		retType: retType,
		cs:      cs,
		mkts:    mktsState,
		gsReq:   request.ID(),
	}

	// Record the data transfer ID so that we can intercept future
	// events for this transfer
	g.trackTransfer(peerID, msg.TransferID(), state)

	// Fire transfer queued event
	g.publishDTEvent(datatransfer.TransferRequestQueued, "", cs)

	// This is an unpaid retrieval, so this class is responsible for
	// handling it
	return true, nil
}

// Called by graphsync when an incoming request is processed
func (g *GraphsyncUnpaidRetrieval) RegisterIncomingRequestHook(hook graphsync.OnIncomingRequestHook) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterIncomingRequestHook(func(p peer.ID, request graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
		log.Debugw("incoming request", "request", request)
		stats.Record(g.ctx, metrics.GraphsyncRequestStartedCount.M(1))

		// Check if this is a request for a retrieval that we should handle
		msg, state, intercept := g.isRequestForActiveUnpaidRetrieval(p, request)
		if !intercept {
			// Otherwise pass it through to the legacy code
			hook(p, request, hookActions)
			stats.Record(g.ctx, metrics.GraphsyncRequestStartedPaidCount.M(1))
			log.Debugw("passing paid request through to legacy code", "request", request)
			return
		}

		stats.Record(g.ctx, metrics.GraphsyncRequestStartedUnpaidCount.M(1))

		var dtOpenMsg string
		if state.retType == RetrievalTypeLegs {
			dtOpenMsg = "request from network indexer"
		} else {
			dtOpenMsg = "unpaid retrieval"
		}
		if msg.IsRestart() {
			dtOpenMsg += " (restart)"
		}
		log.Debugw("handling unpaid request", "request", request, "msg", msg, "state", state)
		g.publishDTEvent(datatransfer.Open, dtOpenMsg, state.cs)
		g.publishMktsEvent(legacyretrievaltypes.ProviderEventOpen, *state.mkts)

		err := func() error {
			voucher, decodeErr := g.decodeVoucher(msg, g.decoder)
			if decodeErr != nil {
				log.Debugw("decoding new request voucher", "request", request, "err", decodeErr)
				return fmt.Errorf("decoding new request voucher: %w", decodeErr)
			}

			// Validate the request
			res, validateErr := g.validator.validatePullRequest(msg.IsRestart(), p, voucher, request.Root(), request.Selector())
			log.Debugw("validating request", "request", request, "result", res, "err", validateErr)

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
			log.Debugw("validation failed", "request", request, "err", err)
			hookActions.TerminateWithError(err)
			g.failTransfer(state, err)
			stats.Record(g.ctx, metrics.GraphsyncRequestStartedUnpaidFailCount.M(1))
			return
		}

		// Mark the request as valid
		hookActions.ValidateRequest()

		// Fire events
		state.cs.status = datatransfer.Ongoing
		g.publishDTEvent(datatransfer.Accept, "", state.cs)
		state.mkts.Status = legacyretrievaltypes.DealStatusUnsealing
		g.publishMktsEvent(legacyretrievaltypes.ProviderEventDealAccepted, *state.mkts)
		state.mkts.Status = legacyretrievaltypes.DealStatusUnsealed
		g.publishMktsEvent(legacyretrievaltypes.ProviderEventUnsealComplete, *state.mkts)

		stats.Record(g.ctx, metrics.GraphsyncRequestStartedUnpaidSuccessCount.M(1))
		log.Debugw("successfully validated request", "request", request)
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
			if p != g.peerID {
				stats.Record(g.ctx, metrics.GraphsyncRequestCompletedCount.M(1))
				stats.Record(g.ctx, metrics.GraphsyncRequestCompletedPaidCount.M(1))
			}
			return
		}

		// Check that it's an incoming response from the other peer
		// (not an outgoing response)
		if p == g.peerID {
			return
		}

		stats.Record(g.ctx, metrics.GraphsyncRequestCompletedCount.M(1))
		stats.Record(g.ctx, metrics.GraphsyncRequestCompletedUnpaidCount.M(1))

		defer g.untrackTransfer(p, msg.TransferID())

		// Request was cancelled, nothing further to do
		if status == graphsync.RequestCancelled {
			return
		}

		if status != graphsync.RequestCompletedFull {
			completeErr := fmt.Errorf("graphsync response to peer %s did not complete: response status code %s", p, status)
			g.failTransfer(state, completeErr)
			stats.Record(g.ctx, metrics.GraphsyncRequestCompletedUnpaidFailCount.M(1))
			return
		}

		// Fire markets blocks completed event
		state.mkts.Status = legacyretrievaltypes.DealStatusBlocksComplete
		g.publishMktsEvent(legacyretrievaltypes.ProviderEventBlocksCompleted, *state.mkts)

		// Include a markets protocol Completed message in the response
		dealResponse := &legacyretrievaltypes.DealResponse{
			ID:     state.mkts.ID,
			Status: legacyretrievaltypes.DealStatusCompleted,
		}

		const isAccepted = true
		const isPaused = false
		respMsg, err := message.CompleteResponse(msg.TransferID(), isAccepted, isPaused, dealResponse.Type(), dealResponse)
		if err != nil {
			g.failTransfer(state, fmt.Errorf("getting complete response: %w", err))
			return
		}

		// Send the other peer a message that the transfer has completed
		if err := g.dtnet.SendMessage(g.ctx, p, respMsg); err != nil {
			err := fmt.Errorf("failed to send completion message to requestor %s: %w", p, err)
			g.failTransfer(state, err)
			stats.Record(g.ctx, metrics.GraphsyncRequestCompletedUnpaidFailCount.M(1))
			return
		}

		state.cs.status = datatransfer.Completed
		g.publishDTEvent(datatransfer.Complete, "", state.cs)
		// Fire markets blocks completed event
		state.mkts.Status = legacyretrievaltypes.DealStatusCompleted
		g.publishMktsEvent(legacyretrievaltypes.ProviderEventComplete, *state.mkts)

		stats.Record(g.ctx, metrics.GraphsyncRequestCompletedUnpaidSuccessCount.M(1))
		log.Infow("successfully sent completion message to requestor", "peer", p)
	})
}

func (g *GraphsyncUnpaidRetrieval) RegisterRequestorCancelledListener(listener graphsync.OnRequestorCancelledListener) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterRequestorCancelledListener(func(p peer.ID, request graphsync.RequestData) {
		stats.Record(g.ctx, metrics.GraphsyncRequestClientCancelledCount.M(1))

		_, state, intercept := g.isRequestForActiveUnpaidRetrieval(p, request)
		if !intercept {
			listener(p, request)
			stats.Record(g.ctx, metrics.GraphsyncRequestClientCancelledPaidCount.M(1))
			return
		}

		state.cs.status = datatransfer.Cancelled
		g.publishDTEvent(datatransfer.Cancel, "client cancelled", state.cs)
		state.mkts.Status = legacyretrievaltypes.DealStatusCancelled
		g.publishMktsEvent(legacyretrievaltypes.ProviderEventCancelComplete, *state.mkts)

		g.untrackTransfer(p, state.cs.transferID)

		stats.Record(g.ctx, metrics.GraphsyncRequestClientCancelledUnpaidCount.M(1))
	})
}

func (g *GraphsyncUnpaidRetrieval) RegisterBlockSentListener(listener graphsync.OnBlockSentListener) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterBlockSentListener(func(p peer.ID, request graphsync.RequestData, block graphsync.BlockData) {
		sizeOnWire := block.BlockSizeOnWire()
		_, state, intercept := g.isRequestForActiveUnpaidRetrieval(p, request)
		if !intercept {
			listener(p, request, block)
			if sizeOnWire > 0 {
				stats.Record(g.ctx, metrics.GraphsyncRequestBlockSentCount.M(1))
				stats.Record(g.ctx, metrics.GraphsyncRequestBlockSentPaidCount.M(1))
				stats.Record(g.ctx, metrics.GraphsyncRequestBytesSentCount.M(int64(sizeOnWire)))
				stats.Record(g.ctx, metrics.GraphsyncRequestBytesSentPaidCount.M(int64(sizeOnWire)))
			}
			return
		}

		// When a data transfer is restarted, the requester sends a list of CIDs
		// that it already has. Graphsync calls the sent hook for all blocks even
		// if they are in the list (meaning, they aren't actually sent over the
		// wire). So here we check if the block was actually sent
		// over the wire before firing the data sent event.
		if sizeOnWire == 0 {
			return
		}

		// Fire block sent event
		state.cs.sent += block.BlockSizeOnWire()
		g.publishDTEvent(datatransfer.DataSent, "", state.cs)
		state.mkts.TotalSent += block.BlockSizeOnWire()

		stats.Record(g.ctx, metrics.GraphsyncRequestBlockSentCount.M(1))
		stats.Record(g.ctx, metrics.GraphsyncRequestBlockSentUnpaidCount.M(1))
		stats.Record(g.ctx, metrics.GraphsyncRequestBytesSentCount.M(int64(sizeOnWire)))
		stats.Record(g.ctx, metrics.GraphsyncRequestBytesSentUnpaidCount.M(int64(sizeOnWire)))
	})
}

func (g *GraphsyncUnpaidRetrieval) RegisterNetworkErrorListener(listener graphsync.OnNetworkErrorListener) graphsync.UnregisterHookFunc {
	return g.GraphExchange.RegisterNetworkErrorListener(func(p peer.ID, request graphsync.RequestData, err error) {
		stats.Record(g.ctx, metrics.GraphsyncRequestNetworkErrorCount.M(1))

		_, state, intercept := g.isRequestForActiveUnpaidRetrieval(p, request)
		if !intercept {
			listener(p, request, err)
			return
		}

		// Consider network errors as fatal, clients can send a new request if they wish

		// Cancel the graphsync retrieval
		cancelErr := g.Cancel(g.ctx, request.ID())
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
	state.mkts.Status = legacyretrievaltypes.DealStatusErrored
	g.publishMktsEvent(legacyretrievaltypes.ProviderEventDataTransferError, *state.mkts)

	g.untrackTransfer(state.cs.recipient, state.cs.transferID)
	log.Infow("transfer failed", "transfer id", state.cs.transferID, "peer", state.cs.recipient, "err", err)
}

var ErrUnknownVoucher = errors.New("unknown voucher type")

func (g *GraphsyncUnpaidRetrieval) decodeVoucher(request datatransfer.Request, registry *registry.Registry) (datatransfer.Voucher, error) {
	vtypStr := request.VoucherType()
	decoder, has := registry.Decoder(vtypStr)
	if !has {
		return nil, fmt.Errorf("voucher type: %s: %w", vtypStr, ErrUnknownVoucher)
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

func (g *GraphsyncUnpaidRetrieval) SubscribeToValidationEvents(sub ProviderValidationSubscriber) legacyretrievaltypes.Unsubscribe {
	return g.validator.Subscribe(sub)
}
