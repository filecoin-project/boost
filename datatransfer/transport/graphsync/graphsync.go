package graphsync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	graphsync "github.com/filecoin-project/boost-graphsync"
	"github.com/filecoin-project/boost-graphsync/donotsendfirstblocks"
	"github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/transport/graphsync/extension"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/sync/errgroup"
)

var log = logging.Logger("dt_graphsync")

// When restarting a data transfer, we cancel the existing graphsync request
// before opening a new one.
// This constant defines the maximum time to wait for the request to be
// cancelled.
const maxGSCancelWait = time.Second

var defaultSupportedExtensions = []graphsync.ExtensionName{
	extension.ExtensionDataTransfer1_1,
}

var incomingReqExtensions = []graphsync.ExtensionName{
	extension.ExtensionIncomingRequest1_1,
	extension.ExtensionDataTransfer1_1,
}

var outgoingBlkExtensions = []graphsync.ExtensionName{
	extension.ExtensionOutgoingBlock1_1,
	extension.ExtensionDataTransfer1_1,
}

// Option is an option for setting up the graphsync transport
type Option func(*Transport)

// SupportedExtensions sets what data transfer extensions are supported
func SupportedExtensions(supportedExtensions []graphsync.ExtensionName) Option {
	return func(t *Transport) {
		t.supportedExtensions = supportedExtensions
	}
}

// RegisterCompletedRequestListener is used by the tests
func RegisterCompletedRequestListener(l func(channelID datatransfer.ChannelID)) Option {
	return func(t *Transport) {
		t.completedRequestListener = l
	}
}

// RegisterCompletedResponseListener is used by the tests
func RegisterCompletedResponseListener(l func(channelID datatransfer.ChannelID)) Option {
	return func(t *Transport) {
		t.completedResponseListener = l
	}
}

// Transport manages graphsync hooks for data transfer, translating from
// graphsync hooks to semantic data transfer events
type Transport struct {
	events datatransfer.EventsHandler
	gs     graphsync.GraphExchange
	peerID peer.ID

	supportedExtensions       []graphsync.ExtensionName
	unregisterFuncs           []graphsync.UnregisterHookFunc
	completedRequestListener  func(channelID datatransfer.ChannelID)
	completedResponseListener func(channelID datatransfer.ChannelID)

	// Map from data transfer channel ID to information about that channel
	dtChannelsLk sync.RWMutex
	dtChannels   map[datatransfer.ChannelID]*dtChannel

	// Used in graphsync callbacks to map from graphsync request to the
	// associated data-transfer channel ID.
	requestIDToChannelID *requestIDToChannelIDMap
}

// NewTransport makes a new hooks manager with the given hook events interface
func NewTransport(peerID peer.ID, gs graphsync.GraphExchange, options ...Option) *Transport {
	t := &Transport{
		gs:                   gs,
		peerID:               peerID,
		supportedExtensions:  defaultSupportedExtensions,
		dtChannels:           make(map[datatransfer.ChannelID]*dtChannel),
		requestIDToChannelID: newRequestIDToChannelIDMap(),
	}
	for _, option := range options {
		option(t)
	}
	return t
}

// OpenChannel initiates an outgoing request for the other peer to send data
// to us on this channel
// Note: from a data transfer symantic standpoint, it doesn't matter if the
// request is push or pull -- OpenChannel is called by the party that is
// intending to receive data
func (t *Transport) OpenChannel(
	ctx context.Context,
	dataSender peer.ID,
	channelID datatransfer.ChannelID,
	root ipld.Link,
	stor ipld.Node,
	channel datatransfer.ChannelState,
	msg datatransfer.Message,
) error {
	if t.events == nil {
		return datatransfer.ErrHandlerNotSet
	}

	exts, err := extension.ToExtensionData(msg, t.supportedExtensions)
	if err != nil {
		return err
	}
	// If this is a restart request, the client can indicate the blocks that
	// it has already received, so that the provider knows not to resend
	// those blocks
	restartExts, err := t.getRestartExtension(ctx, dataSender, channel)
	if err != nil {
		return err
	}
	exts = append(exts, restartExts...)

	// Start tracking the data-transfer channel
	ch := t.trackDTChannel(channelID)

	// Open a graphsync request to the remote peer
	req, err := ch.open(ctx, channelID, dataSender, root, stor, channel, exts)
	if err != nil {
		return err
	}

	// Process incoming data
	go t.executeGsRequest(req)

	return nil
}

// Get the extension data for sending a Restart message, depending on the
// protocol version of the peer
func (t *Transport) getRestartExtension(ctx context.Context, p peer.ID, channel datatransfer.ChannelState) ([]graphsync.ExtensionData, error) {
	if channel == nil {
		return nil, nil
	}
	return getDoNotSendFirstBlocksExtension(channel)
}

// Skip the first N blocks because they were already received
func getDoNotSendFirstBlocksExtension(channel datatransfer.ChannelState) ([]graphsync.ExtensionData, error) {
	skipBlockCount := channel.ReceivedCidsTotal()
	data := donotsendfirstblocks.EncodeDoNotSendFirstBlocks(skipBlockCount)
	return []graphsync.ExtensionData{{
		Name: graphsync.ExtensionsDoNotSendFirstBlocks,
		Data: data,
	}}, nil
}

// Read from the graphsync response and error channels until they are closed,
// and return the last error on the error channel
func (t *Transport) consumeResponses(req *gsReq) error {
	var lastError error
	for range req.responseChan {
	}
	log.Debugf("channel %s: finished consuming graphsync response channel", req.channelID)

	for err := range req.errChan {
		lastError = err
	}
	log.Debugf("channel %s: finished consuming graphsync error channel", req.channelID)

	return lastError
}

// Read from the graphsync response and error channels until they are closed
// or there is an error, then call the channel completed callback
func (t *Transport) executeGsRequest(req *gsReq) {
	// Make sure to call the onComplete callback before returning
	defer func() {
		log.Infow("gs request complete for channel", "chid", req.channelID)
		req.onComplete()
	}()

	// Consume the response and error channels for the graphsync request
	lastError := t.consumeResponses(req)

	// Request cancelled by client
	if _, ok := lastError.(graphsync.RequestClientCancelledErr); ok {
		terr := fmt.Errorf("graphsync request cancelled")
		log.Warnf("channel %s: %s", req.channelID, terr)
		if err := t.events.OnRequestCancelled(req.channelID, terr); err != nil {
			log.Error(err)
		}
		return
	}

	// Request cancelled by responder
	if _, ok := lastError.(graphsync.RequestCancelledErr); ok {
		log.Infof("channel %s: graphsync request cancelled by responder", req.channelID)
		// TODO Should we do anything for RequestCancelledErr ?
		return
	}

	if lastError != nil {
		log.Warnf("channel %s: graphsync error: %s", req.channelID, lastError)
	}

	log.Debugf("channel %s: finished executing graphsync request", req.channelID)

	var completeErr error
	if lastError != nil {
		completeErr = fmt.Errorf("channel %s: graphsync request failed to complete: %w", req.channelID, lastError)
	}

	// Used by the tests to listen for when a request completes
	if t.completedRequestListener != nil {
		t.completedRequestListener(req.channelID)
	}

	err := t.events.OnChannelCompleted(req.channelID, completeErr)
	if err != nil {
		log.Errorf("channel %s: processing OnChannelCompleted: %s", req.channelID, err)
	}
}

// PauseChannel pauses the given data-transfer channel
func (t *Transport) PauseChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	ch, err := t.getDTChannel(chid)
	if err != nil {
		return err
	}
	return ch.pause(ctx)
}

// ResumeChannel resumes the given data-transfer channel and sends the message
// if there is one
func (t *Transport) ResumeChannel(
	ctx context.Context,
	msg datatransfer.Message,
	chid datatransfer.ChannelID,
) error {
	ch, err := t.getDTChannel(chid)
	if err != nil {
		return err
	}
	return ch.resume(ctx, msg)
}

// CloseChannel closes the given data-transfer channel
func (t *Transport) CloseChannel(ctx context.Context, chid datatransfer.ChannelID) error {
	ch, err := t.getDTChannel(chid)
	if err != nil {
		return err
	}

	err = ch.close(ctx)
	if err != nil {
		return fmt.Errorf("closing channel: %w", err)
	}
	return nil
}

// CleanupChannel is called on the otherside of a cancel - removes any associated
// data for the channel
func (t *Transport) CleanupChannel(chid datatransfer.ChannelID) {
	t.dtChannelsLk.Lock()

	ch, ok := t.dtChannels[chid]
	if ok {
		// Remove the reference to the channel from the channels map
		delete(t.dtChannels, chid)
	}

	t.dtChannelsLk.Unlock()

	// Clean up the channel
	if ok {
		ch.cleanup()
	}
}

// SetEventHandler sets the handler for events on channels
func (t *Transport) SetEventHandler(events datatransfer.EventsHandler) error {
	if t.events != nil {
		return datatransfer.ErrHandlerAlreadySet
	}
	t.events = events

	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterIncomingRequestQueuedHook(t.gsReqQueuedHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterIncomingRequestHook(t.gsReqRecdHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterCompletedResponseListener(t.gsCompletedResponseListener))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterIncomingBlockHook(t.gsIncomingBlockHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterOutgoingBlockHook(t.gsOutgoingBlockHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterBlockSentListener(t.gsBlockSentHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterOutgoingRequestHook(t.gsOutgoingRequestHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterIncomingResponseHook(t.gsIncomingResponseHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterRequestUpdatedHook(t.gsRequestUpdatedHook))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterRequestorCancelledListener(t.gsRequestorCancelledListener))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterNetworkErrorListener(t.gsNetworkSendErrorListener))
	t.unregisterFuncs = append(t.unregisterFuncs, t.gs.RegisterReceiverNetworkErrorListener(t.gsNetworkReceiveErrorListener))
	return nil
}

// Shutdown disconnects a transport interface from graphsync
func (t *Transport) Shutdown(ctx context.Context) error {
	for _, unregisterFunc := range t.unregisterFuncs {
		unregisterFunc()
	}

	t.dtChannelsLk.Lock()
	defer t.dtChannelsLk.Unlock()

	var eg errgroup.Group
	for _, ch := range t.dtChannels {
		ch := ch
		eg.Go(func() error {
			return ch.shutdown(ctx)
		})
	}

	err := eg.Wait()
	if err != nil {
		return fmt.Errorf("shutting down graphsync transport: %w", err)
	}
	return nil
}

// UseStore tells the graphsync transport to use the given loader and storer for this channelID
func (t *Transport) UseStore(channelID datatransfer.ChannelID, lsys ipld.LinkSystem) error {
	ch := t.trackDTChannel(channelID)
	return ch.useStore(lsys)
}

// ChannelGraphsyncRequests describes any graphsync request IDs associated with a given channel
type ChannelGraphsyncRequests struct {
	// Current is the current request ID for the transfer
	Current graphsync.RequestID
	// Previous are ids of previous GraphSync requests in a transfer that
	// has been restarted. We may be interested to know if these IDs are active
	// on either side of the request
	Previous []graphsync.RequestID
}

// ChannelsForPeer describes current active channels for a given peer and their
// associated graphsync requests
type ChannelsForPeer struct {
	SendingChannels   map[datatransfer.ChannelID]ChannelGraphsyncRequests
	ReceivingChannels map[datatransfer.ChannelID]ChannelGraphsyncRequests
}

// ChannelsForPeer identifies which channels are open and which request IDs they map to
func (t *Transport) ChannelsForPeer(p peer.ID) ChannelsForPeer {
	t.dtChannelsLk.RLock()
	defer t.dtChannelsLk.RUnlock()

	// cannot have active transfers with self
	if p == t.peerID {
		return ChannelsForPeer{
			SendingChannels:   map[datatransfer.ChannelID]ChannelGraphsyncRequests{},
			ReceivingChannels: map[datatransfer.ChannelID]ChannelGraphsyncRequests{},
		}
	}

	sending := make(map[datatransfer.ChannelID]ChannelGraphsyncRequests)
	receiving := make(map[datatransfer.ChannelID]ChannelGraphsyncRequests)
	// loop through every graphsync request key we're currently tracking
	t.requestIDToChannelID.forEach(func(requestID graphsync.RequestID, isSending bool, chid datatransfer.ChannelID) {
		// if the associated channel ID includes the requested peer
		if chid.Initiator == p || chid.Responder == p {
			// determine whether the requested peer is one at least one end of the channel
			// and whether we're receving from that peer or sending to it
			collection := sending
			if !isSending {
				collection = receiving
			}
			channelGraphsyncRequests := collection[chid]
			// finally, determine if the request key matches the current GraphSync key we're tracking for
			// this channel, indicating it's the current graphsync request
			if t.dtChannels[chid] != nil && t.dtChannels[chid].requestID != nil && (*t.dtChannels[chid].requestID) == requestID {
				channelGraphsyncRequests.Current = requestID
			} else {
				// otherwise this id was a previous graphsync request on a channel that was restarted
				// and it has not been cleaned up yet
				channelGraphsyncRequests.Previous = append(channelGraphsyncRequests.Previous, requestID)
			}
			collection[chid] = channelGraphsyncRequests
		}
	})
	return ChannelsForPeer{
		SendingChannels:   sending,
		ReceivingChannels: receiving,
	}
}

// gsOutgoingRequestHook is called when a graphsync request is made
func (t *Transport) gsOutgoingRequestHook(p peer.ID, request graphsync.RequestData, hookActions graphsync.OutgoingRequestHookActions) {
	message, _ := extension.GetTransferData(request, t.supportedExtensions)

	// extension not found; probably not our request.
	if message == nil {
		return
	}

	// A graphsync request is made when either
	// - The local node opens a data-transfer pull channel, so the local node
	//   sends a graphsync request to ask the remote peer for the data
	// - The remote peer opened a data-transfer push channel, and in response
	//   the local node sends a graphsync request to ask for the data
	var initiator peer.ID
	var responder peer.ID
	if message.IsRequest() {
		// This is a pull request so the data-transfer initiator is the local node
		initiator = t.peerID
		responder = p
	} else {
		// This is a push response so the data-transfer initiator is the remote
		// peer: They opened the push channel, we respond by sending a
		// graphsync request for the data
		initiator = p
		responder = t.peerID
	}
	chid := datatransfer.ChannelID{Initiator: initiator, Responder: responder, ID: message.TransferID()}

	// A data transfer channel was opened
	err := t.events.OnChannelOpened(chid)
	if err != nil {
		// There was an error opening the channel, bail out
		log.Errorf("processing OnChannelOpened for %s: %s", chid, err)
		t.CleanupChannel(chid)
		return
	}

	// Start tracking the channel if we're not already
	ch := t.trackDTChannel(chid)

	// Signal that the channel has been opened
	ch.gsReqOpened(request.ID(), hookActions)
}

// gsIncomingBlockHook is called when a block is received
func (t *Transport) gsIncomingBlockHook(p peer.ID, response graphsync.ResponseData, block graphsync.BlockData, hookActions graphsync.IncomingBlockHookActions) {
	chid, ok := t.requestIDToChannelID.load(response.RequestID())
	if !ok {
		return
	}

	err := t.events.OnDataReceived(chid, block.Link(), block.BlockSize(), block.Index(), block.BlockSizeOnWire() != 0)
	if err != nil && err != datatransfer.ErrPause {
		hookActions.TerminateWithError(err)
		return
	}

	if err == datatransfer.ErrPause {
		hookActions.PauseRequest()
	}
}

func (t *Transport) gsBlockSentHook(p peer.ID, request graphsync.RequestData, block graphsync.BlockData) {
	// When a data transfer is restarted, the requester sends a list of CIDs
	// that it already has. Graphsync calls the sent hook for all blocks even
	// if they are in the list (meaning, they aren't actually sent over the
	// wire). So here we check if the block was actually sent
	// over the wire before firing the data sent event.
	if block.BlockSizeOnWire() == 0 {
		return
	}

	chid, ok := t.requestIDToChannelID.load(request.ID())
	if !ok {
		return
	}

	if err := t.events.OnDataSent(chid, block.Link(), block.BlockSize(), block.Index(), block.BlockSizeOnWire() != 0); err != nil {
		log.Errorf("failed to process data sent: %+v", err)
	}
}

func (t *Transport) gsOutgoingBlockHook(p peer.ID, request graphsync.RequestData, block graphsync.BlockData, hookActions graphsync.OutgoingBlockHookActions) {
	// When a data transfer is restarted, the requester sends a list of CIDs
	// that it already has. Graphsync calls the outgoing block hook for all
	// blocks even if they are in the list (meaning, they aren't actually going
	// to be sent over the wire). So here we check if the block is actually
	// going to be sent over the wire before firing the data queued event.
	if block.BlockSizeOnWire() == 0 {
		return
	}

	chid, ok := t.requestIDToChannelID.load(request.ID())
	if !ok {
		return
	}

	// OnDataQueued is called when a block is queued to be sent to the remote
	// peer. It can return ErrPause to pause the response (eg if payment is
	// required) and it can return a message that will be sent with the block
	// (eg to ask for payment).
	msg, err := t.events.OnDataQueued(chid, block.Link(), block.BlockSize(), block.Index(), block.BlockSizeOnWire() != 0)
	if err != nil && err != datatransfer.ErrPause {
		hookActions.TerminateWithError(err)
		return
	}

	if err == datatransfer.ErrPause {
		hookActions.PauseResponse()
	}

	if msg != nil {
		// gsOutgoingBlockHook uses a unique extension name so it can be attached with data from a different hook
		// outgoingBlkExtensions also includes the default extension name so it remains compatible with all data-transfer protocol versions out there
		extensions, err := extension.ToExtensionData(msg, outgoingBlkExtensions)
		if err != nil {
			hookActions.TerminateWithError(err)
			return
		}
		for _, extension := range extensions {
			hookActions.SendExtensionData(extension)
		}
	}
}

// gsReqQueuedHook is called when graphsync enqueues an incoming request for data
func (t *Transport) gsReqQueuedHook(p peer.ID, request graphsync.RequestData, hookActions graphsync.RequestQueuedHookActions) {
	msg, err := extension.GetTransferData(request, t.supportedExtensions)
	if err != nil {
		log.Errorf("failed GetTransferData, req=%+v, err=%s", request, err)
	}
	// extension not found; probably not our request.
	if msg == nil {
		return
	}

	var chid datatransfer.ChannelID
	if msg.IsRequest() {
		// when a data transfer request comes in on graphsync, the remote peer
		// initiated a pull
		chid = datatransfer.ChannelID{ID: msg.TransferID(), Initiator: p, Responder: t.peerID}
		dtRequest := msg.(datatransfer.Request)
		if dtRequest.IsNew() {
			log.Infof("%s, pull request queued, req_id=%d", chid, request.ID())
			t.events.OnTransferQueued(chid)
		} else {
			log.Infof("%s, pull restart request queued, req_id=%d", chid, request.ID())
		}
	} else {
		// when a data transfer response comes in on graphsync, this node
		// initiated a push, and the remote peer responded with a request
		// for data
		chid = datatransfer.ChannelID{ID: msg.TransferID(), Initiator: t.peerID, Responder: p}
		response := msg.(datatransfer.Response)
		if response.IsNew() {
			log.Infof("%s, GS pull request queued in response to our push, req_id=%d", chid, request.ID())
			t.events.OnTransferQueued(chid)
		} else {
			log.Infof("%s, GS pull request queued in response to our restart push, req_id=%d", chid, request.ID())
		}
	}
	augmentContext := t.events.OnContextAugment(chid)
	if augmentContext != nil {
		hookActions.AugmentContext(augmentContext)
	}
}

// gsReqRecdHook is called when graphsync receives an incoming request for data
func (t *Transport) gsReqRecdHook(p peer.ID, request graphsync.RequestData, hookActions graphsync.IncomingRequestHookActions) {
	// if this is a push request the sender is us.
	msg, err := extension.GetTransferData(request, t.supportedExtensions)
	if err != nil {
		hookActions.TerminateWithError(err)
		return
	}

	// extension not found; probably not our request.
	if msg == nil {
		return
	}

	// An incoming graphsync request for data is received when either
	// - The remote peer opened a data-transfer pull channel, so the local node
	//   receives a graphsync request for the data
	// - The local node opened a data-transfer push channel, and in response
	//   the remote peer sent a graphsync request for the data, and now the
	//   local node receives that request for data
	var chid datatransfer.ChannelID
	var responseMessage datatransfer.Message
	var ch *dtChannel
	if msg.IsRequest() {
		// when a data transfer request comes in on graphsync, the remote peer
		// initiated a pull
		chid = datatransfer.ChannelID{ID: msg.TransferID(), Initiator: p, Responder: t.peerID}

		log.Debugf("%s: received request for data (pull), req_id=%d", chid, request.ID())

		// Lock the channel for the duration of this method
		ch = t.trackDTChannel(chid)
		ch.lk.Lock()
		defer ch.lk.Unlock()

		request := msg.(datatransfer.Request)
		responseMessage, err = t.events.OnRequestReceived(chid, request)
	} else {
		// when a data transfer response comes in on graphsync, this node
		// initiated a push, and the remote peer responded with a request
		// for data
		chid = datatransfer.ChannelID{ID: msg.TransferID(), Initiator: t.peerID, Responder: p}

		log.Debugf("%s: received request for data (push), req_id=%d", chid, request.ID())

		// Lock the channel for the duration of this method
		ch = t.trackDTChannel(chid)
		ch.lk.Lock()
		defer ch.lk.Unlock()

		response := msg.(datatransfer.Response)
		err = t.events.OnResponseReceived(chid, response)
	}

	// If we need to send a response, add the response message as an extension
	if responseMessage != nil {
		// gsReqRecdHook uses a unique extension name so it can be attached with data from a different hook
		// incomingReqExtensions also includes default extension name so it remains compatible with previous data-transfer
		// protocol versions out there.
		extensions, extensionErr := extension.ToExtensionData(responseMessage, incomingReqExtensions)
		if extensionErr != nil {
			hookActions.TerminateWithError(err)
			return
		}
		for _, extension := range extensions {
			hookActions.SendExtensionData(extension)
		}
	}

	if err != nil && err != datatransfer.ErrPause {
		hookActions.TerminateWithError(err)
		return
	}

	// Check if the callback indicated that the channel should be paused
	// immediately (eg because data is still being unsealed)
	paused := false
	if err == datatransfer.ErrPause {
		log.Debugf("%s: pausing graphsync response", chid)

		paused = true
		hookActions.PauseResponse()
	}

	// If this is a restart request, and the data transfer still hasn't got
	// out of the paused state (eg because we're still unsealing), start this
	// graphsync response in the paused state.
	if ch.isOpen && !ch.xferStarted && !paused {
		log.Debugf("%s: pausing graphsync response after restart", chid)

		paused = true
		hookActions.PauseResponse()
	}

	// If the transfer is not paused, record that the transfer has started
	if !paused {
		ch.xferStarted = true
	}

	ch.gsDataRequestRcvd(request.ID(), hookActions)

	hookActions.ValidateRequest()
}

// gsCompletedResponseListener is a graphsync.OnCompletedResponseListener. We use it learn when the data transfer is complete
// for the side that is responding to a graphsync request
func (t *Transport) gsCompletedResponseListener(p peer.ID, request graphsync.RequestData, status graphsync.ResponseStatusCode) {
	chid, ok := t.requestIDToChannelID.load(request.ID())
	if !ok {
		return
	}

	if status == graphsync.RequestCancelled {
		return
	}

	var completeErr error
	if status != graphsync.RequestCompletedFull {
		statusStr := gsResponseStatusCodeString(status)
		completeErr = fmt.Errorf("graphsync response to peer %s did not complete: response status code %s", p, statusStr)
	}

	// Used by the tests to listen for when a response completes
	if t.completedResponseListener != nil {
		t.completedResponseListener(chid)
	}

	err := t.events.OnChannelCompleted(chid, completeErr)
	if err != nil {
		log.Error(err)
	}
}

// Remove this map once this PR lands: https://github.com/ipfs/go-graphsync/pull/148
var gsResponseStatusCodes = map[graphsync.ResponseStatusCode]string{
	graphsync.RequestAcknowledged:          "RequestAcknowledged",
	graphsync.AdditionalPeers:              "AdditionalPeers",
	graphsync.NotEnoughGas:                 "NotEnoughGas",
	graphsync.OtherProtocol:                "OtherProtocol",
	graphsync.PartialResponse:              "PartialResponse",
	graphsync.RequestPaused:                "RequestPaused",
	graphsync.RequestCompletedFull:         "RequestCompletedFull",
	graphsync.RequestCompletedPartial:      "RequestCompletedPartial",
	graphsync.RequestRejected:              "RequestRejected",
	graphsync.RequestFailedBusy:            "RequestFailedBusy",
	graphsync.RequestFailedUnknown:         "RequestFailedUnknown",
	graphsync.RequestFailedLegal:           "RequestFailedLegal",
	graphsync.RequestFailedContentNotFound: "RequestFailedContentNotFound",
	graphsync.RequestCancelled:             "RequestCancelled",
}

func gsResponseStatusCodeString(code graphsync.ResponseStatusCode) string {
	str, ok := gsResponseStatusCodes[code]
	if ok {
		return str
	}
	return gsResponseStatusCodes[graphsync.RequestFailedUnknown]
}

func (t *Transport) gsRequestUpdatedHook(p peer.ID, request graphsync.RequestData, update graphsync.RequestData, hookActions graphsync.RequestUpdatedHookActions) {
	chid, ok := t.requestIDToChannelID.load(request.ID())
	if !ok {
		return
	}

	responseMessage, err := t.processExtension(chid, update, p, t.supportedExtensions)

	if responseMessage != nil {
		extensions, extensionErr := extension.ToExtensionData(responseMessage, t.supportedExtensions)
		if extensionErr != nil {
			hookActions.TerminateWithError(err)
			return
		}
		for _, extension := range extensions {
			hookActions.SendExtensionData(extension)
		}
	}

	if err != nil && err != datatransfer.ErrPause {
		hookActions.TerminateWithError(err)
	}

}

// gsIncomingResponseHook is a graphsync.OnIncomingResponseHook. We use it to pass on responses
func (t *Transport) gsIncomingResponseHook(p peer.ID, response graphsync.ResponseData, hookActions graphsync.IncomingResponseHookActions) {
	chid, ok := t.requestIDToChannelID.load(response.RequestID())
	if !ok {
		return
	}

	responseMessage, err := t.processExtension(chid, response, p, incomingReqExtensions)

	if responseMessage != nil {
		extensions, extensionErr := extension.ToExtensionData(responseMessage, t.supportedExtensions)
		if extensionErr != nil {
			hookActions.TerminateWithError(err)
			return
		}
		for _, extension := range extensions {
			hookActions.UpdateRequestWithExtensions(extension)
		}
	}

	if err != nil {
		hookActions.TerminateWithError(err)
	}

	// In a case where the transfer sends blocks immediately this extension may contain both a
	// response message and a revalidation request so we trigger OnResponseReceived again for this
	// specific extension name
	_, err = t.processExtension(chid, response, p, []graphsync.ExtensionName{extension.ExtensionOutgoingBlock1_1})

	if err != nil {
		hookActions.TerminateWithError(err)
	}
}

func (t *Transport) processExtension(chid datatransfer.ChannelID, gsMsg extension.GsExtended, p peer.ID, exts []graphsync.ExtensionName) (datatransfer.Message, error) {

	// if this is a push request the sender is us.
	msg, err := extension.GetTransferData(gsMsg, exts)
	if err != nil {
		return nil, err
	}

	// extension not found; probably not our request.
	if msg == nil {
		return nil, nil
	}

	if msg.IsRequest() {

		// only accept request message updates when original message was also request
		if (chid != datatransfer.ChannelID{ID: msg.TransferID(), Initiator: p, Responder: t.peerID}) {
			return nil, errors.New("received request on response channel")
		}
		dtRequest := msg.(datatransfer.Request)
		return t.events.OnRequestReceived(chid, dtRequest)
	}

	// only accept response message updates when original message was also response
	if (chid != datatransfer.ChannelID{ID: msg.TransferID(), Initiator: t.peerID, Responder: p}) {
		return nil, errors.New("received response on request channel")
	}

	dtResponse := msg.(datatransfer.Response)
	return nil, t.events.OnResponseReceived(chid, dtResponse)
}

func (t *Transport) gsRequestorCancelledListener(p peer.ID, request graphsync.RequestData) {
	chid, ok := t.requestIDToChannelID.load(request.ID())
	if !ok {
		return
	}

	ch, err := t.getDTChannel(chid)
	if err != nil {
		if !errors.Is(datatransfer.ErrChannelNotFound, err) {
			log.Errorf("requestor cancelled: getting channel %s: %s", chid, err)
		}
		return
	}

	log.Debugf("%s: requester cancelled data-transfer", chid)
	ch.onRequesterCancelled()
}

// Called when there is a graphsync error sending data
func (t *Transport) gsNetworkSendErrorListener(p peer.ID, request graphsync.RequestData, gserr error) {
	// Fire an error if the graphsync request was made by this node or the remote peer
	chid, ok := t.requestIDToChannelID.load(request.ID())
	if !ok {
		return
	}

	err := t.events.OnSendDataError(chid, gserr)
	if err != nil {
		log.Errorf("failed to fire transport send error %s: %s", gserr, err)
	}
}

// Called when there is a graphsync error receiving data
func (t *Transport) gsNetworkReceiveErrorListener(p peer.ID, gserr error) {
	// Fire a receive data error on all ongoing graphsync transfers with that
	// peer
	t.requestIDToChannelID.forEach(func(k graphsync.RequestID, sending bool, chid datatransfer.ChannelID) {
		if chid.Initiator != p && chid.Responder != p {
			return
		}

		err := t.events.OnReceiveDataError(chid, gserr)
		if err != nil {
			log.Errorf("failed to fire transport receive error %s: %s", gserr, err)
		}
	})
}

func (t *Transport) newDTChannel(chid datatransfer.ChannelID) *dtChannel {
	return &dtChannel{
		t:         t,
		channelID: chid,
		opened:    make(chan graphsync.RequestID, 1),
	}
}

func (t *Transport) trackDTChannel(chid datatransfer.ChannelID) *dtChannel {
	t.dtChannelsLk.Lock()
	defer t.dtChannelsLk.Unlock()

	ch, ok := t.dtChannels[chid]
	if !ok {
		ch = t.newDTChannel(chid)
		t.dtChannels[chid] = ch
	}

	return ch
}

func (t *Transport) getDTChannel(chid datatransfer.ChannelID) (*dtChannel, error) {
	if t.events == nil {
		return nil, datatransfer.ErrHandlerNotSet
	}

	t.dtChannelsLk.RLock()
	defer t.dtChannelsLk.RUnlock()

	ch, ok := t.dtChannels[chid]
	if !ok {
		return nil, fmt.Errorf("channel %s: %w", chid, datatransfer.ErrChannelNotFound)
	}
	return ch, nil
}

// Info needed to keep track of a data transfer channel
type dtChannel struct {
	channelID datatransfer.ChannelID
	t         *Transport

	lk                 sync.RWMutex
	isOpen             bool
	requestID          *graphsync.RequestID
	completed          chan struct{}
	requesterCancelled bool
	xferStarted        bool
	pendingExtensions  []graphsync.ExtensionData

	opened chan graphsync.RequestID

	storeLk         sync.RWMutex
	storeRegistered bool
}

// Info needed to monitor an ongoing graphsync request
type gsReq struct {
	channelID    datatransfer.ChannelID
	responseChan <-chan graphsync.ResponseProgress
	errChan      <-chan error
	onComplete   func()
}

// Open a graphsync request for data to the remote peer
func (c *dtChannel) open(
	ctx context.Context,
	chid datatransfer.ChannelID,
	dataSender peer.ID,
	root ipld.Link,
	stor ipld.Node,
	channel datatransfer.ChannelState,
	exts []graphsync.ExtensionData,
) (*gsReq, error) {
	c.lk.Lock()
	defer c.lk.Unlock()

	// If there is an existing graphsync request for this channelID
	if c.requestID != nil {
		// Cancel the existing graphsync request
		completed := c.completed
		errch := c.cancel(ctx)

		// Wait for the complete callback to be called
		err := waitForCompleteHook(ctx, completed)
		if err != nil {
			return nil, fmt.Errorf("%s: waiting for cancelled graphsync request to complete: %w", chid, err)
		}

		// Wait for the cancel request method to complete
		select {
		case err = <-errch:
		case <-ctx.Done():
			err = fmt.Errorf("timed out waiting for graphsync request to be cancelled")
		}
		if err != nil {
			return nil, fmt.Errorf("%s: restarting graphsync request: %w", chid, err)
		}
	}

	// Set up a completed channel that will be closed when the request
	// completes (or is cancelled)
	completed := make(chan struct{})
	var onCompleteOnce sync.Once
	onComplete := func() {
		// Ensure the channel is only closed once
		onCompleteOnce.Do(func() {
			log.Debugw("closing the completion ch for data-transfer channel", "chid", chid)
			close(completed)
		})
	}
	c.completed = completed

	// Open a new graphsync request
	msg := fmt.Sprintf("Opening graphsync request to %s for root %s", dataSender, root)
	if channel != nil {
		msg += fmt.Sprintf(" with %d Blocks already received", channel.ReceivedCidsTotal())
	}
	log.Info(msg)
	responseChan, errChan := c.t.gs.Request(ctx, dataSender, root, stor, exts...)

	// Wait for graphsync "request opened" callback
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case requestID := <-c.opened:
		// Mark the channel as open and save the Graphsync request key
		c.isOpen = true
		c.requestID = &requestID
	}

	return &gsReq{
		channelID:    chid,
		responseChan: responseChan,
		errChan:      errChan,
		onComplete:   onComplete,
	}, nil
}

func waitForCompleteHook(ctx context.Context, completed chan struct{}) error {
	// Wait for the cancel to propagate through to graphsync, and for
	// the graphsync request to complete
	select {
	case <-completed:
		return nil
	case <-time.After(maxGSCancelWait):
		// Fail-safe: give up waiting after a certain amount of time
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// gsReqOpened is called when graphsync makes a request to the remote peer to ask for data
func (c *dtChannel) gsReqOpened(requestID graphsync.RequestID, hookActions graphsync.OutgoingRequestHookActions) {
	// Tell graphsync to store the received blocks in the registered store
	if c.hasStore() {
		hookActions.UsePersistenceOption("data-transfer-" + c.channelID.String())
	}
	log.Infow("outgoing graphsync request", "peer", c.channelID.OtherParty(c.t.peerID), "graphsync request id", requestID, "data transfer channel id", c.channelID)
	// Save a mapping from the graphsync key to the channel ID so that
	// subsequent graphsync callbacks are associated with this channel
	c.t.requestIDToChannelID.set(requestID, false, c.channelID)

	c.opened <- requestID
}

// gsDataRequestRcvd is called when the transport receives an incoming request
// for data.
// Note: Must be called under the lock.
func (c *dtChannel) gsDataRequestRcvd(requestID graphsync.RequestID, hookActions graphsync.IncomingRequestHookActions) {
	log.Debugf("%s: received request for data, req_id=%d", c.channelID, requestID)

	// If the requester had previously cancelled their request, send any
	// message that was queued since the cancel
	if c.requesterCancelled {
		c.requesterCancelled = false

		extensions := c.pendingExtensions
		c.pendingExtensions = nil
		for _, ext := range extensions {
			hookActions.SendExtensionData(ext)
		}
	}

	// Tell graphsync to load blocks from the registered store
	if c.hasStore() {
		hookActions.UsePersistenceOption("data-transfer-" + c.channelID.String())
	}

	// Save a mapping from the graphsync key to the channel ID so that
	// subsequent graphsync callbacks are associated with this channel
	c.requestID = &requestID
	log.Infow("incoming graphsync request", "peer", c.channelID.OtherParty(c.t.peerID), "graphsync request id", requestID, "data transfer channel id", c.channelID)
	c.t.requestIDToChannelID.set(requestID, true, c.channelID)

	c.isOpen = true
}

func (c *dtChannel) pause(ctx context.Context) error {
	c.lk.Lock()
	defer c.lk.Unlock()

	// Check if the channel was already cancelled
	if c.requestID == nil {
		log.Debugf("%s: channel was cancelled so not pausing channel", c.channelID)
		return nil
	}

	// If the requester cancelled, bail out
	if c.requesterCancelled {
		log.Debugf("%s: requester has cancelled so not pausing response", c.channelID)
		return nil
	}

	// Pause the response
	log.Debugf("%s: pausing response", c.channelID)
	return c.t.gs.Pause(ctx, *c.requestID)
}

func (c *dtChannel) resume(ctx context.Context, msg datatransfer.Message) error {
	c.lk.Lock()
	defer c.lk.Unlock()

	// Check if the channel was already cancelled
	if c.requestID == nil {
		log.Debugf("%s: channel was cancelled so not resuming channel", c.channelID)
		return nil
	}

	var extensions []graphsync.ExtensionData
	if msg != nil {
		var err error
		extensions, err = extension.ToExtensionData(msg, c.t.supportedExtensions)
		if err != nil {
			return err
		}
	}

	// If the requester cancelled, bail out
	if c.requesterCancelled {
		// If there was an associated message, we still want to send it to the
		// remote peer. We're not sending any message now, so instead queue up
		// the message to be sent next time the peer makes a request to us.
		c.pendingExtensions = append(c.pendingExtensions, extensions...)

		log.Debugf("%s: requester has cancelled so not unpausing response", c.channelID)
		return nil
	}

	// Record that the transfer has started
	c.xferStarted = true

	log.Debugf("%s: unpausing response", c.channelID)
	return c.t.gs.Unpause(ctx, *c.requestID, extensions...)
}

func (c *dtChannel) close(ctx context.Context) error {
	var errch chan error
	c.lk.Lock()
	{
		// Check if the channel was already cancelled
		if c.requestID != nil {
			errch = c.cancel(ctx)
		}
	}
	c.lk.Unlock()

	// Wait for the cancel message to complete
	select {
	case err := <-errch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Called when the responder gets a cancel message from the requester
func (c *dtChannel) onRequesterCancelled() {
	c.lk.Lock()
	defer c.lk.Unlock()

	c.requesterCancelled = true
}

func (c *dtChannel) hasStore() bool {
	c.storeLk.RLock()
	defer c.storeLk.RUnlock()

	return c.storeRegistered
}

// Use the given loader and storer to get / put blocks for the data-transfer.
// Note that each data-transfer channel uses a separate blockstore.
func (c *dtChannel) useStore(lsys ipld.LinkSystem) error {
	c.storeLk.Lock()
	defer c.storeLk.Unlock()

	// Register the channel's store with graphsync
	err := c.t.gs.RegisterPersistenceOption("data-transfer-"+c.channelID.String(), lsys)
	if err != nil {
		return err
	}

	c.storeRegistered = true

	return nil
}

func (c *dtChannel) cleanup() {
	c.lk.Lock()
	defer c.lk.Unlock()

	log.Debugf("%s: cleaning up channel", c.channelID)

	if c.hasStore() {
		// Unregister the channel's store from graphsync
		opt := "data-transfer-" + c.channelID.String()
		err := c.t.gs.UnregisterPersistenceOption(opt)
		if err != nil {
			log.Errorf("failed to unregister persistence option %s: %s", opt, err)
		}
	}

	// Clean up mapping from gs key to channel ID
	c.t.requestIDToChannelID.deleteRefs(c.channelID)
}

func (c *dtChannel) shutdown(ctx context.Context) error {
	// Cancel the graphsync request
	c.lk.Lock()
	errch := c.cancel(ctx)
	c.lk.Unlock()

	// Wait for the cancel message to complete
	select {
	case err := <-errch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Cancel the graphsync request.
// Note: must be called under the lock.
func (c *dtChannel) cancel(ctx context.Context) chan error {
	errch := make(chan error, 1)

	// Check that the request has not already been cancelled
	if c.requesterCancelled || c.requestID == nil {
		errch <- nil
		return errch
	}

	// Clear the graphsync key to indicate that the request has been cancelled
	requestID := c.requestID
	c.requestID = nil

	go func() {
		log.Debugf("%s: cancelling request", c.channelID)
		err := c.t.gs.Cancel(ctx, *requestID)

		// Ignore "request not found" errors
		if err != nil && !errors.Is(graphsync.RequestNotFoundErr{}, err) {
			errch <- fmt.Errorf("cancelling graphsync request for channel %s: %w", c.channelID, err)
		} else {
			errch <- nil
		}
	}()

	return errch
}

type channelInfo struct {
	sending   bool
	channelID datatransfer.ChannelID
}

// Used in graphsync callbacks to map from graphsync request to the
// associated data-transfer channel ID.
type requestIDToChannelIDMap struct {
	lk sync.RWMutex
	m  map[graphsync.RequestID]channelInfo
}

func newRequestIDToChannelIDMap() *requestIDToChannelIDMap {
	return &requestIDToChannelIDMap{
		m: make(map[graphsync.RequestID]channelInfo),
	}
}

// get the value for a key
func (m *requestIDToChannelIDMap) load(key graphsync.RequestID) (datatransfer.ChannelID, bool) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	val, ok := m.m[key]
	return val.channelID, ok
}

// set the value for a key
func (m *requestIDToChannelIDMap) set(key graphsync.RequestID, sending bool, chid datatransfer.ChannelID) {
	m.lk.Lock()
	defer m.lk.Unlock()

	m.m[key] = channelInfo{sending, chid}
}

// call f for each key / value in the map
func (m *requestIDToChannelIDMap) forEach(f func(k graphsync.RequestID, isSending bool, chid datatransfer.ChannelID)) {
	m.lk.RLock()
	defer m.lk.RUnlock()

	for k, ch := range m.m {
		f(k, ch.sending, ch.channelID)
	}
}

// delete any keys that reference this value
func (m *requestIDToChannelIDMap) deleteRefs(id datatransfer.ChannelID) {
	m.lk.Lock()
	defer m.lk.Unlock()

	for k, ch := range m.m {
		if ch.channelID == id {
			delete(m.m, k)
		}
	}
}
