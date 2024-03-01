package datatransfer

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RequestValidator is an interface implemented by the client of the
// data transfer module to validate requests
type RequestValidator interface {
	// ValidatePush validates a push request received from the peer that will send data
	ValidatePush(
		isRestart bool,
		chid ChannelID,
		sender peer.ID,
		voucher Voucher,
		baseCid cid.Cid,
		selector ipld.Node) (VoucherResult, error)
	// ValidatePull validates a pull request received from the peer that will receive data
	ValidatePull(
		isRestart bool,
		chid ChannelID,
		receiver peer.ID,
		voucher Voucher,
		baseCid cid.Cid,
		selector ipld.Node) (VoucherResult, error)
}

// Revalidator is a request validator revalidates in progress requests
// by requesting request additional vouchers, and resuming when it receives them
type Revalidator interface {
	// Revalidate revalidates a request with a new voucher
	Revalidate(channelID ChannelID, voucher Voucher) (VoucherResult, error)
	// OnPullDataSent is called on the responder side when more bytes are sent
	// for a given pull request. The first value indicates whether the request was
	// recognized by this revalidator and should be considered 'handled'. If true,
	// the remaining two values are interpreted. If 'false' the request is passed on
	// to the next revalidators.
	// It should return a VoucherResult + ErrPause to
	// request revalidation or nil to continue uninterrupted,
	// other errors will terminate the request.
	OnPullDataSent(chid ChannelID, additionalBytesSent uint64) (bool, VoucherResult, error)
	// OnPushDataReceived is called on the responder side when more bytes are received
	// for a given push request. The first value indicates whether the request was
	// recognized by this revalidator and should be considered 'handled'. If true,
	// the remaining two values are interpreted. If 'false' the request is passed on
	// to the next revalidators. It should return a VoucherResult + ErrPause to
	// request revalidation or nil to continue uninterrupted,
	// other errors will terminate the request
	OnPushDataReceived(chid ChannelID, additionalBytesReceived uint64) (bool, VoucherResult, error)
	// OnComplete is called to make a final request for revalidation -- often for the
	// purpose of settlement. The first value indicates whether the request was
	// recognized by this revalidator and should be considered 'handled'. If true,
	// the remaining two values are interpreted. If 'false' the request is passed on
	// to the next revalidators.
	// if VoucherResult is non nil, the request will enter a settlement phase awaiting
	// a final update
	OnComplete(chid ChannelID) (bool, VoucherResult, error)
}

// TransportConfigurer provides a mechanism to provide transport specific configuration for a given voucher type
type TransportConfigurer func(chid ChannelID, voucher Voucher, transport Transport)

// ReadyFunc is function that gets called once when the data transfer module is ready
type ReadyFunc func(error)

// Manager is the core interface presented by all implementations of
// of the data transfer sub system
type Manager interface {

	// Start initializes data transfer processing
	Start(ctx context.Context) error

	// OnReady registers a listener for when the data transfer comes on line
	OnReady(ReadyFunc)

	// Stop terminates all data transfers and ends processing
	Stop(ctx context.Context) error

	// RegisterVoucherType registers a validator for the given voucher type
	// will error if voucher type does not implement voucher
	// or if there is a voucher type registered with an identical identifier
	RegisterVoucherType(voucherType Voucher, validator RequestValidator) error

	// RegisterRevalidator registers a revalidator for the given voucher type
	// Note: this is the voucher type used to revalidate. It can share a name
	// with the initial validator type and CAN be the same type, or a different type.
	// The revalidator can simply be the sampe as the original request validator,
	// or a different validator that satisfies the revalidator interface.
	RegisterRevalidator(voucherType Voucher, revalidator Revalidator) error

	// RegisterVoucherResultType allows deserialization of a voucher result,
	// so that a listener can read the metadata
	RegisterVoucherResultType(resultType VoucherResult) error

	// RegisterTransportConfigurer registers the given transport configurer to be run on requests with the given voucher
	// type
	RegisterTransportConfigurer(voucherType Voucher, configurer TransportConfigurer) error

	// open a data transfer that will send data to the recipient peer and
	// transfer parts of the piece that match the selector
	OpenPushDataChannel(ctx context.Context, to peer.ID, voucher Voucher, baseCid cid.Cid, selector ipld.Node) (ChannelID, error)

	// open a data transfer that will request data from the sending peer and
	// transfer parts of the piece that match the selector
	OpenPullDataChannel(ctx context.Context, to peer.ID, voucher Voucher, baseCid cid.Cid, selector ipld.Node) (ChannelID, error)

	// send an intermediate voucher as needed when the receiver sends a request for revalidation
	SendVoucher(ctx context.Context, chid ChannelID, voucher Voucher) error

	// close an open channel (effectively a cancel)
	CloseDataTransferChannel(ctx context.Context, chid ChannelID) error

	// pause a data transfer channel (only allowed if transport supports it)
	PauseDataTransferChannel(ctx context.Context, chid ChannelID) error

	// resume a data transfer channel (only allowed if transport supports it)
	ResumeDataTransferChannel(ctx context.Context, chid ChannelID) error

	// get status of a transfer
	TransferChannelStatus(ctx context.Context, x ChannelID) Status

	// get channel state
	ChannelState(ctx context.Context, chid ChannelID) (ChannelState, error)

	// get notified when certain types of events happen
	SubscribeToEvents(subscriber Subscriber) Unsubscribe

	// get all in progress transfers
	InProgressChannels(ctx context.Context) (map[ChannelID]ChannelState, error)

	// RestartDataTransferChannel restarts an existing data transfer channel
	RestartDataTransferChannel(ctx context.Context, chid ChannelID) error
}

/*
Transport defines the interface for a transport layer for data
transfer. Where the data transfer manager will coordinate setting up push and
pull requests, validation, etc, the transport layer is responsible for moving
data back and forth, and may be medium specific. For example, some transports
may have the ability to pause and resume requests, while others may not.
Some may support individual data events, while others may only support message
events. Some transport layers may opt to use the actual data transfer network
protocols directly while others may be able to encode messages in their own
data protocol.

Transport is the minimum interface that must be satisfied to serve as a datatransfer
transport layer. Transports must be able to open (open is always called by the receiving peer)
and close channels, and set at an event handler
*/
type Transport interface {
	// OpenChannel initiates an outgoing request for the other peer to send data
	// to us on this channel
	// Note: from a data transfer symantic standpoint, it doesn't matter if the
	// request is push or pull -- OpenChannel is called by the party that is
	// intending to receive data
	OpenChannel(
		ctx context.Context,
		dataSender peer.ID,
		channelID ChannelID,
		root ipld.Link,
		stor ipld.Node,
		channel ChannelState,
		msg Message,
	) error

	// CloseChannel closes the given channel
	CloseChannel(ctx context.Context, chid ChannelID) error
	// SetEventHandler sets the handler for events on channels
	SetEventHandler(events EventsHandler) error
	// CleanupChannel is called on the otherside of a cancel - removes any associated
	// data for the channel
	CleanupChannel(chid ChannelID)
	Shutdown(ctx context.Context) error
}

// EventsHandler are semantic data transfer events that happen as a result of graphsync hooks
type EventsHandler interface {
	// OnChannelOpened is called when we send a request for data to the other
	// peer on the given channel ID
	// return values are:
	// - error = ignore incoming data for this channel
	OnChannelOpened(chid ChannelID) error
	// OnResponseReceived is called when we receive a response to a request
	// - nil = continue receiving data
	// - error = cancel this request
	OnResponseReceived(chid ChannelID, msg Response) error
	// OnDataReceive is called when we receive data for the given channel ID
	// return values are:
	// - nil = proceed with sending data
	// - error = cancel this request
	// - err == ErrPause - pause this request
	OnDataReceived(chid ChannelID, link ipld.Link, size uint64, index int64, unique bool) error

	// OnDataQueued is called when data is queued for sending for the given channel ID
	// return values are:
	// message = data transfer message along with data
	// err = error
	// - nil = proceed with sending data
	// - error = cancel this request
	// - err == ErrPause - pause this request
	OnDataQueued(chid ChannelID, link ipld.Link, size uint64, index int64, unique bool) (Message, error)

	// OnDataSent is called when we send data for the given channel ID
	OnDataSent(chid ChannelID, link ipld.Link, size uint64, index int64, unique bool) error

	// OnTransferQueued is called when a new data transfer request is queued in the transport layer.
	OnTransferQueued(chid ChannelID)

	// OnRequestReceived is called when we receive a new request to send data
	// for the given channel ID
	// return values are:
	// message = data transfer message along with reply
	// err = error
	// - nil = proceed with sending data
	// - error = cancel this request
	// - err == ErrPause - pause this request (only for new requests)
	// - err == ErrResume - resume this request (only for update requests)
	OnRequestReceived(chid ChannelID, msg Request) (Response, error)
	// OnChannelCompleted is called when we finish transferring data for the given channel ID
	// Error returns are logged but otherwise have no effect
	OnChannelCompleted(chid ChannelID, err error) error

	// OnRequestCancelled is called when a request we opened (with the given channel Id) to
	// receive data is cancelled by us.
	// Error returns are logged but otherwise have no effect
	OnRequestCancelled(chid ChannelID, err error) error

	// OnRequestDisconnected is called when a network error occurs trying to send a request
	OnRequestDisconnected(chid ChannelID, err error) error

	// OnSendDataError is called when a network error occurs sending data
	// at the transport layer
	OnSendDataError(chid ChannelID, err error) error

	// OnReceiveDataError is called when a network error occurs receiving data
	// at the transport layer
	OnReceiveDataError(chid ChannelID, err error) error

	// OnContextAugment allows the transport to attach data transfer tracing information
	// to its local context, in order to create a hierarchical trace
	OnContextAugment(chid ChannelID) func(context.Context) context.Context
}

// PauseableTransport is a transport that can also pause and resume channels
type PauseableTransport interface {
	Transport
	// PauseChannel paused the given channel ID
	PauseChannel(ctx context.Context,
		chid ChannelID,
	) error
	// ResumeChannel resumes the given channel
	ResumeChannel(ctx context.Context,
		msg Message,
		chid ChannelID,
	) error
}
