package datatransfer

import (
	"fmt"
	"time"

	"github.com/filecoin-project/boost/datatransfer/encoding"
	"github.com/filecoin-project/go-statemachine/fsm"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p/core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"
)

//go:generate cbor-gen-for ChannelID ChannelStages ChannelStage Log

// TypeIdentifier is a unique string identifier for a type of encodable object in a
// registry
type TypeIdentifier string

// EmptyTypeIdentifier means there is no voucher present
const EmptyTypeIdentifier = TypeIdentifier("")

// Voucher is used to validate
// a data transfer request against the underlying storage or retrieval deal
// that precipitated it. The only requirement is a voucher can read and write
// from bytes, and has a string identifier type
type Voucher Registerable

// VoucherResult is used to provide option additional information about a
// voucher being rejected or accepted
type VoucherResult Registerable

// TransferID is an identifier for a data transfer, shared between
// request/responder and unique to the requester
type TransferID uint64

// TypedVoucher is a voucher or voucher result in IPLD form and an associated
// type identifier for that voucher or voucher result
type TypedVoucher struct {
	Voucher datamodel.Node
	Type    TypeIdentifier
}

// Registerable is a type of object in a registry. It must be encodable and must
// have a single method that uniquely identifies its type
type Registerable interface {
	encoding.Encodable
	// Type is a unique string identifier for this voucher type
	Type() TypeIdentifier
}

// Equals is a utility to compare that two TypedVouchers are the same - both type
// and the voucher's IPLD content
func (tv1 TypedVoucher) Equals(tv2 TypedVoucher) bool {
	return tv1.Type == tv2.Type && ipld.DeepEqual(tv1.Voucher, tv2.Voucher)
}

// ChannelID is a unique identifier for a channel, distinct by both the other
// party's peer ID + the transfer ID
type ChannelID struct {
	Initiator peer.ID
	Responder peer.ID
	ID        TransferID
}

func (c ChannelID) String() string {
	return fmt.Sprintf("%s-%s-%d", c.Initiator, c.Responder, c.ID)
}

// OtherParty returns the peer on the other side of the request, depending
// on whether this peer is the initiator or responder
func (c ChannelID) OtherParty(thisPeer peer.ID) peer.ID {
	if thisPeer == c.Initiator {
		return c.Responder
	}
	return c.Initiator
}

// Channel represents all the parameters for a single data transfer
type Channel interface {
	// TransferID returns the transfer id for this channel
	TransferID() TransferID

	// BaseCID returns the CID that is at the root of this data transfer
	BaseCID() cid.Cid

	// Selector returns the IPLD selector for this data transfer (represented as
	// an IPLD node)
	Selector() datamodel.Node

	// Voucher returns the initial voucher for this data transfer
	Voucher() Voucher

	// Sender returns the peer id for the node that is sending data
	Sender() peer.ID

	// Recipient returns the peer id for the node that is receiving data
	Recipient() peer.ID

	// TotalSize returns the total size for the data being transferred
	TotalSize() uint64

	// IsPull returns whether this is a pull request
	IsPull() bool

	// ChannelID returns the ChannelID for this request
	ChannelID() ChannelID

	// OtherPeer returns the counter party peer for this channel
	OtherPeer() peer.ID
}

// ChannelState is channel parameters plus it's current state
type ChannelState interface {
	Channel

	// SelfPeer returns the peer this channel belongs to
	SelfPeer() peer.ID

	// Status is the current status of this channel
	Status() Status

	// Sent returns the number of bytes sent
	Sent() uint64

	// Received returns the number of bytes received
	Received() uint64

	// Message offers additional information about the current status
	Message() string

	// Vouchers returns all vouchers sent on this channel
	Vouchers() []Voucher

	// VoucherResults are results of vouchers sent on the channel
	VoucherResults() []VoucherResult

	// LastVoucher returns the last voucher sent on the channel
	LastVoucher() Voucher

	// LastVoucherResult returns the last voucher result sent on the channel
	LastVoucherResult() VoucherResult

	// ReceivedCidsTotal returns the number of (non-unique) cids received so far
	// on the channel - note that a block can exist in more than one place in the DAG
	ReceivedCidsTotal() int64

	// QueuedCidsTotal returns the number of (non-unique) cids queued so far
	// on the channel - note that a block can exist in more than one place in the DAG
	QueuedCidsTotal() int64

	// SentCidsTotal returns the number of (non-unique) cids sent so far
	// on the channel - note that a block can exist in more than one place in the DAG
	SentCidsTotal() int64

	// Queued returns the number of bytes read from the node and queued for sending
	Queued() uint64

	// Stages returns the timeline of events this data transfer has gone through,
	// for observability purposes.
	//
	// It is unsafe for the caller to modify the return value, and changes
	// may not be persisted. It should be treated as immutable.
	Stages() *ChannelStages
}

// ChannelStages captures a timeline of the progress of a data transfer channel,
// grouped by stages.
//
// EXPERIMENTAL; subject to change.
type ChannelStages struct {
	// Stages contains an entry for every stage the channel has gone through.
	// Each stage then contains logs.
	Stages []*ChannelStage
}

// ChannelStage traces the execution of a data transfer channel stage.
//
// EXPERIMENTAL; subject to change.
type ChannelStage struct {
	// Human-readable fields.
	// TODO: these _will_ need to be converted to canonical representations, so
	//  they are machine readable.
	Name        string
	Description string

	// Timestamps.
	// TODO: may be worth adding an exit timestamp. It _could_ be inferred from
	//  the start of the next stage, or from the timestamp of the last log line
	//  if this is a terminal stage. But that's non-determistic and it relies on
	//  assumptions.
	CreatedTime cbg.CborTime
	UpdatedTime cbg.CborTime

	// Logs contains a detailed timeline of events that occurred inside
	// this stage.
	Logs []*Log
}

// Log represents a point-in-time event that occurred inside a channel stage.
//
// EXPERIMENTAL; subject to change.
type Log struct {
	// Log is a human readable message.
	//
	// TODO: this _may_ need to be converted to a canonical data model so it
	//  is machine-readable.
	Log string

	UpdatedTime cbg.CborTime
}

// AddLog adds a log to the specified stage, creating the stage if
// it doesn't exist yet.
//
// EXPERIMENTAL; subject to change.
func (cs *ChannelStages) AddLog(stage, msg string) {
	if cs == nil {
		return
	}

	now := curTime()
	st := cs.GetStage(stage)
	if st == nil {
		st = &ChannelStage{
			CreatedTime: now,
		}
		cs.Stages = append(cs.Stages, st)
	}

	st.Name = stage
	st.UpdatedTime = now
	if msg != "" && (len(st.Logs) == 0 || st.Logs[len(st.Logs)-1].Log != msg) {
		// only add the log if it's not a duplicate.
		st.Logs = append(st.Logs, &Log{msg, now})
	}
}

// GetStage returns the ChannelStage object for a named stage, or nil if not found.
//
// TODO: the input should be a strongly-typed enum instead of a free-form string.
// TODO: drop Get from GetStage to make this code more idiomatic. Return a
//
//	second ok boolean to make it even more idiomatic.
//
// EXPERIMENTAL; subject to change.
func (cs *ChannelStages) GetStage(stage string) *ChannelStage {
	if cs == nil {
		return nil
	}

	for _, s := range cs.Stages {
		if s.Name == stage {
			return s
		}
	}

	return nil
}

func curTime() cbg.CborTime {
	now := time.Now()
	return cbg.CborTime(time.Unix(0, now.UnixNano()).UTC())
}

// Status is the status of transfer for a given channel
type Status uint64

const (
	// Requested means a data transfer was requested by has not yet been approved
	Requested Status = iota

	// Ongoing means the data transfer is in progress
	Ongoing

	// TransferFinished indicates the initiator is done sending/receiving
	// data but is awaiting confirmation from the responder
	TransferFinished

	// ResponderCompleted indicates the initiator received a message from the
	// responder that it's completed
	ResponderCompleted

	// Finalizing means the responder is awaiting a final message from the initator to
	// consider the transfer done
	Finalizing

	// Completing just means we have some final cleanup for a completed request
	Completing

	// Completed means the data transfer is completed successfully
	Completed

	// Failing just means we have some final cleanup for a failed request
	Failing

	// Failed means the data transfer failed
	Failed

	// Cancelling just means we have some final cleanup for a cancelled request
	Cancelling

	// Cancelled means the data transfer ended prematurely
	Cancelled

	// DEPRECATED: Use InitiatorPaused() method on ChannelState
	InitiatorPaused

	// DEPRECATED: Use ResponderPaused() method on ChannelState
	ResponderPaused

	// DEPRECATED: Use BothPaused() method on ChannelState
	BothPaused

	// ResponderFinalizing is a unique state where the responder is awaiting a final voucher
	ResponderFinalizing

	// ResponderFinalizingTransferFinished is a unique state where the responder is awaiting a final voucher
	// and we have received all data
	ResponderFinalizingTransferFinished

	// ChannelNotFoundError means the searched for data transfer does not exist
	ChannelNotFoundError

	// Queued indicates a data transfer request has been accepted, but is not actively transfering yet
	Queued

	// AwaitingAcceptance indicates a transfer request is actively being processed by the transport
	// even if the remote has not yet responded that it's accepted the transfer. Such a state can
	// occur, for example, in a requestor-initiated transfer that starts processing prior to receiving
	// acceptance from the server.
	AwaitingAcceptance
)

type statusList []Status

func (sl statusList) Contains(s Status) bool {
	for _, ts := range sl {
		if ts == s {
			return true
		}
	}
	return false
}

func (sl statusList) AsFSMStates() []fsm.StateKey {
	sk := make([]fsm.StateKey, 0, len(sl))
	for _, s := range sl {
		sk = append(sk, s)
	}
	return sk
}

var NotAcceptedStates = statusList{
	Requested,
	AwaitingAcceptance,
	Cancelled,
	Cancelling,
	Failed,
	Failing,
	ChannelNotFoundError}

func (s Status) IsAccepted() bool {
	return !NotAcceptedStates.Contains(s)
}
func (s Status) String() string {
	return Statuses[s]
}

var FinalizationStatuses = statusList{Finalizing, Completed, Completing}

func (s Status) InFinalization() bool {
	return FinalizationStatuses.Contains(s)
}

var TransferCompleteStates = statusList{
	TransferFinished,
	ResponderFinalizingTransferFinished,
	Finalizing,
	Completed,
	Completing,
	Failing,
	Failed,
	Cancelling,
	Cancelled,
	ChannelNotFoundError,
}

func (s Status) TransferComplete() bool {
	return TransferCompleteStates.Contains(s)
}

var TransferringStates = statusList{
	Ongoing,
	ResponderCompleted,
	ResponderFinalizing,
	AwaitingAcceptance,
}

func (s Status) Transferring() bool {
	return TransferringStates.Contains(s)
}

// Statuses are human readable names for data transfer states
var Statuses = map[Status]string{
	// Requested means a data transfer was requested by has not yet been approved
	Requested:                           "Requested",
	Ongoing:                             "Ongoing",
	TransferFinished:                    "TransferFinished",
	ResponderCompleted:                  "ResponderCompleted",
	Finalizing:                          "Finalizing",
	Completing:                          "Completing",
	Completed:                           "Completed",
	Failing:                             "Failing",
	Failed:                              "Failed",
	Cancelling:                          "Cancelling",
	Cancelled:                           "Cancelled",
	InitiatorPaused:                     "InitiatorPaused",
	ResponderPaused:                     "ResponderPaused",
	BothPaused:                          "BothPaused",
	ResponderFinalizing:                 "ResponderFinalizing",
	ResponderFinalizingTransferFinished: "ResponderFinalizingTransferFinished",
	ChannelNotFoundError:                "ChannelNotFoundError",
	Queued:                              "Queued",
	AwaitingAcceptance:                  "AwaitingAcceptance",
}
