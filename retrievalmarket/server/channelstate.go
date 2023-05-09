package server

import (
	"bytes"

	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/libp2p/go-libp2p/core/peer"
	cbg "github.com/whyrusleeping/cbor-gen"
)

type RetrievalType string

const RetrievalTypeDeal RetrievalType = "Deal"
const RetrievalTypeLegs RetrievalType = "Legs"

type retrievalState struct {
	retType RetrievalType
	cs      *channelState
	mkts    *retrievalmarket.ProviderDealState
}

func (r retrievalState) ChannelState() channelState                           { return *r.cs }
func (r retrievalState) ProviderDealState() retrievalmarket.ProviderDealState { return *r.mkts }

// channelState is immutable channel data plus mutable state
type channelState struct {
	// peerId of the manager peer
	selfPeer peer.ID
	// an identifier for this channel shared by request and responder, set by requester through protocol
	transferID datatransfer.TransferID
	// base CID for the piece being transferred
	baseCid cid.Cid
	// portion of Piece to return, specified by an IPLD selector
	selector *cbg.Deferred
	// the party that is sending the data (not who initiated the request)
	sender peer.ID
	// the party that is receiving the data (not who initiated the request)
	recipient peer.ID
	// expected amount of data to be transferred
	totalSize uint64
	// current status of this deal
	status datatransfer.Status
	// isPull indicates if this is a push or pull request
	isPull bool
	// total bytes read from this node and queued for sending (0 if receiver)
	queued uint64
	// total bytes sent from this node (0 if receiver)
	sent uint64
	// total bytes received by this node (0 if sender)
	received uint64
	// number of blocks that have been received, including blocks that are
	// present in more than one place in the DAG
	receivedBlocksTotal int64
	// Number of blocks that have been queued, including blocks that are
	// present in more than one place in the DAG
	queuedBlocksTotal int64
	// Number of blocks that have been sent, including blocks that are
	// present in more than one place in the DAG
	sentBlocksTotal int64
	// more informative status on a channel
	message string
}

// EmptyChannelState is the zero value for channel state, meaning not present
var EmptyChannelState = channelState{}

// Status is the current status of this channel
func (c channelState) Status() datatransfer.Status { return c.status }

// Received returns the number of bytes received
func (c channelState) Queued() uint64 { return c.queued }

// Sent returns the number of bytes sent
func (c channelState) Sent() uint64 { return c.sent }

// Received returns the number of bytes received
func (c channelState) Received() uint64 { return c.received }

// TransferID returns the transfer id for this channel
func (c channelState) TransferID() datatransfer.TransferID { return c.transferID }

// BaseCID returns the CID that is at the root of this data transfer
func (c channelState) BaseCID() cid.Cid { return c.baseCid }

// Selector returns the IPLD selector for this data transfer (represented as
// an IPLD node)
func (c channelState) Selector() ipld.Node {
	builder := basicnode.Prototype.Any.NewBuilder()
	reader := bytes.NewReader(c.selector.Raw)
	err := dagcbor.Decode(builder, reader)
	if err != nil {
		log.Error(err)
	}
	return builder.Build()
}

// Voucher returns the voucher for this data transfer
func (c channelState) Voucher() datatransfer.Voucher {
	return nil
}

// ReceivedCidsTotal returns the number of (non-unique) cids received so far
// on the channel - note that a block can exist in more than one place in the DAG
func (c channelState) ReceivedCidsTotal() int64 {
	return c.receivedBlocksTotal
}

// QueuedCidsTotal returns the number of (non-unique) cids queued so far
// on the channel - note that a block can exist in more than one place in the DAG
func (c channelState) QueuedCidsTotal() int64 {
	return c.queuedBlocksTotal
}

// SentCidsTotal returns the number of (non-unique) cids sent so far
// on the channel - note that a block can exist in more than one place in the DAG
func (c channelState) SentCidsTotal() int64 {
	return c.sentBlocksTotal
}

// Sender returns the peer id for the node that is sending data
func (c channelState) Sender() peer.ID { return c.sender }

// Recipient returns the peer id for the node that is receiving data
func (c channelState) Recipient() peer.ID { return c.recipient }

// TotalSize returns the total size for the data being transferred
func (c channelState) TotalSize() uint64 { return c.totalSize }

// IsPull returns whether this is a pull request based on who initiated it
func (c channelState) IsPull() bool {
	return c.isPull
}

func (c channelState) ChannelID() datatransfer.ChannelID {
	if c.isPull {
		return datatransfer.ChannelID{ID: c.transferID, Initiator: c.recipient, Responder: c.sender}
	}
	return datatransfer.ChannelID{ID: c.transferID, Initiator: c.sender, Responder: c.recipient}
}

func (c channelState) Message() string {
	return c.message
}

func (c channelState) Vouchers() []datatransfer.Voucher {
	return nil
}

func (c channelState) LastVoucher() datatransfer.Voucher {
	return nil
}

func (c channelState) LastVoucherResult() datatransfer.VoucherResult {
	return nil
}

func (c channelState) VoucherResults() []datatransfer.VoucherResult {
	return nil
}

func (c channelState) SelfPeer() peer.ID {
	return c.selfPeer
}

func (c channelState) OtherPeer() peer.ID {
	if c.sender == c.selfPeer {
		return c.recipient
	}
	return c.sender
}

func (c channelState) Stages() *datatransfer.ChannelStages {
	return nil
}

var _ datatransfer.ChannelState = channelState{}
