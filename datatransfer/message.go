package datatransfer

import (
	"io"

	"github.com/filecoin-project/boost/datatransfer/encoding"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var (
	// ProtocolDataTransfer1_2 is the protocol identifier for the latest
	// version of data-transfer (supports do-not-send-first-blocks extension)
	ProtocolDataTransfer1_2 protocol.ID = "/fil/datatransfer/1.2.0"
)

// Message is a message for the data transfer protocol
// (either request or response) that can serialize to a protobuf
type Message interface {
	IsRequest() bool
	IsRestart() bool
	IsNew() bool
	IsUpdate() bool
	IsPaused() bool
	IsCancel() bool
	TransferID() TransferID
	ToNet(w io.Writer) error
	ToIPLD() (datamodel.Node, error)
	MessageForProtocol(targetProtocol protocol.ID) (newMsg Message, err error)
}

// Request is a response message for the data transfer protocol
type Request interface {
	Message
	IsPull() bool
	IsVoucher() bool
	VoucherType() TypeIdentifier
	Voucher(decoder encoding.Decoder) (encoding.Encodable, error)
	BaseCid() cid.Cid
	Selector() (ipld.Node, error)
	IsRestartExistingChannelRequest() bool
	RestartChannelId() (ChannelID, error)
}

// Response is a response message for the data transfer protocol
type Response interface {
	Message
	IsVoucherResult() bool
	IsComplete() bool
	Accepted() bool
	VoucherResultType() TypeIdentifier
	VoucherResult(decoder encoding.Decoder) (encoding.Encodable, error)
	EmptyVoucherResult() bool
}
