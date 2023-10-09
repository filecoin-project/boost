package message1_1

import (
	"io"

	datatransfer2 "github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/encoding"
	"github.com/filecoin-project/boost/datatransfer/message/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/libp2p/go-libp2p/core/protocol"
	"golang.org/x/xerrors"
)

// TransferRequest1_1 is a struct for the 1.1 Data Transfer Protocol that fulfills the datatransfer.Request interface.
// its members are exported to be used by cbor-gen
type TransferRequest1_1 struct {
	BaseCidPtr            *cid.Cid
	MessageType           uint64
	Pause                 bool
	Partial               bool
	Pull                  bool
	SelectorPtr           *datamodel.Node
	VoucherPtr            *datamodel.Node
	VoucherTypeIdentifier datatransfer2.TypeIdentifier
	TransferId            uint64
	RestartChannel        datatransfer2.ChannelID
}

func (trq *TransferRequest1_1) MessageForProtocol(targetProtocol protocol.ID) (datatransfer2.Message, error) {
	switch targetProtocol {
	case datatransfer2.ProtocolDataTransfer1_2:
		return trq, nil
	default:
		return nil, xerrors.Errorf("protocol not supported")
	}
}

// IsRequest always returns true in this case because this is a transfer request
func (trq *TransferRequest1_1) IsRequest() bool {
	return true
}

func (trq *TransferRequest1_1) IsRestart() bool {
	return trq.MessageType == uint64(types.RestartMessage)
}

func (trq *TransferRequest1_1) IsRestartExistingChannelRequest() bool {
	return trq.MessageType == uint64(types.RestartExistingChannelRequestMessage)
}

func (trq *TransferRequest1_1) RestartChannelId() (datatransfer2.ChannelID, error) {
	if !trq.IsRestartExistingChannelRequest() {
		return datatransfer2.ChannelID{}, xerrors.New("not a restart request")
	}
	return trq.RestartChannel, nil
}

func (trq *TransferRequest1_1) IsNew() bool {
	return trq.MessageType == uint64(types.NewMessage)
}

func (trq *TransferRequest1_1) IsUpdate() bool {
	return trq.MessageType == uint64(types.UpdateMessage)
}

func (trq *TransferRequest1_1) IsVoucher() bool {
	return trq.MessageType == uint64(types.VoucherMessage) || trq.MessageType == uint64(types.NewMessage)
}

func (trq *TransferRequest1_1) IsPaused() bool {
	return trq.Pause
}

func (trq *TransferRequest1_1) TransferID() datatransfer2.TransferID {
	return datatransfer2.TransferID(trq.TransferId)
}

// ========= datatransfer.Request interface
// IsPull returns true if this is a data pull request
func (trq *TransferRequest1_1) IsPull() bool {
	return trq.Pull
}

// VoucherType returns the Voucher ID
func (trq *TransferRequest1_1) VoucherType() datatransfer2.TypeIdentifier {
	return trq.VoucherTypeIdentifier
}

// Voucher returns the Voucher bytes
func (trq *TransferRequest1_1) Voucher(decoder encoding.Decoder) (encoding.Encodable, error) {
	if trq.VoucherPtr == nil {
		return nil, xerrors.New("No voucher present to read")
	}
	return decoder.DecodeFromNode(*trq.VoucherPtr)
}

func (trq *TransferRequest1_1) EmptyVoucher() bool {
	return trq.VoucherTypeIdentifier == datatransfer2.EmptyTypeIdentifier
}

// BaseCid returns the Base CID
func (trq *TransferRequest1_1) BaseCid() cid.Cid {
	if trq.BaseCidPtr == nil {
		return cid.Undef
	}
	return *trq.BaseCidPtr
}

// Selector returns the message Selector bytes
func (trq *TransferRequest1_1) Selector() (datamodel.Node, error) {
	if trq.SelectorPtr == nil {
		return nil, xerrors.New("No selector present to read")
	}
	return *trq.SelectorPtr, nil
}

// IsCancel returns true if this is a cancel request
func (trq *TransferRequest1_1) IsCancel() bool {
	return trq.MessageType == uint64(types.CancelMessage)
}

// IsPartial returns true if this is a partial request
func (trq *TransferRequest1_1) IsPartial() bool {
	return trq.Partial
}

func (trsp *TransferRequest1_1) toIPLD() schema.TypedNode {
	msg := TransferMessage1_1{
		IsRequest: true,
		Request:   trsp,
		Response:  nil,
	}
	return msg.toIPLD()
}

func (trq *TransferRequest1_1) ToIPLD() (datamodel.Node, error) {
	return trq.toIPLD().Representation(), nil
}

// ToNet serializes a transfer request.
func (trq *TransferRequest1_1) ToNet(w io.Writer) error {
	return dagcbor.Encode(trq.toIPLD().Representation(), w)
}
