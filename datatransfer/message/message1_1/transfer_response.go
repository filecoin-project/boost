package message1_1

import (
	"bytes"
	"io"

	datatransfer2 "github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/encoding"
	"github.com/filecoin-project/boost/datatransfer/message/types"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/libp2p/go-libp2p/core/protocol"
	cbg "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

//go:generate cbor-gen-for --map-encoding TransferResponse1_1

// TransferResponse1_1 is a private struct that satisfies the datatransfer.Response interface
// It is the response message for the Data Transfer 1.1 and 1.2 Protocol.
type TransferResponse1_1 struct {
	Type   uint64
	Acpt   bool
	Paus   bool
	XferID uint64
	VRes   *cbg.Deferred
	VTyp   datatransfer2.TypeIdentifier
}

func (trsp *TransferResponse1_1) TransferID() datatransfer2.TransferID {
	return datatransfer2.TransferID(trsp.XferID)
}

// IsRequest always returns false in this case because this is a transfer response
func (trsp *TransferResponse1_1) IsRequest() bool {
	return false
}

// IsNew returns true if this is the first response sent
func (trsp *TransferResponse1_1) IsNew() bool {
	return trsp.Type == uint64(types.NewMessage)
}

// IsUpdate returns true if this response is an update
func (trsp *TransferResponse1_1) IsUpdate() bool {
	return trsp.Type == uint64(types.UpdateMessage)
}

// IsPaused returns true if the responder is paused
func (trsp *TransferResponse1_1) IsPaused() bool {
	return trsp.Paus
}

// IsCancel returns true if the responder has cancelled this response
func (trsp *TransferResponse1_1) IsCancel() bool {
	return trsp.Type == uint64(types.CancelMessage)
}

// IsComplete returns true if the responder has completed this response
func (trsp *TransferResponse1_1) IsComplete() bool {
	return trsp.Type == uint64(types.CompleteMessage)
}

func (trsp *TransferResponse1_1) IsVoucherResult() bool {
	return trsp.Type == uint64(types.VoucherResultMessage) || trsp.Type == uint64(types.NewMessage) || trsp.Type == uint64(types.CompleteMessage) ||
		trsp.Type == uint64(types.RestartMessage)
}

// Accepted returns true if the request is accepted in the response
func (trsp *TransferResponse1_1) Accepted() bool {
	return trsp.Acpt
}

func (trsp *TransferResponse1_1) VoucherResultType() datatransfer2.TypeIdentifier {
	return trsp.VTyp
}

func (trsp *TransferResponse1_1) VoucherResult(decoder encoding.Decoder) (encoding.Encodable, error) {
	if trsp.VRes == nil {
		return nil, xerrors.New("No voucher present to read")
	}
	return decoder.DecodeFromCbor(trsp.VRes.Raw)
}

func (trq *TransferResponse1_1) IsRestart() bool {
	return trq.Type == uint64(types.RestartMessage)
}

func (trsp *TransferResponse1_1) EmptyVoucherResult() bool {
	return trsp.VTyp == datatransfer2.EmptyTypeIdentifier
}

func (trsp *TransferResponse1_1) MessageForProtocol(targetProtocol protocol.ID) (datatransfer2.Message, error) {
	switch targetProtocol {
	case datatransfer2.ProtocolDataTransfer1_2:
		return trsp, nil
	default:
		return nil, xerrors.Errorf("protocol %s not supported", targetProtocol)
	}
}

func (trsp *TransferResponse1_1) ToIPLD() (datamodel.Node, error) {
	buf := new(bytes.Buffer)
	err := trsp.ToNet(buf)
	if err != nil {
		return nil, err
	}
	nb := basicnode.Prototype.Any.NewBuilder()
	err = dagcbor.Decode(nb, buf)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

// ToNet serializes a transfer response. It's a wrapper for MarshalCBOR to provide
// symmetry with FromNet
func (trsp *TransferResponse1_1) ToNet(w io.Writer) error {
	msg := TransferMessage1_1{
		IsRq:     false,
		Request:  nil,
		Response: trsp,
	}
	return msg.MarshalCBOR(w)
}
