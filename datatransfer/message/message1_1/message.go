package message1_1

import (
	"bytes"
	"io"

	datatransfer2 "github.com/filecoin-project/boost/datatransfer"
	"github.com/filecoin-project/boost/datatransfer/encoding"
	"github.com/filecoin-project/boost/datatransfer/message/types"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	cborgen "github.com/whyrusleeping/cbor-gen"
	"golang.org/x/xerrors"
)

// NewRequest generates a new request for the data transfer protocol
func NewRequest(id datatransfer2.TransferID, isRestart bool, isPull bool, vtype datatransfer2.TypeIdentifier, voucher encoding.Encodable, baseCid cid.Cid, selector ipld.Node) (datatransfer2.Request, error) {
	vbytes, err := encoding.Encode(voucher)
	if err != nil {
		return nil, xerrors.Errorf("Creating request: %w", err)
	}
	if baseCid == cid.Undef {
		return nil, xerrors.Errorf("base CID must be defined")
	}
	selBytes, err := encoding.Encode(selector)
	if err != nil {
		return nil, xerrors.Errorf("Error encoding selector")
	}

	var typ uint64
	if isRestart {
		typ = uint64(types.RestartMessage)
	} else {
		typ = uint64(types.NewMessage)
	}

	return &TransferRequest1_1{
		Type:   typ,
		Pull:   isPull,
		Vouch:  &cborgen.Deferred{Raw: vbytes},
		Stor:   &cborgen.Deferred{Raw: selBytes},
		BCid:   &baseCid,
		VTyp:   vtype,
		XferID: uint64(id),
	}, nil
}

// RestartExistingChannelRequest creates a request to ask the other side to restart an existing channel
func RestartExistingChannelRequest(channelId datatransfer2.ChannelID) datatransfer2.Request {

	return &TransferRequest1_1{Type: uint64(types.RestartExistingChannelRequestMessage),
		RestartChannel: channelId}
}

// CancelRequest request generates a request to cancel an in progress request
func CancelRequest(id datatransfer2.TransferID) datatransfer2.Request {
	return &TransferRequest1_1{
		Type:   uint64(types.CancelMessage),
		XferID: uint64(id),
	}
}

// UpdateRequest generates a new request update
func UpdateRequest(id datatransfer2.TransferID, isPaused bool) datatransfer2.Request {
	return &TransferRequest1_1{
		Type:   uint64(types.UpdateMessage),
		Paus:   isPaused,
		XferID: uint64(id),
	}
}

// VoucherRequest generates a new request for the data transfer protocol
func VoucherRequest(id datatransfer2.TransferID, vtype datatransfer2.TypeIdentifier, voucher encoding.Encodable) (datatransfer2.Request, error) {
	vbytes, err := encoding.Encode(voucher)
	if err != nil {
		return nil, xerrors.Errorf("Creating request: %w", err)
	}
	return &TransferRequest1_1{
		Type:   uint64(types.VoucherMessage),
		Vouch:  &cborgen.Deferred{Raw: vbytes},
		VTyp:   vtype,
		XferID: uint64(id),
	}, nil
}

// RestartResponse builds a new Data Transfer response
func RestartResponse(id datatransfer2.TransferID, accepted bool, isPaused bool, voucherResultType datatransfer2.TypeIdentifier, voucherResult encoding.Encodable) (datatransfer2.Response, error) {
	vbytes, err := encoding.Encode(voucherResult)
	if err != nil {
		return nil, xerrors.Errorf("Creating request: %w", err)
	}
	return &TransferResponse1_1{
		Acpt:   accepted,
		Type:   uint64(types.RestartMessage),
		Paus:   isPaused,
		XferID: uint64(id),
		VTyp:   voucherResultType,
		VRes:   &cborgen.Deferred{Raw: vbytes},
	}, nil
}

// NewResponse builds a new Data Transfer response
func NewResponse(id datatransfer2.TransferID, accepted bool, isPaused bool, voucherResultType datatransfer2.TypeIdentifier, voucherResult encoding.Encodable) (datatransfer2.Response, error) {
	vbytes, err := encoding.Encode(voucherResult)
	if err != nil {
		return nil, xerrors.Errorf("Creating request: %w", err)
	}
	return &TransferResponse1_1{
		Acpt:   accepted,
		Type:   uint64(types.NewMessage),
		Paus:   isPaused,
		XferID: uint64(id),
		VTyp:   voucherResultType,
		VRes:   &cborgen.Deferred{Raw: vbytes},
	}, nil
}

// VoucherResultResponse builds a new response for a voucher result
func VoucherResultResponse(id datatransfer2.TransferID, accepted bool, isPaused bool, voucherResultType datatransfer2.TypeIdentifier, voucherResult encoding.Encodable) (datatransfer2.Response, error) {
	vbytes, err := encoding.Encode(voucherResult)
	if err != nil {
		return nil, xerrors.Errorf("Creating request: %w", err)
	}
	return &TransferResponse1_1{
		Acpt:   accepted,
		Type:   uint64(types.VoucherResultMessage),
		Paus:   isPaused,
		XferID: uint64(id),
		VTyp:   voucherResultType,
		VRes:   &cborgen.Deferred{Raw: vbytes},
	}, nil
}

// UpdateResponse returns a new update response
func UpdateResponse(id datatransfer2.TransferID, isPaused bool) datatransfer2.Response {
	return &TransferResponse1_1{
		Type:   uint64(types.UpdateMessage),
		Paus:   isPaused,
		XferID: uint64(id),
	}
}

// CancelResponse makes a new cancel response message
func CancelResponse(id datatransfer2.TransferID) datatransfer2.Response {
	return &TransferResponse1_1{
		Type:   uint64(types.CancelMessage),
		XferID: uint64(id),
	}
}

// CompleteResponse returns a new complete response message
func CompleteResponse(id datatransfer2.TransferID, isAccepted bool, isPaused bool, voucherResultType datatransfer2.TypeIdentifier, voucherResult encoding.Encodable) (datatransfer2.Response, error) {
	vbytes, err := encoding.Encode(voucherResult)
	if err != nil {
		return nil, xerrors.Errorf("Creating request: %w", err)
	}
	return &TransferResponse1_1{
		Type:   uint64(types.CompleteMessage),
		Acpt:   isAccepted,
		Paus:   isPaused,
		VTyp:   voucherResultType,
		VRes:   &cborgen.Deferred{Raw: vbytes},
		XferID: uint64(id),
	}, nil
}

// FromNet can read a network stream to deserialize a GraphSyncMessage
func FromNet(r io.Reader) (datatransfer2.Message, error) {
	tresp := TransferMessage1_1{}
	err := tresp.UnmarshalCBOR(r)
	if err != nil {
		return nil, err
	}

	if (tresp.IsRequest() && tresp.Request == nil) || (!tresp.IsRequest() && tresp.Response == nil) {
		return nil, xerrors.Errorf("invalid/malformed message")
	}

	if tresp.IsRequest() {
		return tresp.Request, nil
	}
	return tresp.Response, nil
}

// FromNet can read a network stream to deserialize a GraphSyncMessage
func FromIPLD(nd datamodel.Node) (datatransfer2.Message, error) {
	buf := new(bytes.Buffer)
	err := dagcbor.Encode(nd, buf)
	if err != nil {
		return nil, err
	}
	return FromNet(buf)
}
