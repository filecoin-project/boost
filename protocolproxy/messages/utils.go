package messages

import (
	"bytes"
	"io"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/libp2p/go-libp2p/core/crypto"
	peer "github.com/libp2p/go-libp2p/core/peer"
	protocol "github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
)

// ReadForwardingRequest reads a forwarding request from a network stream
func ReadForwardingRequest(s io.Reader) (*ForwardingRequest, error) {
	// read the message with a delimited length
	delimited := msgio.NewVarintReader(s)
	buf, err := delimited.ReadMsg()
	if err != nil {
		return nil, err
	}
	// now deserialize the buffer
	forwardingRequestI, err := BindnodeRegistry.TypeFromReader(bytes.NewReader(buf), (*ForwardingRequest)(nil), dagcbor.Decode)
	if err != nil {
		return nil, err
	}
	return forwardingRequestI.(*ForwardingRequest), nil
}

// WriteInboundForwardingRequest writes a new inbound forwarding request to a network strema
func WriteInboundForwardingRequest(s io.Writer, remote peer.ID, protocolID protocol.ID) error {
	request := &ForwardingRequest{
		Kind:      ForwardingInbound,
		Remote:    remote,
		Protocols: []protocol.ID{protocolID},
	}
	buf, err := BindnodeRegistry.TypeToBytes(request, dagcbor.Encode)
	if err != nil {
		return err
	}
	delimited := msgio.NewVarintWriter(s)
	return delimited.WriteMsg(buf)
}

// WriteOutboundForwardingRequest writes a new outbound forwarding request to a network strema
func WriteOutboundForwardingRequest(s io.Writer, remote peer.ID, protocols []protocol.ID) error {
	request := &ForwardingRequest{
		Kind:      ForwardingOutbound,
		Remote:    remote,
		Protocols: protocols,
	}
	buf, err := BindnodeRegistry.TypeToBytes(request, dagcbor.Encode)
	if err != nil {
		return err
	}
	delimited := msgio.NewVarintWriter(s)
	return delimited.WriteMsg(buf)
}

// ReadForwardingResponse reads a forwarding response from a network stream
func ReadForwardingResponse(s io.Reader) (*ForwardingResponse, error) {
	// read the message with a delimited length
	delimited := msgio.NewVarintReader(s)
	buf, err := delimited.ReadMsg()
	if err != nil {
		return nil, err
	}
	// now deserialize the buffer
	forwardingResponse, err := BindnodeRegistry.TypeFromReader(bytes.NewReader(buf), (*ForwardingResponse)(nil), dagcbor.Decode)
	if err != nil {
		return nil, err
	}
	return forwardingResponse.(*ForwardingResponse), nil
}

// WriteOutboundForwardingResponseSuccess writes a new outbound forwarding success response to a network strema
func WriteOutboundForwardingResponseSuccess(s io.Writer, remotePubKey crypto.PubKey, protocolID protocol.ID) error {
	response := &ForwardingResponse{
		Code:       ResponseOk,
		ProtocolID: &protocolID,
	}
	buf, err := BindnodeRegistry.TypeToBytes(response, dagcbor.Encode)
	if err != nil {
		return err
	}
	delimited := msgio.NewVarintWriter(s)
	return delimited.WriteMsg(buf)
}

// WriteForwardingResponseError sends a routing response with code Rejected and a message containing the error to
// a io.Writer
func WriteForwardingResponseError(s io.Writer, err error) error {
	response := &ForwardingResponse{
		Code:    ResponseRejected,
		Message: err.Error(),
	}
	buf, err := BindnodeRegistry.TypeToBytes(response, dagcbor.Encode)
	if err != nil {
		return err
	}
	delimited := msgio.NewVarintWriter(s)
	return delimited.WriteMsg(buf)
}
