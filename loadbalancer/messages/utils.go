package messages

import (
	"bytes"
	"io"
	"time"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/libp2p/go-libp2p-core/crypto"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-msgio"
)

// ReadRoutingRequest reads a new routing request from a network stream
func ReadRoutingRequest(s io.Reader) (*RoutingRequest, error) {
	routingRequestI, err := BindnodeRegistry.TypeFromReader(s, (*RoutingRequest)(nil), dagcbor.Decode)
	if err != nil {
		return nil, err
	}
	return routingRequestI.(*RoutingRequest), nil
}

// WriteNewRoutingRequest sends a new routing request to a load balancer for the given protocols
func WriteNewRoutingRequest(s io.Writer, protocols []protocol.ID) error {
	request := &RoutingRequest{
		Kind:      RoutingKindNew,
		Protocols: protocols,
	}
	return BindnodeRegistry.TypeToWriter(request, s, dagcbor.Encode)
}

// WriteTerminateRoutingRequest sends a terminate routing request to a load balancer for the given protocols
func WriteTerminateRoutingRequest(s io.Writer, protocols []protocol.ID) error {
	request := &RoutingRequest{
		Kind:      RoutingKindTerminate,
		Protocols: protocols,
	}
	return BindnodeRegistry.TypeToWriter(request, s, dagcbor.Encode)
}

// WriteExtendRoutingRequest sends a extend routing request to a load balancer for the given protocols
func WriteExtendRoutingRequest(s io.Writer, protocols []protocol.ID) error {
	request := &RoutingRequest{
		Kind:      RoutingKindExtend,
		Protocols: protocols,
	}
	return BindnodeRegistry.TypeToWriter(request, s, dagcbor.Encode)
}

// ReadRoutingResponse reads a new routing response from a network stream
func ReadRoutingResponse(s io.Reader) (*RoutingResponse, error) {
	routingResponseI, err := BindnodeRegistry.TypeFromReader(s, (*RoutingResponse)(nil), dagcbor.Decode)
	if err != nil {
		return nil, err
	}
	return routingResponseI.(*RoutingResponse), nil
}

// WriteRoutingResponseError sends a routing response with code Rejected and a message containing the error to
// an io.Writer
func WriteRoutingResponseError(s io.Writer, err error) error {
	response := &RoutingResponse{
		Code:    ResponseRejected,
		Message: err.Error(),
	}
	return BindnodeRegistry.TypeToWriter(response, s, dagcbor.Encode)
}

// WriteRoutingResponseSuccess sends a routing response with code OK and the given expiry, if present
func WriteRoutingResponseSuccess(s io.Writer, expiry *time.Time) error {
	response := &RoutingResponse{
		Code:   ResponseOk,
		Expiry: expiry,
	}
	return BindnodeRegistry.TypeToWriter(response, s, dagcbor.Encode)
}

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
func WriteInboundForwardingRequest(s io.Writer, remote peer.ID, remotePubKey crypto.PubKey, protocolID protocol.ID) error {
	request := &ForwardingRequest{
		Kind:         ForwardingInbound,
		Remote:       remote,
		RemotePubKey: &remotePubKey,
		Protocols:    []protocol.ID{protocolID},
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
		Code:         ResponseOk,
		RemotePubKey: &remotePubKey,
		ProtocolID:   &protocolID,
	}
	buf, err := BindnodeRegistry.TypeToBytes(response, dagcbor.Encode)
	if err != nil {
		return err
	}
	delimited := msgio.NewVarintWriter(s)
	return delimited.WriteMsg(buf)
}

// WriteInboundForwardingResponseSuccess writes a new inbound forwarding success response to a network strema
func WriteInboundForwardingResponseSuccess(s io.Writer) error {
	response := &ForwardingResponse{
		Code: ResponseOk,
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
