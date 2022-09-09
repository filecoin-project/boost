package messages_test

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	"github.com/filecoin-project/boost/loadbalancer/messages"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/libp2p/go-libp2p-core/crypto"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/stretchr/testify/require"
)

func TestRoundtripRoutingRequest(t *testing.T) {
	buf := new(bytes.Buffer)
	expectedMessage := &messages.RoutingRequest{
		Kind:      messages.RoutingKindNew,
		Protocols: []protocol.ID{"applesauce/cheese", "house/door"},
	}
	err := messages.BindnodeRegistry.TypeToWriter(expectedMessage, buf, dagcbor.Encode)
	require.NoError(t, err)
	outMessage, err := messages.BindnodeRegistry.TypeFromReader(buf, (*messages.RoutingRequest)(nil), dagcbor.Decode)
	require.NoError(t, err)
	require.Equal(t, expectedMessage, outMessage)
}
func TestRoundtripRoutingResponse(t *testing.T) {
	buf := new(bytes.Buffer)
	expiry := time.Now().Add(time.Hour)
	expectedMessage := &messages.RoutingResponse{
		Code:    messages.ResponseRejected,
		Message: "",
		Expiry:  &expiry,
	}
	err := messages.BindnodeRegistry.TypeToWriter(expectedMessage, buf, dagcbor.Encode)
	require.NoError(t, err)
	outMessage, err := messages.BindnodeRegistry.TypeFromReader(buf, (*messages.RoutingResponse)(nil), dagcbor.Decode)
	require.NoError(t, err)
	require.Equal(t, expectedMessage.Code, outMessage.(*messages.RoutingResponse).Code)
	require.Equal(t, expectedMessage.Message, outMessage.(*messages.RoutingResponse).Message)
	require.Equal(t, expectedMessage.Expiry.UnixNano(), outMessage.(*messages.RoutingResponse).Expiry.UnixNano())
}

func TestRoundtripForwardingRequest(t *testing.T) {
	buf := new(bytes.Buffer)
	_, pub, err := crypto.GenerateECDSAKeyPair(rand.Reader)
	require.NoError(t, err)
	expectedMessage := &messages.ForwardingRequest{
		Kind:         messages.ForwardingInbound,
		Remote:       peer.ID("apples"),
		RemotePubKey: &pub,
		Protocols:    []protocol.ID{"applesauce/cheese", "house/door"},
	}
	err = messages.BindnodeRegistry.TypeToWriter(expectedMessage, buf, dagcbor.Encode)
	require.NoError(t, err)
	outMessage, err := messages.BindnodeRegistry.TypeFromReader(buf, (*messages.ForwardingRequest)(nil), dagcbor.Decode)
	require.NoError(t, err)
	require.Equal(t, expectedMessage, outMessage)
}

func TestRoundtripForwardingResponse(t *testing.T) {
	buf := new(bytes.Buffer)
	_, pub, err := crypto.GenerateECDSAKeyPair(rand.Reader)
	protocol := protocol.ID("applesauce/cheese")
	require.NoError(t, err)
	expectedMessage := &messages.ForwardingResponse{
		Code:         messages.ResponseOk,
		Message:      "",
		RemotePubKey: &pub,
		ProtocolID:   &protocol,
	}
	err = messages.BindnodeRegistry.TypeToWriter(expectedMessage, buf, dagcbor.Encode)
	require.NoError(t, err)
	outMessage, err := messages.BindnodeRegistry.TypeFromReader(buf, (*messages.ForwardingResponse)(nil), dagcbor.Decode)
	require.NoError(t, err)
	require.Equal(t, expectedMessage, outMessage)
}
