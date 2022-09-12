package messages_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
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

func TestReadWriteFunctions(t *testing.T) {
	singleTestProtocol := protocol.ID("test/me")
	testProtocols := []protocol.ID{"applesauce/cheese", "big/face"}
	expiry := time.Now().Add(time.Hour)
	expiry = time.Unix(0, expiry.UnixNano())
	responseErr := errors.New("something went wrong")
	remote := peer.ID("something")
	_, pubKey, err := crypto.GenerateECDSAKeyPair(rand.Reader)
	require.NoError(t, err)

	var testCases = []struct {
		name          string
		write         func(io.Writer) error
		read          func(io.Reader) (interface{}, error)
		expectedValue interface{}
	}{
		{
			name: "new RoutingRequest",
			write: func(w io.Writer) error {
				return messages.WriteRoutingRequest(w, messages.RoutingKindNew, testProtocols)
			},
			read: func(r io.Reader) (interface{}, error) { return messages.ReadRoutingRequest(r) },
			expectedValue: &messages.RoutingRequest{
				Kind:      messages.RoutingKindNew,
				Protocols: testProtocols,
			},
		},
		{
			name:  "success RoutingResponse",
			write: func(w io.Writer) error { return messages.WriteRoutingResponseSuccess(w, &expiry) },
			read:  func(r io.Reader) (interface{}, error) { return messages.ReadRoutingResponse(r) },
			expectedValue: &messages.RoutingResponse{
				Code:   messages.ResponseOk,
				Expiry: &expiry,
			},
		},
		{
			name:  "error RoutingResponse",
			write: func(w io.Writer) error { return messages.WriteRoutingResponseError(w, responseErr) },
			read:  func(r io.Reader) (interface{}, error) { return messages.ReadRoutingResponse(r) },
			expectedValue: &messages.RoutingResponse{
				Code:    messages.ResponseRejected,
				Message: "something went wrong",
				Expiry:  nil,
			},
		},
		{
			name:  "outbound ForwardingRequest",
			write: func(w io.Writer) error { return messages.WriteOutboundForwardingRequest(w, remote, testProtocols) },
			read:  func(r io.Reader) (interface{}, error) { return messages.ReadForwardingRequest(r) },
			expectedValue: &messages.ForwardingRequest{
				Kind:      messages.ForwardingOutbound,
				Remote:    remote,
				Protocols: testProtocols,
			},
		},
		{
			name: "inbound ForwardingRequest",
			write: func(w io.Writer) error {
				return messages.WriteInboundForwardingRequest(w, remote, pubKey, singleTestProtocol)
			},
			read: func(r io.Reader) (interface{}, error) { return messages.ReadForwardingRequest(r) },
			expectedValue: &messages.ForwardingRequest{
				Kind:         messages.ForwardingInbound,
				Remote:       remote,
				RemotePubKey: &pubKey,
				Protocols:    []protocol.ID{singleTestProtocol},
			},
		},
		{
			name: "outbound success ForwardingResponse",
			write: func(w io.Writer) error {
				return messages.WriteOutboundForwardingResponseSuccess(w, pubKey, singleTestProtocol)
			},
			read: func(r io.Reader) (interface{}, error) { return messages.ReadForwardingResponse(r) },
			expectedValue: &messages.ForwardingResponse{
				Code:         messages.ResponseOk,
				RemotePubKey: &pubKey,
				ProtocolID:   &singleTestProtocol,
			},
		},
		{
			name: "inbound success ForwardingResponse",
			write: func(w io.Writer) error {
				return messages.WriteInboundForwardingResponseSuccess(w)
			},
			read: func(r io.Reader) (interface{}, error) { return messages.ReadForwardingResponse(r) },
			expectedValue: &messages.ForwardingResponse{
				Code: messages.ResponseOk,
			},
		},
		{
			name: "error ForwardingResponse",
			write: func(w io.Writer) error {
				return messages.WriteForwardingResponseError(w, responseErr)
			},
			read: func(r io.Reader) (interface{}, error) { return messages.ReadForwardingResponse(r) },
			expectedValue: &messages.ForwardingResponse{
				Code:    messages.ResponseRejected,
				Message: responseErr.Error(),
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := testCase.write(buf)
			require.NoError(t, err)
			resultValue, err := testCase.read(buf)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedValue, resultValue)
		})
	}
}
