package messages_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"testing"

	"github.com/filecoin-project/boost/protocolproxy/messages"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/libp2p/go-libp2p-core/crypto"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
	"github.com/stretchr/testify/require"
)

func TestRoundtripForwardingRequest(t *testing.T) {
	buf := new(bytes.Buffer)
	expectedMessage := &messages.ForwardingRequest{
		Kind:      messages.ForwardingInbound,
		Remote:    peer.ID("apples"),
		Protocols: []protocol.ID{"applesauce/cheese", "house/door"},
	}
	err := messages.BindnodeRegistry.TypeToWriter(expectedMessage, buf, dagcbor.Encode)
	require.NoError(t, err)
	outMessage, err := messages.BindnodeRegistry.TypeFromReader(buf, (*messages.ForwardingRequest)(nil), dagcbor.Decode)
	require.NoError(t, err)
	require.Equal(t, expectedMessage, outMessage)
}

func TestRoundtripForwardingResponse(t *testing.T) {
	buf := new(bytes.Buffer)
	protocol := protocol.ID("applesauce/cheese")
	expectedMessage := &messages.ForwardingResponse{
		Code:       messages.ResponseOk,
		Message:    "",
		ProtocolID: &protocol,
	}
	err := messages.BindnodeRegistry.TypeToWriter(expectedMessage, buf, dagcbor.Encode)
	require.NoError(t, err)
	outMessage, err := messages.BindnodeRegistry.TypeFromReader(buf, (*messages.ForwardingResponse)(nil), dagcbor.Decode)
	require.NoError(t, err)
	require.Equal(t, expectedMessage, outMessage)
}

func TestReadWriteFunctions(t *testing.T) {
	singleTestProtocol := protocol.ID("test/me")
	testProtocols := []protocol.ID{"applesauce/cheese", "big/face"}
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
				return messages.WriteInboundForwardingRequest(w, remote, singleTestProtocol)
			},
			read: func(r io.Reader) (interface{}, error) { return messages.ReadForwardingRequest(r) },
			expectedValue: &messages.ForwardingRequest{
				Kind:      messages.ForwardingInbound,
				Remote:    remote,
				Protocols: []protocol.ID{singleTestProtocol},
			},
		},
		{
			name: "outbound success ForwardingResponse",
			write: func(w io.Writer) error {
				return messages.WriteOutboundForwardingResponseSuccess(w, pubKey, singleTestProtocol)
			},
			read: func(r io.Reader) (interface{}, error) { return messages.ReadForwardingResponse(r) },
			expectedValue: &messages.ForwardingResponse{
				Code:       messages.ResponseOk,
				ProtocolID: &singleTestProtocol,
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
