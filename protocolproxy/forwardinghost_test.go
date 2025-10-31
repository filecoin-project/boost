package protocolproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/filecoin-project/boost/protocolproxy/messages"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestSetStreamHandler(t *testing.T) {
	testProtocol := protocol.ID("applesauce/mcgee")
	otherProtocol := protocol.ID("cheeseface/mcgaw")
	ctx := context.Background()
	peers := makePeers(t)
	testCases := []struct {
		name        string
		protocol    protocol.ID
		expectedErr error
	}{
		{
			name:     "new - simple success",
			protocol: testProtocol,
		},
		{
			name:        "unregistered protocol",
			protocol:    otherProtocol,
			expectedErr: errors.New("stream reset"),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			tn := setupTestNet(ctx, t, peers)

			// setup a mock load balancer for routing
			fh := NewForwardingHost(tn.serviceNode, peer.AddrInfo{
				ID:    peers.proxyNode.id,
				Addrs: []multiaddr.Multiaddr{peers.proxyNode.multiAddr},
			})

			fh.SetStreamHandler(testProtocol, func(s network.Stream) {
				defer func() {
					_ = s.Close()
				}()
				require.Equal(t, s.Protocol(), testProtocol)
				require.Equal(t, s.Conn().RemotePeer(), peers.publicNode.id)
				data, err := io.ReadAll(s)
				require.NoError(t, err)
				require.Equal(t, []byte("request"), data)
				_, err = s.Write([]byte("response"))
				require.NoError(t, err)
			})
			s, err := tn.proxyNode.NewStream(ctx, tn.serviceNode.ID(), ForwardingProtocolID)
			require.NoError(t, err)
			defer func() {
				_ = s.Close()
			}()
			err = messages.WriteInboundForwardingRequest(s, tn.publicNode.ID(), testCase.protocol)
			require.NoError(t, err)
			_, err = s.Write([]byte("request"))
			require.NoError(t, err)
			err = s.CloseWrite()
			if testCase.expectedErr != nil {
				require.EqualError(t, err, testCase.expectedErr.Error())
			} else {
				require.NoError(t, err)
				data, err := io.ReadAll(s)
				require.NoError(t, err)
				require.Equal(t, []byte("response"), data)
			}

		})
	}
}
func TestNewStream(t *testing.T) {
	testProtocol := protocol.ID("applesauce/mcgee")
	ctx := context.Background()
	peers := makePeers(t)
	testCases := []struct {
		name                         string
		protocolProxyForwardingError error
		writeForwardingResponse      func(io.Writer) error
		expectedDataResponse         []byte
	}{
		{
			name: "outbound - simple success",
			writeForwardingResponse: func(w io.Writer) error {
				return messages.WriteOutboundForwardingResponseSuccess(w, peers.publicNode.publicKey, testProtocol)
			},
		},
		{
			name: "outbound - rejection",
			writeForwardingResponse: func(w io.Writer) error {
				return messages.WriteForwardingResponseError(w, errors.New("something went wrong"))
			},
			protocolProxyForwardingError: fmt.Errorf("opening forwarded stream: something went wrong"),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			tn := setupTestNet(ctx, t, peers)

			tn.proxyNode.SetStreamHandler(ForwardingProtocolID, func(s network.Stream) {
				defer func() {
					_ = s.Close()
				}()
				request, err := messages.ReadForwardingRequest(s)
				require.NoError(t, err)
				require.Equal(t, messages.ForwardingOutbound, request.Kind)
				require.Equal(t, []protocol.ID{testProtocol}, request.Protocols)
				require.Equal(t, peers.publicNode.id, request.Remote)
				if testCase.writeForwardingResponse != nil {
					err = testCase.writeForwardingResponse(s)
					require.NoError(t, err)
				}
				if testCase.protocolProxyForwardingError == nil {
					data, err := io.ReadAll(s)
					require.NoError(t, err)
					require.Equal(t, []byte("request"), data)
					_, err = s.Write([]byte("response"))
					require.NoError(t, err)
				}
			})
			fh := NewForwardingHost(tn.serviceNode, peer.AddrInfo{
				ID:    peers.proxyNode.id,
				Addrs: []multiaddr.Multiaddr{peers.proxyNode.multiAddr},
			})
			s, err := fh.NewStream(ctx, tn.publicNode.ID(), testProtocol)
			if testCase.protocolProxyForwardingError != nil {
				require.EqualError(t, err, testCase.protocolProxyForwardingError.Error())
			} else {
				require.NoError(t, err)
				defer func() {
					_ = s.Close()
				}()
				_, err = s.Write([]byte("request"))
				require.NoError(t, err)
				err = s.CloseWrite()
				require.NoError(t, err)
				data, err := io.ReadAll(s)
				require.NoError(t, err)
				require.Equal(t, []byte("response"), data)
			}
		})
	}
}
