package protocolproxy

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/boost/protocolproxy/messages"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestOutboundForwarding(t *testing.T) {
	testProtocols := []protocol.ID{"applesauce/mcgee", "test/me"}
	otherProtocol := protocol.ID("noprotocol/here")
	startTime := time.Date(2001, 12, 1, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()
	peers := makePeers(t)
	testCases := []struct {
		name             string
		write            func(io.Writer) error
		expectedResponse *messages.ForwardingResponse
		registerHandler  bool
	}{
		{
			name: "simple success",
			write: func(w io.Writer) error {
				return messages.WriteOutboundForwardingRequest(w, peers.publicNode.id, testProtocols)
			},
			expectedResponse: &messages.ForwardingResponse{
				Code:       messages.ResponseOk,
				ProtocolID: &testProtocols[1],
			},
			registerHandler: true,
		},
		{
			name: "error - protocol not registered",
			write: func(w io.Writer) error {
				return messages.WriteOutboundForwardingRequest(w, peers.publicNode.id, []protocol.ID{otherProtocol})
			},
			expectedResponse: &messages.ForwardingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNotRegistered{peers.serviceNode.id, otherProtocol}.Error(),
			},
			registerHandler: true,
		},
		{
			name: "error - no inbound requests",
			write: func(w io.Writer) error {
				return messages.WriteInboundForwardingRequest(w, peers.publicNode.id, testProtocols[0])
			},
			expectedResponse: &messages.ForwardingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNoInboundRequests.Error(),
			},
			registerHandler: true,
		},
		{
			name: "error - public peer not listening on channel",
			write: func(w io.Writer) error {
				return messages.WriteOutboundForwardingRequest(w, peers.publicNode.id, testProtocols)
			},
			expectedResponse: &messages.ForwardingResponse{
				Code:    messages.ResponseRejected,
				Message: "remote peer: failed to negotiate protocol: protocols not supported",
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			//ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			//defer cancel()
			tn := setupTestNet(ctx, t, peers)
			testClock := clock.NewMock()
			testClock.Set(startTime)
			pp, err := NewProtocolProxy(tn.proxyNode, map[peer.ID][]protocol.ID{
				peers.serviceNode.id: testProtocols,
			})
			require.NoError(t, err)
			pp.Start(ctx)

			if testCase.registerHandler {
				handler := func(s network.Stream) {
					request, err := io.ReadAll(s)
					require.NoError(t, err)
					require.Equal(t, "request", string(request))
					_, err = s.Write([]byte("response"))
					require.NoError(t, err)
					_ = s.Close()
				}
				tn.publicNode.SetStreamHandler(testProtocols[1], handler)
				tn.publicNode.SetStreamHandler(otherProtocol, handler)
			}
			response, s := tn.openOutboundForwardingRequest(testCase.write)
			require.Equal(t, testCase.expectedResponse.Code, response.Code)
			require.Equal(t, testCase.expectedResponse.ProtocolID, response.ProtocolID)
			require.Contains(t, response.Message, testCase.expectedResponse.Message)
			if response.Code == messages.ResponseOk {
				_, err := s.Write([]byte("request"))
				require.NoError(t, err)
				err = s.CloseWrite()
				require.NoError(t, err)
				streamResponse, err := io.ReadAll(s)
				require.NoError(t, err)
				require.Equal(t, "response", string(streamResponse))
				_ = s.Close()
			}
			pp.Close()
		})
	}
}

func TestInboundForwarding(t *testing.T) {
	testProtocols := []protocol.ID{"applesauce/mcgee", "test/me"}
	otherProtocol := protocol.ID("noprotocol/here")
	startTime := time.Date(2001, 12, 1, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()
	peers := makePeers(t)
	testCases := []struct {
		name             string
		protocols        []protocol.ID
		doNotConnect     bool
		willErrorOpening bool
		willErrorReading bool
		registerHandler  bool
		rejectResponse   bool
	}{
		{
			name:             "simple success",
			protocols:        testProtocols,
			willErrorOpening: false,
			willErrorReading: false,
			registerHandler:  true,
		},
		{
			name:             "error - not connected to service node",
			protocols:        testProtocols,
			doNotConnect:     true,
			willErrorOpening: false,
			willErrorReading: true,
			registerHandler:  true,
		},
		{
			name:             "error - protocol not registered",
			protocols:        []protocol.ID{otherProtocol},
			willErrorOpening: true,
			willErrorReading: false,
			registerHandler:  true,
		},
		{
			name:             "error - connecting on forwarding protocol error",
			protocols:        testProtocols,
			willErrorOpening: false,
			willErrorReading: true,
			registerHandler:  false,
		},
		{
			name:             "error - response rejected",
			protocols:        testProtocols,
			willErrorOpening: false,
			willErrorReading: true,
			registerHandler:  true,
			rejectResponse:   true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			//ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			//defer cancel()
			tn := setupTestNet(ctx, t, peers)
			testClock := clock.NewMock()
			testClock.Set(startTime)
			pp, err := NewProtocolProxy(tn.proxyNode, map[peer.ID][]protocol.ID{
				peers.serviceNode.id: testProtocols,
			})
			require.NoError(t, err)
			pp.Start(ctx)
			if testCase.registerHandler {
				handler := func(s network.Stream) {
					defer func() {
						_ = s.Close()
					}()
					request, err := messages.ReadForwardingRequest(s)
					require.Equal(t, &messages.ForwardingRequest{
						Kind:      messages.ForwardingInbound,
						Remote:    peers.publicNode.id,
						Protocols: []protocol.ID{testProtocols[0]},
					}, request)
					require.NoError(tn.t, err)
					if testCase.rejectResponse {
						_ = s.Reset()
					} else {
						userRequest, err := io.ReadAll(s)
						require.NoError(t, err)
						require.Equal(t, "request", string(userRequest))
						_, err = s.Write([]byte("response"))
						require.NoError(t, err)
					}
				}
				tn.serviceNode.SetStreamHandler(ForwardingProtocolID, handler)
			}
			if !testCase.doNotConnect {
				err = tn.serviceNode.Connect(ctx, peer.AddrInfo{
					ID: peers.proxyNode.id,
					Addrs: []multiaddr.Multiaddr{
						peers.proxyNode.multiAddr,
					},
				})
				require.NoError(t, err)
			}
			s, err := tn.publicNode.NewStream(tn.ctx, tn.proxyNode.ID(), testCase.protocols...)
			if testCase.willErrorOpening {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				_, err := s.Write([]byte("request"))

				require.NoError(t, err)
				_ = s.CloseWrite()
				require.NoError(t, err)
				streamResponse, err := io.ReadAll(s)
				if testCase.willErrorReading {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, "response", string(streamResponse))
					_ = s.Close()
				}
			}
			pp.Close()
		})
	}
}

type testNet struct {
	ctx              context.Context
	t                *testing.T
	proxyNode        host.Host
	serviceNode      host.Host
	otherServiceNode host.Host
	publicNode       host.Host
}

type peerInfo struct {
	key       crypto.PrivKey
	publicKey crypto.PubKey
	id        peer.ID
	multiAddr multiaddr.Multiaddr
}

type peerInfos struct {
	proxyNode        peerInfo
	serviceNode      peerInfo
	otherServiceNode peerInfo
	publicNode       peerInfo
}

func setupTestNet(ctx context.Context, t *testing.T, pis peerInfos) *testNet {
	mn := mocknet.New()
	prn, err := mn.AddPeer(pis.proxyNode.key, pis.proxyNode.multiAddr)
	require.NoError(t, err)
	sn, err := mn.AddPeer(pis.serviceNode.key, pis.serviceNode.multiAddr)
	require.NoError(t, err)
	osn, err := mn.AddPeer(pis.otherServiceNode.key, pis.otherServiceNode.multiAddr)
	require.NoError(t, err)
	pn, err := mn.AddPeer(pis.publicNode.key, pis.publicNode.multiAddr)
	require.NoError(t, err)
	err = mn.LinkAll()
	require.NoError(t, err)
	tn := &testNet{
		t:                t,
		ctx:              ctx,
		proxyNode:        prn,
		serviceNode:      sn,
		otherServiceNode: osn,
		publicNode:       pn,
	}
	return tn
}

func (tn *testNet) openOutboundForwardingRequest(write func(io.Writer) error) (*messages.ForwardingResponse, network.Stream) {
	s, err := tn.serviceNode.NewStream(tn.ctx, tn.proxyNode.ID(), ForwardingProtocolID)
	require.NoError(tn.t, err)
	err = write(s)
	require.NoError(tn.t, err)
	response, err := messages.ReadForwardingResponse(s)
	require.NoError(tn.t, err)
	return response, s
}

var blackholeIP6 = net.ParseIP("100::")

func makePeer(t *testing.T) peerInfo {
	sk, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(sk)
	require.NoError(t, err)
	suffix := id
	if len(id) > 8 {
		suffix = id[len(id)-8:]
	}
	ip := append(net.IP{}, blackholeIP6...)
	copy(ip[net.IPv6len-len(suffix):], suffix)
	a, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip6/%s/tcp/4242", ip))
	require.NoError(t, err)
	return peerInfo{sk, sk.GetPublic(), id, a}
}

func makePeers(t *testing.T) peerInfos {
	return peerInfos{
		proxyNode:        makePeer(t),
		serviceNode:      makePeer(t),
		otherServiceNode: makePeer(t),
		publicNode:       makePeer(t),
	}
}
