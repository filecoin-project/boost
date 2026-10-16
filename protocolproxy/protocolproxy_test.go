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
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/boost/protocolproxy/messages"
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

			// Connect the service node up front and wait for the proxy to
			// register its routes, so that the forwarding request below is
			// never sent before the route exists
			require.NoError(t, tn.serviceNode.Connect(ctx, peer.AddrInfo{
				ID: peers.proxyNode.id,
				Addrs: []multiaddr.Multiaddr{
					peers.proxyNode.multiAddr,
				},
			}))
			waitForRoutes(t, pp, testProtocols...)

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

				// the proxy learns the route from a notifiee fired on the
				// listener goroutine, so it may not be active yet
				waitForRoutes(t, pp, testProtocols...)
			}
			s, err := tn.publicNode.NewStream(tn.ctx, tn.proxyNode.ID(), testCase.protocols...)
			switch {
			case testCase.willErrorOpening:
				require.Error(t, err)
			case testCase.willErrorReading:
				// the proxy resets a stream it cannot route. The reset can
				// land before negotiation finishes, so the failure shows up
				// either on the open or somewhere in the round trip
				if err == nil {
					_, err = requestAndReadResponse(s)
				}
				require.Error(t, err)
			default:
				require.NoError(t, err)
				streamResponse, err := requestAndReadResponse(s)
				require.NoError(t, err)
				require.Equal(t, "response", streamResponse)
				_ = s.Close()
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

// requestAndReadResponse writes a request on the stream, half closes it and
// reads the response. It returns the first error from any of those steps: a
// stream the proxy resets fails wherever the reset happens to land.
func requestAndReadResponse(s network.Stream) (string, error) {
	if _, err := s.Write([]byte("request")); err != nil {
		return "", err
	}
	if err := s.CloseWrite(); err != nil {
		return "", err
	}
	response, err := io.ReadAll(s)
	return string(response), err
}

// waitForRoutes blocks until the proxy has registered the service node's
// routes. The proxy learns them from libp2p's connection notifiee, which the
// swarm fires from its listener goroutine, so a connection can be established
// (and a stream opened) before the route is active. A request that arrives in
// that window is rejected, or has its stream reset.
func waitForRoutes(t *testing.T, pp *ProtocolProxy, protocols ...protocol.ID) {
	t.Helper()
	require.Eventually(t, func() bool {
		pp.activeRoutesLk.RLock()
		defer pp.activeRoutesLk.RUnlock()
		for _, id := range protocols {
			if _, ok := pp.activeRoutes[id]; !ok {
				return false
			}
		}
		return true
	}, 10*time.Second, time.Millisecond, "proxy did not register routes %v", protocols)
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
