package loadbalancer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"testing"
	"time"

	"github.com/filecoin-project/boost/loadbalancer/messages"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/multiformats/go-multiaddr"
	"github.com/raulk/clock"
	"github.com/stretchr/testify/require"
)

func TestRoutingRequest(t *testing.T) {
	testProtocols := []protocol.ID{"applesauce/mcgee", "test/me"}
	startTime := time.Date(2001, 12, 1, 0, 0, 0, 0, time.UTC)
	expiry := time.Unix(0, startTime.Add(DefaultDuration).UnixNano())
	expiryAfterHalfDurationPassed := time.Unix(0, startTime.Add(DefaultDuration*3/2).UnixNano())
	expiryAfterTwiceDurationPassed := time.Unix(0, startTime.Add(DefaultDuration*3).UnixNano())
	ctx := context.Background()
	peers := makePeers(t)
	testCases := []struct {
		name             string
		newRouteFilter   func(p peer.ID, protocols []protocol.ID) error
		pre              func(t *testing.T, tn *testNet, testClock *clock.Mock)
		write            func(io.Writer) error
		expectedResponse *messages.RoutingResponse
	}{
		{
			name:  "new - simple success",
			write: func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:   messages.ResponseOk,
				Expiry: &expiry,
			},
		},
		{
			name: "new - filter errors",
			newRouteFilter: func(p peer.ID, protocols []protocol.ID) error {
				return errors.New("unauthorized")
			},
			write: func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:    messages.ResponseRejected,
				Message: "unauthorized",
			},
		},
		{
			name: "new - already registered",
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock) {
				tn.claimRoutesForOtherNode(testProtocols)
			},
			write: func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrAlreadyRegistered{testProtocols[0]}.Error(),
			},
		},
		{
			name: "new - already registered but expired",
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock) {
				tn.claimRoutesForOtherNode(testProtocols)
				testClock.Add(DefaultDuration * 2)
			},
			write: func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:   messages.ResponseOk,
				Expiry: &expiryAfterTwiceDurationPassed,
			},
		},
		{
			name: "new - registered then deleted",
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock) {
				tn.claimRoutesForOtherNode(testProtocols)
				tn.releaseRoutesForOtherNode(testProtocols)
			},
			write: func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:   messages.ResponseOk,
				Expiry: &expiry,
			},
		},
		{
			name: "extend - already registered",
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock) {
				response := tn.sendRoutingRequest(func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) })
				require.Equal(t, &messages.RoutingResponse{
					Code:   messages.ResponseOk,
					Expiry: &expiry,
				}, response)
				testClock.Add(DefaultDuration / 2)
			},
			write: func(w io.Writer) error { return messages.WriteExtendRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:   messages.ResponseOk,
				Expiry: &expiryAfterHalfDurationPassed,
			},
		},
		{
			name:  "extend - not registered",
			write: func(w io.Writer) error { return messages.WriteExtendRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNotRegistered{peers.serviceNode.id, testProtocols[0]}.Error(),
			},
		},
		{
			name: "extend - registered to other",
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock) {
				tn.claimRoutesForOtherNode(testProtocols)
			},
			write: func(w io.Writer) error { return messages.WriteExtendRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNotRegistered{peers.serviceNode.id, testProtocols[0]}.Error(),
			},
		},
		{
			name: "extend - registered but expired",
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock) {
				response := tn.sendRoutingRequest(func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) })
				require.Equal(t, &messages.RoutingResponse{
					Code:   messages.ResponseOk,
					Expiry: &expiry,
				}, response)
				testClock.Add(DefaultDuration * 2)
			},
			write: func(w io.Writer) error { return messages.WriteExtendRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNotRegistered{peers.serviceNode.id, testProtocols[0]}.Error(),
			},
		},
		{
			name: "extend - registered then deleted",
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock) {
				response := tn.sendRoutingRequest(func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) })
				require.Equal(t, &messages.RoutingResponse{
					Code:   messages.ResponseOk,
					Expiry: &expiry,
				}, response)
				response = tn.sendRoutingRequest(func(w io.Writer) error { return messages.WriteTerminateRoutingRequest(w, testProtocols) })
				require.Equal(t, &messages.RoutingResponse{
					Code: messages.ResponseOk,
				}, response)
			},
			write: func(w io.Writer) error { return messages.WriteExtendRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNotRegistered{peers.serviceNode.id, testProtocols[0]}.Error(),
			},
		},
		{
			name: "terminate - already registered",
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock) {
				response := tn.sendRoutingRequest(func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) })
				require.Equal(t, &messages.RoutingResponse{
					Code:   messages.ResponseOk,
					Expiry: &expiry,
				}, response)
			},
			write: func(w io.Writer) error { return messages.WriteTerminateRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code: messages.ResponseOk,
			},
		},
		{
			name:  "terminate - not registered",
			write: func(w io.Writer) error { return messages.WriteTerminateRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNotRegistered{peers.serviceNode.id, testProtocols[0]}.Error(),
			},
		},
		{
			name: "terminate - registered to other",
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock) {
				tn.claimRoutesForOtherNode(testProtocols)
			},
			write: func(w io.Writer) error { return messages.WriteTerminateRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNotRegistered{peers.serviceNode.id, testProtocols[0]}.Error(),
			},
		},
		{
			name: "terminate - registered but expired",
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock) {
				response := tn.sendRoutingRequest(func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) })
				require.Equal(t, &messages.RoutingResponse{
					Code:   messages.ResponseOk,
					Expiry: &expiry,
				}, response)
				testClock.Add(DefaultDuration * 2)
			},
			write: func(w io.Writer) error { return messages.WriteTerminateRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNotRegistered{peers.serviceNode.id, testProtocols[0]}.Error(),
			},
		},
		{
			name: "terminate - registered then deleted",
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock) {
				response := tn.sendRoutingRequest(func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) })
				require.Equal(t, &messages.RoutingResponse{
					Code:   messages.ResponseOk,
					Expiry: &expiry,
				}, response)
				response = tn.sendRoutingRequest(func(w io.Writer) error { return messages.WriteTerminateRoutingRequest(w, testProtocols) })
				require.Equal(t, &messages.RoutingResponse{
					Code: messages.ResponseOk,
				}, response)
			},
			write: func(w io.Writer) error { return messages.WriteTerminateRoutingRequest(w, testProtocols) },
			expectedResponse: &messages.RoutingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNotRegistered{peers.serviceNode.id, testProtocols[0]}.Error(),
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			tn := setupTestNet(ctx, t, peers)
			testClock := clock.NewMock()
			testClock.Set(startTime)
			lb := newLoadBalancer(ctx, tn.loadBalancer, testCase.newRouteFilter, testClock)
			lb.Start()
			if testCase.pre != nil {
				testCase.pre(t, tn, testClock)
			}
			response := tn.sendRoutingRequest(testCase.write)
			require.Equal(t, testCase.expectedResponse, response)
			err := lb.Close()
			require.NoError(t, err)
		})
	}
}

func TestOutboundForwarding(t *testing.T) {
	testProtocols := []protocol.ID{"applesauce/mcgee", "test/me"}
	otherProtocol := protocol.ID("noprotocol/here")
	startTime := time.Date(2001, 12, 1, 0, 0, 0, 0, time.UTC)
	expiry := time.Unix(0, startTime.Add(DefaultDuration).UnixNano())
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
				Code:         messages.ResponseOk,
				ProtocolID:   &testProtocols[1],
				RemotePubKey: &peers.publicNode.publicKey,
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
				return messages.WriteInboundForwardingRequest(w, peers.publicNode.id, peers.publicNode.publicKey, testProtocols[0])
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
				Message: "remote peer: protocol not supported",
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
			lb := newLoadBalancer(ctx, tn.loadBalancer, nil, testClock)
			lb.Start()
			// register routes
			routingResponse := tn.sendRoutingRequest(func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) })
			require.Equal(t, &messages.RoutingResponse{
				Code:   messages.ResponseOk,
				Expiry: &expiry,
			}, routingResponse)

			if testCase.registerHandler {
				handler := func(s network.Stream) {
					request, err := io.ReadAll(s)
					require.NoError(t, err)
					require.Equal(t, "request", string(request))
					_, err = s.Write([]byte("response"))
					require.NoError(t, err)
					s.Close()
				}
				tn.publicNode.SetStreamHandler(testProtocols[1], handler)
				tn.publicNode.SetStreamHandler(otherProtocol, handler)
			}
			response, s := tn.openOutboundForwardingRequest(testCase.write)
			require.Equal(t, testCase.expectedResponse, response)
			if response.Code == messages.ResponseOk {
				_, err := s.Write([]byte("request"))
				require.NoError(t, err)
				s.CloseWrite()
				streamResponse, err := io.ReadAll(s)
				require.NoError(t, err)
				require.Equal(t, "response", string(streamResponse))
				s.Close()
			}
			err := lb.Close()
			require.NoError(t, err)
		})
	}
}

func TestInboundForwarding(t *testing.T) {
	testProtocols := []protocol.ID{"applesauce/mcgee", "test/me"}
	otherProtocol := protocol.ID("noprotocol/here")
	startTime := time.Date(2001, 12, 1, 0, 0, 0, 0, time.UTC)
	expiry := time.Unix(0, startTime.Add(DefaultDuration).UnixNano())
	ctx := context.Background()
	peers := makePeers(t)
	testCases := []struct {
		name             string
		protocols        []protocol.ID
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
			lb := newLoadBalancer(ctx, tn.loadBalancer, nil, testClock)
			lb.Start()
			// register routes
			routingResponse := tn.sendRoutingRequest(func(w io.Writer) error { return messages.WriteNewRoutingRequest(w, testProtocols) })
			require.Equal(t, &messages.RoutingResponse{
				Code:   messages.ResponseOk,
				Expiry: &expiry,
			}, routingResponse)

			if testCase.registerHandler {
				handler := func(s network.Stream) {
					defer s.Close()
					request, err := messages.ReadForwardingRequest(s)
					require.Equal(t, &messages.ForwardingRequest{
						Kind:         messages.ForwardingInbound,
						Remote:       peers.publicNode.id,
						RemotePubKey: &peers.publicNode.publicKey,
						Protocols:    []protocol.ID{testProtocols[0]},
					}, request)
					require.NoError(tn.t, err)
					if testCase.rejectResponse {
						err = messages.WriteForwardingResponseError(s, errors.New("something went wrong"))
					} else {
						err = messages.WriteInboundForwardingResponseSuccess(s)
						userRequest, err := ioutil.ReadAll(s)
						require.NoError(t, err)
						require.Equal(t, "request", string(userRequest))
						_, err = s.Write([]byte("response"))
						require.NoError(t, err)
					}
				}
				tn.serviceNode.SetStreamHandler(ForwardingProtocolID, handler)
			}
			s, err := tn.publicNode.NewStream(tn.ctx, tn.loadBalancer.ID(), testCase.protocols...)
			if testCase.willErrorOpening {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				_, err := s.Write([]byte("request"))

				require.NoError(t, err)
				s.CloseWrite()
				streamResponse, err := io.ReadAll(s)
				if testCase.willErrorReading {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, "response", string(streamResponse))
					s.Close()
				}
			}
			err = lb.Close()
			require.NoError(t, err)
		})
	}
}

type forwardingRequestHandler func(*messages.ForwardingRequest, io.Writer) error
type testNet struct {
	ctx              context.Context
	t                *testing.T
	loadBalancer     host.Host
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
	loadBalancer     peerInfo
	serviceNode      peerInfo
	otherServiceNode peerInfo
	publicNode       peerInfo
}

func setupTestNet(ctx context.Context, t *testing.T, pis peerInfos) *testNet {
	mn := mocknet.New()
	lb, err := mn.AddPeer(pis.loadBalancer.key, pis.loadBalancer.multiAddr)
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
		loadBalancer:     lb,
		serviceNode:      sn,
		otherServiceNode: osn,
		publicNode:       pn,
	}
	return tn
}

func (tn *testNet) sendRoutingRequest(write func(io.Writer) error) *messages.RoutingResponse {
	s, err := tn.serviceNode.NewStream(tn.ctx, tn.loadBalancer.ID(), RegisterRoutingProtocolID)
	require.NoError(tn.t, err)
	err = write(s)
	require.NoError(tn.t, err)
	err = s.CloseWrite()
	require.NoError(tn.t, err)
	response, err := messages.ReadRoutingResponse(s)
	require.NoError(tn.t, err)
	return response
}

func (tn *testNet) claimRoutesForOtherNode(protocols []protocol.ID) {
	s, err := tn.otherServiceNode.NewStream(tn.ctx, tn.loadBalancer.ID(), RegisterRoutingProtocolID)
	require.NoError(tn.t, err)
	err = messages.WriteNewRoutingRequest(s, protocols)
	require.NoError(tn.t, err)
	err = s.CloseWrite()
	require.NoError(tn.t, err)
	response, err := messages.ReadRoutingResponse(s)
	require.NoError(tn.t, err)
	require.Equal(tn.t, messages.ResponseOk, response.Code)
}

func (tn *testNet) releaseRoutesForOtherNode(protocols []protocol.ID) {
	s, err := tn.otherServiceNode.NewStream(tn.ctx, tn.loadBalancer.ID(), RegisterRoutingProtocolID)
	require.NoError(tn.t, err)
	err = messages.WriteTerminateRoutingRequest(s, protocols)
	require.NoError(tn.t, err)
	s.CloseWrite()
	response, err := messages.ReadRoutingResponse(s)
	require.NoError(tn.t, err)
	require.Equal(tn.t, messages.ResponseOk, response.Code)
}

func (tn *testNet) openOutboundForwardingRequest(write func(io.Writer) error) (*messages.ForwardingResponse, network.Stream) {
	s, err := tn.serviceNode.NewStream(tn.ctx, tn.loadBalancer.ID(), ForwardingProtocolID)
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
		loadBalancer:     makePeer(t),
		serviceNode:      makePeer(t),
		otherServiceNode: makePeer(t),
		publicNode:       makePeer(t),
	}
}
