package loadbalancer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/boost/loadbalancer/messages"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

type routingRoundtrip struct {
	expectedRequest *messages.RoutingRequest
	writeResponse   func(io.Writer) error
}

func TestSetStreamHandler(t *testing.T) {
	testProtocol := protocol.ID("applesauce/mcgee")
	startTime := time.Date(2001, 12, 1, 0, 0, 0, 0, time.UTC)
	expiry := time.Unix(0, startTime.Add(DefaultDuration).UnixNano())
	expiryPlusHalfDurationPassed := time.Unix(0, startTime.Add(DefaultDuration*3/2).UnixNano())
	ctx := context.Background()
	peers := makePeers(t)
	testCases := []struct {
		name                           string
		pre                            func(t *testing.T, tn *testNet, testClock *clock.Mock, sn host.Host, blockingRequests <-chan struct{})
		post                           func(t *testing.T, tn *testNet, testClock *clock.Mock, sn host.Host, blockingRequests <-chan struct{})
		loadBalancerRouting            []routingRoundtrip
		loadBalancerForwardingResponse *messages.ForwardingResponse
		expectedDataResponse           []byte
	}{
		{
			name: "new - simple success",
			loadBalancerRouting: []routingRoundtrip{
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindNew,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, &expiry)
					},
				},
			},
			loadBalancerForwardingResponse: &messages.ForwardingResponse{
				Code: messages.ResponseOk,
			},
		},
		{
			name: "new - rejection",
			loadBalancerRouting: []routingRoundtrip{
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindNew,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteForwardingResponseError(w, errors.New("something went wrong"))
					},
				},
			},
			loadBalancerForwardingResponse: &messages.ForwardingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNotRegistered{peers.serviceNode.id, testProtocol}.Error(),
			},
		},
		{
			name: "new - already registered",
			loadBalancerRouting: []routingRoundtrip{
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindNew,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, &expiry)
					},
				},
			},
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock, sn host.Host, blockingRequests <-chan struct{}) {
				sn.SetStreamHandler(testProtocol, func(s network.Stream) {
					defer s.Close()
					data, err := io.ReadAll(s)
					require.NoError(t, err)
					require.Equal(t, []byte("request"), data)
					_, err = s.Write([]byte("other response"))
					require.NoError(t, err)
				})
			},
			loadBalancerForwardingResponse: &messages.ForwardingResponse{
				Code: messages.ResponseOk,
			},
			expectedDataResponse: []byte("other response"),
		},
		{
			name: "new - registration in progress",
			loadBalancerRouting: []routingRoundtrip{
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindNew,
						Protocols: []protocol.ID{testProtocol},
					},
				},
			},
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock, sn host.Host, blockingRequests <-chan struct{}) {
				go func() {
					sn.SetStreamHandler(testProtocol, func(s network.Stream) {
						defer s.Close()
					})
				}()
				<-blockingRequests
			},
			loadBalancerForwardingResponse: &messages.ForwardingResponse{
				Code:    messages.ResponseRejected,
				Message: ErrNotRegistered{peers.serviceNode.id, testProtocol}.Error(),
			},
		},
		{
			name: "new - registered then deleted",
			loadBalancerRouting: []routingRoundtrip{
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindNew,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, &expiry)
					},
				},
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindTerminate,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, nil)
					},
				},
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindNew,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, &expiry)
					},
				},
			},
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock, sn host.Host, blockingRequests <-chan struct{}) {
				sn.SetStreamHandler(testProtocol, func(s network.Stream) {
					defer s.Close()
				})
				sn.RemoveStreamHandler(testProtocol)
			},
			loadBalancerForwardingResponse: &messages.ForwardingResponse{
				Code: messages.ResponseOk,
			},
		},
		{
			name: "extended",
			loadBalancerRouting: []routingRoundtrip{
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindNew,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, &expiry)
					},
				},
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindExtend,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, &expiryPlusHalfDurationPassed)
					},
				},
			},
			post: func(t *testing.T, tn *testNet, testClock *clock.Mock, sn host.Host, blockingRequests <-chan struct{}) {
				testClock.Add(DefaultDuration / 2)
			},
			loadBalancerForwardingResponse: &messages.ForwardingResponse{
				Code: messages.ResponseOk,
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
			blockingRequests := make(chan struct{})
			requestCount := 0

			// setup a mock load balancer for routing
			tn.loadBalancer.SetStreamHandler(RegisterRoutingProtocolID, func(s network.Stream) {
				defer s.Close()
				if requestCount >= len(testCase.loadBalancerRouting) {
					requestCount++
					return
				}
				expectedRouting := testCase.loadBalancerRouting[requestCount]
				requestCount++
				request, err := messages.ReadRoutingRequest(s)
				require.NoError(t, err)
				require.Equal(t, expectedRouting.expectedRequest, request)
				if expectedRouting.writeResponse == nil {
					blockingRequests <- struct{}{}
				} else {
					err = expectedRouting.writeResponse(s)
					require.NoError(t, err)
				}
			})

			sn, err := newServiceNode(ctx, tn.serviceNode, peer.AddrInfo{
				ID:    peers.loadBalancer.id,
				Addrs: []multiaddr.Multiaddr{peers.loadBalancer.multiAddr},
			}, testClock)
			require.NoError(t, err)
			if testCase.pre != nil {
				testCase.pre(t, tn, testClock, sn, blockingRequests)
			}
			sn.SetStreamHandler(testProtocol, func(s network.Stream) {
				defer s.Close()
				require.Equal(t, s.Protocol(), testProtocol)
				require.Equal(t, s.Conn().RemotePeer(), peers.publicNode.id)
				require.Equal(t, s.Conn().RemotePublicKey(), peers.publicNode.publicKey)
				data, err := io.ReadAll(s)
				require.NoError(t, err)
				require.Equal(t, []byte("request"), data)
				_, err = s.Write([]byte("response"))
				require.NoError(t, err)
			})
			if testCase.post != nil {
				testCase.post(t, tn, testClock, sn, blockingRequests)
			}
			s, err := tn.loadBalancer.NewStream(ctx, tn.serviceNode.ID(), ForwardingProtocolID)
			require.NoError(t, err)
			defer s.Close()
			err = messages.WriteInboundForwardingRequest(s, tn.publicNode.ID(), peers.publicNode.publicKey, testProtocol)
			require.NoError(t, err)
			response, err := messages.ReadForwardingResponse(s)
			require.NoError(t, err)
			require.Equal(t, testCase.loadBalancerForwardingResponse, response)

			if response.Code != messages.ResponseRejected {
				_, err = s.Write([]byte("request"))
				require.NoError(t, err)
				err = s.CloseWrite()
				require.NoError(t, err)
				data, err := io.ReadAll(s)
				require.NoError(t, err)
				if testCase.expectedDataResponse != nil {
					require.Equal(t, testCase.expectedDataResponse, data)
				} else {
					require.Equal(t, []byte("response"), data)
				}
			}

			require.Equal(t, len(testCase.loadBalancerRouting), requestCount)
		})
	}
}
func TestNewStream(t *testing.T) {
	testProtocol := protocol.ID("applesauce/mcgee")
	startTime := time.Date(2001, 12, 1, 0, 0, 0, 0, time.UTC)
	expiry := time.Unix(0, startTime.Add(DefaultDuration).UnixNano())
	ctx := context.Background()
	peers := makePeers(t)
	testCases := []struct {
		name                        string
		pre                         func(t *testing.T, tn *testNet, testClock *clock.Mock, sn host.Host, blockingRequests <-chan struct{})
		loadBalancerRouting         []routingRoundtrip
		loadBalancerForwardingError error
		writeForwardingResponse     func(io.Writer) error
		expectedDataResponse        []byte
	}{
		{
			name: "outbound - simple success",
			loadBalancerRouting: []routingRoundtrip{
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindNew,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, &expiry)
					},
				},
			},
			writeForwardingResponse: func(w io.Writer) error {
				return messages.WriteOutboundForwardingResponseSuccess(w, peers.publicNode.publicKey, testProtocol)
			},
		},
		{
			name: "outbound - rejection",
			loadBalancerRouting: []routingRoundtrip{
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindNew,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, &expiry)
					},
				},
			},
			writeForwardingResponse: func(w io.Writer) error {
				return messages.WriteForwardingResponseError(w, errors.New("something went wrong"))
			},
			loadBalancerForwardingError: fmt.Errorf("opening forwarded stream: something went wrong"),
		},
		{
			name: "outbound - close in progress",
			loadBalancerRouting: []routingRoundtrip{
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindNew,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, &expiry)
					},
				},
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindTerminate,
						Protocols: []protocol.ID{testProtocol},
					},
				},
			},
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock, sn host.Host, blockingRequests <-chan struct{}) {
				go func() {
					sn.RemoveStreamHandler(testProtocol)
				}()
				<-blockingRequests
			},
			loadBalancerForwardingError: ErrRouteClosing{testProtocol},
		},
		{
			name: "outbound -- not registered",
			loadBalancerRouting: []routingRoundtrip{
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindNew,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, &expiry)
					},
				},
				{
					expectedRequest: &messages.RoutingRequest{
						Kind:      messages.RoutingKindTerminate,
						Protocols: []protocol.ID{testProtocol},
					},
					writeResponse: func(w io.Writer) error {
						return messages.WriteRoutingResponseSuccess(w, nil)
					},
				},
			},
			pre: func(t *testing.T, tn *testNet, testClock *clock.Mock, sn host.Host, blockingRequests <-chan struct{}) {
				sn.RemoveStreamHandler(testProtocol)
			},
			loadBalancerForwardingError: ErrNotRegistered{peers.serviceNode.id, testProtocol},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			tn := setupTestNet(ctx, t, peers)
			testClock := clock.NewMock()
			testClock.Set(startTime)
			blockingRequests := make(chan struct{})
			requestCount := 0

			// setup a mock load balancer for routing
			tn.loadBalancer.SetStreamHandler(RegisterRoutingProtocolID, func(s network.Stream) {
				defer s.Close()
				if requestCount >= len(testCase.loadBalancerRouting) {
					requestCount++
					return
				}
				expectedRouting := testCase.loadBalancerRouting[requestCount]
				requestCount++
				request, err := messages.ReadRoutingRequest(s)
				require.NoError(t, err)
				require.Equal(t, expectedRouting.expectedRequest, request)
				if expectedRouting.writeResponse == nil {
					blockingRequests <- struct{}{}
				} else {
					err = expectedRouting.writeResponse(s)
					require.NoError(t, err)
				}
			})
			tn.loadBalancer.SetStreamHandler(ForwardingProtocolID, func(s network.Stream) {
				defer s.Close()
				request, err := messages.ReadForwardingRequest(s)
				require.NoError(t, err)
				require.Equal(t, messages.ForwardingOutbound, request.Kind)
				require.Equal(t, []protocol.ID{testProtocol}, request.Protocols)
				require.Equal(t, peers.publicNode.id, request.Remote)
				if testCase.writeForwardingResponse != nil {
					err = testCase.writeForwardingResponse(s)
					require.NoError(t, err)
				}
				if testCase.loadBalancerForwardingError == nil {
					data, err := io.ReadAll(s)
					require.NoError(t, err)
					require.Equal(t, []byte("request"), data)
					_, err = s.Write([]byte("response"))
					require.NoError(t, err)
				}
			})
			sn, err := newServiceNode(ctx, tn.serviceNode, peer.AddrInfo{
				ID:    peers.loadBalancer.id,
				Addrs: []multiaddr.Multiaddr{peers.loadBalancer.multiAddr},
			}, testClock)
			require.NoError(t, err)
			sn.SetStreamHandler(testProtocol, func(s network.Stream) {
				defer s.Close()
			})
			if testCase.pre != nil {
				testCase.pre(t, tn, testClock, sn, blockingRequests)
			}
			s, err := sn.NewStream(ctx, tn.publicNode.ID(), testProtocol)
			if testCase.loadBalancerForwardingError != nil {
				require.EqualError(t, err, testCase.loadBalancerForwardingError.Error())
			} else {
				require.NoError(t, err)
				defer s.Close()
				_, err = s.Write([]byte("request"))
				require.NoError(t, err)
				err = s.CloseWrite()
				require.NoError(t, err)
				data, err := io.ReadAll(s)
				require.NoError(t, err)
				require.Equal(t, []byte("response"), data)
			}

			require.Equal(t, len(testCase.loadBalancerRouting), requestCount)
		})
	}
}
