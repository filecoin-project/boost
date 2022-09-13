package loadbalancer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/boost/loadbalancer/messages"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

const DefaultDuration = time.Hour

var log = logging.Logger("loadbalancer")

type registeredRoute struct {
	protocol    protocol.ID
	expiryTimer *clock.Timer
}

type LoadBalancer struct {
	ctx            context.Context
	h              host.Host
	newRouteFilter NewRouteFilter
	clock          clock.Clock
	routesLk       sync.RWMutex
	peers          map[peer.ID][]*registeredRoute
	routes         map[protocol.ID]peer.ID
}

// NewRouteFilter allows user control over whether to accept a new register route request
type NewRouteFilter func(p peer.ID, protocols []protocol.ID) error

func NewLoadBalancer(h host.Host, newRouteFilter NewRouteFilter) *LoadBalancer {
	return newLoadBalancer(h, newRouteFilter, clock.New())
}

func newLoadBalancer(h host.Host, newRouteFilter NewRouteFilter, clock clock.Clock) *LoadBalancer {
	return &LoadBalancer{
		h:              h,
		newRouteFilter: newRouteFilter,
		clock:          clock,
		peers:          make(map[peer.ID][]*registeredRoute),
		routes:         make(map[protocol.ID]peer.ID),
	}
}

func (lb *LoadBalancer) Start(ctx context.Context) {
	lb.ctx = ctx
	lb.h.SetStreamHandler(RegisterRoutingProtocolID, lb.handleRoutingRequest)
	lb.h.SetStreamHandler(ForwardingProtocolID, lb.handleForwarding)
}

func (lb *LoadBalancer) Close() error {
	lb.h.RemoveStreamHandler(RegisterRoutingProtocolID)
	lb.h.RemoveStreamHandler(ForwardingProtocolID)

	lb.routesLk.RLock()
	for id := range lb.routes {
		lb.h.RemoveStreamHandler(id)
	}
	lb.routesLk.RUnlock()
	return nil
}

// handling a new routing request
func (lb *LoadBalancer) handleRoutingRequest(s network.Stream) {
	p := s.Conn().RemotePeer()

	// read request
	request, err := messages.ReadRoutingRequest(s)
	if err != nil {
		log.Warnf("reading routingRequest: %s", err)
		_ = s.Reset()
		return
	}

	defer s.Close()

	expiry := lb.clock.Now().Add(DefaultDuration)

	// process request
	routingErr := lb.processRoutingRequest(p, request, expiry)

	// send response
	if routingErr != nil {
		err = messages.WriteRoutingResponseError(s, routingErr)
	} else {
		// don't send an expiry for terminate requests
		expiryPtr := &expiry
		if request.Kind == messages.RoutingKindTerminate {
			expiryPtr = nil
		}
		err = messages.WriteRoutingResponseSuccess(s, expiryPtr)
	}

	// log any errors writing the response
	if err != nil {
		log.Warnf("writing routing response: %s", err)
		return
	}
}

func (lb *LoadBalancer) processRoutingRequest(p peer.ID, request *messages.RoutingRequest, expiry time.Time) error {
	lb.routesLk.Lock()
	defer lb.routesLk.Unlock()
	switch request.Kind {
	case messages.RoutingKindNew:
		// handle new requests
		return lb.setupNewRoutes(p, request.Protocols, expiry)
	case messages.RoutingKindExtend:
		// handle route extensions
		return lb.extendRoutes(p, request.Protocols, expiry)
	case messages.RoutingKindTerminate:
		// handle terminates
		return lb.terminateRoutes(p, request.Protocols)
	default:
		// this case really shouldn't happen -- it should fail deserializing the request
		return errors.New("unrecognized request kind")
	}
}

func (lb *LoadBalancer) setupNewRoutes(p peer.ID, protocols []protocol.ID, expiry time.Time) error {

	// check route filter if present to verify if we can setup this route
	if lb.newRouteFilter != nil {
		err := lb.newRouteFilter(p, protocols)
		if err != nil {
			return err
		}
	}

	// check existing routes
	for _, id := range protocols {
		if _, ok := lb.routes[id]; ok {
			// error if this protocol is already registered
			return ErrAlreadyRegistered{id}
		}
	}

	// setup routes
	for _, id := range protocols {
		id := id
		expiryTimer := lb.clock.AfterFunc(expiry.Sub(lb.clock.Now()), func() {
			lb.routesLk.Lock()
			lb.expireRoute(id, false)
			lb.routesLk.Unlock()
		})
		lb.peers[p] = append(lb.peers[p], &registeredRoute{id, expiryTimer})
		lb.routes[id] = p
		lb.h.SetStreamHandler(id, lb.handleIncoming)
	}
	return nil
}

func (lb *LoadBalancer) extendRoutes(p peer.ID, protocols []protocol.ID, expiry time.Time) error {

	// check routes to verify ownership
	for _, id := range protocols {
		registeredPeer, ok := lb.routes[id]
		if !ok || p != registeredPeer {
			// error if this protocol not registered to this peer
			return ErrNotRegistered{p, id}
		}
	}

	// setup routes
	for _, id := range protocols {
		id := id
		for _, route := range lb.peers[p] {
			if route.protocol == id {
				// TODO: is there a race around the timer stopping but the after func not yet being called?
				route.expiryTimer.Stop()
				route.expiryTimer = lb.clock.AfterFunc(expiry.Sub(lb.clock.Now()), func() {
					lb.routesLk.Lock()
					lb.expireRoute(id, false)
					lb.routesLk.Unlock()
				})
				break
			}
		}
	}
	return nil
}

func (lb *LoadBalancer) terminateRoutes(p peer.ID, protocols []protocol.ID) error {

	// check routes to verify ownership
	for _, id := range protocols {
		registeredPeer, ok := lb.routes[id]
		if !ok || p != registeredPeer {
			// error if this protocol not registered to this peer
			return ErrNotRegistered{p, id}
		}
	}

	// expire each route
	for _, id := range protocols {
		lb.expireRoute(id, true)
	}
	return nil
}

func (lb *LoadBalancer) expireRoute(id protocol.ID, stopTimer bool) {
	p, ok := lb.routes[id]
	if !ok {
		return
	}
	lb.h.RemoveStreamHandler(id)
	delete(lb.routes, id)
	for i, route := range lb.peers[p] {
		if route.protocol == id {
			if stopTimer {
				route.expiryTimer.Stop()
			}
			lb.peers[p][i] = lb.peers[p][len(lb.peers)-1]
			lb.peers[p] = lb.peers[p][:len(lb.peers[p])-1]
		}
		break
	}
}

// handle a request from a routed peer to make an external ougoing connection
func (lb *LoadBalancer) handleForwarding(s network.Stream) {
	defer s.Close()
	p := s.Conn().RemotePeer()
	request, err := messages.ReadForwardingRequest(s)
	if err != nil {
		log.Warnf("reading forwarding request: %s", err)
		_ = s.Reset()
		return
	}

	// only accept outbound requests
	if request.Kind != messages.ForwardingOutbound {
		messages.WriteForwardingResponseError(s, ErrNoInboundRequests)
		return
	}

	// open the forwarding stream
	outgoingStream, streamErr := lb.processForwardingRequest(p, request.Remote, request.Protocols)

	// if we failed to open the stream, write the response and return
	if streamErr != nil {
		err = messages.WriteForwardingResponseError(s, streamErr)
		if err != nil {
			log.Warnf("writing forwarding response: %s", err)
		}
		return
	}

	defer outgoingStream.Close()

	// write accept response
	err = messages.WriteOutboundForwardingResponseSuccess(s, outgoingStream.Conn().RemotePublicKey(), outgoingStream.Protocol())
	if err != nil {
		log.Warnf("writing forwarding response: %s", err)
		return
	}

	// bridge the streams together
	lb.bridgeStreams(s, outgoingStream)
}

func (lb *LoadBalancer) processForwardingRequest(p peer.ID, remote peer.ID, protocols []protocol.ID) (network.Stream, error) {
	lb.routesLk.RLock()
	// check routes to verify ownership
	for _, id := range protocols {
		registeredPeer, ok := lb.routes[id]
		if !ok || p != registeredPeer {
			lb.routesLk.RUnlock()
			// error if this protocol not registered to this peer
			return nil, ErrNotRegistered{p, id}
		}
	}
	lb.routesLk.RUnlock()
	s, err := lb.h.NewStream(lb.ctx, remote, protocols...)
	if err != nil {
		return nil, fmt.Errorf("remote peer: %w", err)
	}
	return s, nil
}

// pipe a stream through the LB
func (lb *LoadBalancer) bridgeStreams(s1, s2 network.Stream) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		// pipe reads on s1 to writes on s2
		defer wg.Done()
		_, err := io.Copy(s2, s1)
		s2.CloseWrite()
		if err != nil {
			s1.Reset()
		}
	}()
	go func() {
		// pipe reads on s2 to writes on s1
		defer wg.Done()
		_, err := io.Copy(s1, s2)
		s1.CloseWrite()
		if err != nil {
			s2.Reset()
		}
	}()
	wg.Wait()
}

func (lb *LoadBalancer) handleIncoming(s network.Stream) {
	defer s.Close()

	// check routed peer for this stream
	lb.routesLk.RLock()
	routedPeer, ok := lb.routes[s.Protocol()]
	lb.routesLk.RUnlock()
	if !ok {
		// if none exists, return
		log.Warnf("received protocol request for protocol '%s' with no router peer", s.Protocol())
		s.Reset()
		return
	}

	// open a forwarding stream
	routedStream, err := lb.h.NewStream(lb.ctx, routedPeer, ForwardingProtocolID)
	if err != nil {
		log.Warnf("unable to open forwarding stream for protocol '%s' with peer %s", s.Protocol(), routedPeer)
		s.Reset()
		return
	}

	defer routedStream.Close()
	// write an inbound forwarding request with the remote peer and protoocol
	err = messages.WriteInboundForwardingRequest(routedStream, s.Conn().RemotePeer(), s.Conn().RemotePublicKey(), s.Protocol())
	if err != nil {
		log.Warnf("writing forwarding request: %s", err)
		routedStream.Reset()
		s.Reset()
		return
	}

	// read the response
	inbound, err := messages.ReadForwardingResponse(routedStream)
	if err != nil {
		log.Warnf("reading forwarding response: %s", err)
		routedStream.Reset()
		s.Reset()
		return
	}

	// close streams and reset if the forwarding request was not accepted
	if inbound.Code != messages.ResponseOk {
		s.Reset()
		return
	}

	lb.bridgeStreams(s, routedStream)
}
