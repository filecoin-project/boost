package loadbalancer

import (
	"context"
	"fmt"
	"sync"

	"github.com/filecoin-project/boost/loadbalancer/messages"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

// ServiceNode is a host that behaves as a service node connected to a load balancer
// -- all traffic is routed through the load balancer for each registered protocol
type ServiceNode struct {
	host.Host
	balancer   peer.ID
	handlersLk sync.RWMutex
	handlers   map[protocol.ID]network.StreamHandler
}

// NewServiceNode node constructs a service node connected to the given load balancer on the passed
// in host. A service node behaves exactly like a host.Host but setting up new protocol handlers
// registers routes on the load balancer
func NewServiceNode(ctx context.Context, h host.Host, balancer peer.AddrInfo) (host.Host, error) {
	err := h.Connect(ctx, balancer)
	if err != nil {
		return nil, err
	}
	sn := &ServiceNode{
		Host:     h,
		balancer: balancer.ID,
		handlers: make(map[protocol.ID]network.StreamHandler),
	}
	sn.Host.SetStreamHandler(ForwardingProtocolID, sn.handleForwarding)
	return sn, nil
}

// Close shuts down a service node host's forwarding
func (sn *ServiceNode) Close() error {
	sn.Host.RemoveStreamHandler(ForwardingProtocolID)
	return sn.Host.Close()
}

// SetStreamHandler interrupts the normal process of setting up stream handlers by instead
// registering a route on the connected load balancer. All traffic for this protocol
// will go through the forwarding handshake with load balancer, then the native handler will
// be called
func (sn *ServiceNode) SetStreamHandler(pid protocol.ID, handler network.StreamHandler) {
	// only set the handler if we are successful in registering the route
	sn.handlersLk.Lock()
	sn.handlers[pid] = handler
	sn.handlersLk.Unlock()
}

// these wrappings on the stream or conn make it SEEM like the request is coming
// from the original peer, so that it's processed as if it were
type wrappedStream struct {
	network.Stream
	protocol protocol.ID
	remote   peer.ID
}

type wrappedConn struct {
	network.Conn
	remote peer.ID
}

func (ws *wrappedStream) Protocol() protocol.ID {
	return ws.protocol
}

func (ws *wrappedStream) Conn() network.Conn {
	conn := ws.Stream.Conn()
	return &wrappedConn{conn, ws.remote}
}

func (wc *wrappedConn) RemotePeer() peer.ID {
	return wc.remote
}

// handle inbound forwarding requests
func (sn *ServiceNode) handleForwarding(s network.Stream) {
	// only accept requests from the load balancer
	if s.Conn().RemotePeer() != sn.balancer {
		_ = s.Reset()
		return
	}

	// read the forwarding request
	request, err := messages.ReadForwardingRequest(s)
	if err != nil {
		log.Warnf("reading forwarding request: %s", err)
		_ = s.Reset()
		return
	}

	log.Debugw("received forwarding request for protocol", "protocols", request.Protocols, "remote", request.Remote)

	// validate the request
	handler, responseErr := sn.validateForwardingRequest(request)

	if responseErr != nil {
		log.Infof("rejected forwarding request: %s", responseErr)
		_ = s.Reset()
		return
	}

	// forward to regular handler, which will close stream
	handler(&wrappedStream{s, request.Protocols[0], request.Remote})
}

// validates a forwarding request is one we can accept
func (sn *ServiceNode) validateForwardingRequest(request *messages.ForwardingRequest) (network.StreamHandler, error) {
	sn.handlersLk.RLock()
	defer sn.handlersLk.RUnlock()

	// only accept inbound requests
	if request.Kind != messages.ForwardingInbound {
		return nil, ErrNoOutboundRequests
	}

	// only accept inbound requests for one protocol
	if len(request.Protocols) != 1 {
		return nil, ErrInboundRequestsAreSingleProtocol
	}

	// check for a registered handler
	registeredHandler, ok := sn.handlers[request.Protocols[0]]

	// don't accept inbound requests on protocols we didn't setup routing for
	if !ok {
		return nil, ErrNotRegistered{sn.ID(), request.Protocols[0]}
	}

	// return the registered handler
	return registeredHandler, nil
}

// Calls to "NewStream" open an outbound forwarding request to the load balancer, that is then sent on
// the the specified peer
func (sn *ServiceNode) NewStream(ctx context.Context, p peer.ID, protocols ...protocol.ID) (network.Stream, error) {

	// open a forwarding stream
	routedStream, err := sn.Host.NewStream(ctx, sn.balancer, ForwardingProtocolID)
	if err != nil {
		return nil, err
	}

	// write an outbound forwarding request with the remote peer and protoocol
	err = messages.WriteOutboundForwardingRequest(routedStream, p, protocols)
	if err != nil {
		routedStream.Close()
		return nil, err
	}

	// read the response
	outbound, err := messages.ReadForwardingResponse(routedStream)
	// check for error writing the response
	if err != nil {
		routedStream.Close()
		return nil, err
	}
	// check the response was accepted
	if outbound.Code != messages.ResponseOk {
		routedStream.Close()
		return nil, fmt.Errorf("opening forwarded stream: %s", outbound.Message)
	}

	// return a wrapped stream that appears like a normal stream with the original peer
	return &wrappedStream{routedStream, *outbound.ProtocolID, p}, nil
}

// RemoveStreamHandler removes a stream handler by shutting down registered route with the original host
func (sn *ServiceNode) RemoveStreamHandler(pid protocol.ID) {
	// check if the handler is exists
	sn.handlersLk.Lock()
	delete(sn.handlers, pid)
	sn.handlersLk.Unlock()
}

// Connect for now does nothing
func (sn *ServiceNode) Connect(ctx context.Context, pi peer.AddrInfo) error {
	// for now, this does nothing -- see discussion/improvements
	return nil
}
