package protocolproxy

import (
	"context"
	"fmt"
	"sync"

	"github.com/filecoin-project/boost/protocolproxy/messages"
	"github.com/filecoin-project/boost/safe"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// ForwardingHost is a host that behaves as a service node connected to a proxy
// -- all traffic is routed through the proxy for each registered protocol
type ForwardingHost struct {
	host.Host
	proxy      peer.ID
	handlersLk sync.RWMutex
	handlers   map[protocol.ID]network.StreamHandler
}

// NewForwardingHost node constructs a service node connected to the given proxy on the passed
// in host. A forwarding host behaves exactly like a host.Host but setting up new protocol handlers
// registers routes on the proxy node.
func NewForwardingHost(h host.Host, proxy peer.AddrInfo) host.Host {
	fh := &ForwardingHost{
		Host:     h,
		proxy:    proxy.ID,
		handlers: make(map[protocol.ID]network.StreamHandler),
	}
	fh.Host.SetStreamHandler(ForwardingProtocolID, safe.Handle(fh.handleForwarding))
	return fh
}

// Close shuts down a service node host's forwarding
func (fh *ForwardingHost) Close() error {
	fh.Host.RemoveStreamHandler(ForwardingProtocolID)
	return fh.Host.Close()
}

// SetStreamHandler interrupts the normal process of setting up stream handlers by also
// registering a route on the connected protocol proxy. All traffic on the forwarding
// protocol will go through the forwarding handshake with the proxy, then the native
// handler will be called
func (fh *ForwardingHost) SetStreamHandler(pid protocol.ID, handler network.StreamHandler) {
	fh.Host.SetStreamHandler(pid, safe.Handle(handler))

	// Save the handler so it can be invoked from the forwarding protocol's handler
	// only set the handler if we are successful in registering the route
	fh.handlersLk.Lock()
	fh.handlers[pid] = handler
	fh.handlersLk.Unlock()
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
func (fh *ForwardingHost) handleForwarding(s network.Stream) {
	// only accept requests from the proxy
	if s.Conn().RemotePeer() != fh.proxy {
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
	handler, responseErr := fh.validateForwardingRequest(request)

	if responseErr != nil {
		log.Infof("rejected forwarding request: %s", responseErr)
		_ = s.Reset()
		return
	}

	// forward to regular handler, which will close stream
	handler(&wrappedStream{s, request.Protocols[0], request.Remote})
}

// validates a forwarding request is one we can accept
func (fh *ForwardingHost) validateForwardingRequest(request *messages.ForwardingRequest) (network.StreamHandler, error) {
	fh.handlersLk.RLock()
	defer fh.handlersLk.RUnlock()

	// only accept inbound requests
	if request.Kind != messages.ForwardingInbound {
		return nil, ErrNoOutboundRequests
	}

	// only accept inbound requests for one protocol
	if len(request.Protocols) != 1 {
		return nil, ErrInboundRequestsAreSingleProtocol
	}

	// check for a registered handler
	registeredHandler, ok := fh.handlers[request.Protocols[0]]

	// don't accept inbound requests on protocols we didn't setup routing for
	if !ok {
		return nil, ErrNotRegistered{fh.ID(), request.Protocols[0]}
	}

	// return the registered handler
	return registeredHandler, nil
}

// Calls to "NewStream" open an outbound forwarding request to the proxy, that is then sent on
// the the specified peer
func (fh *ForwardingHost) NewStream(ctx context.Context, p peer.ID, protocols ...protocol.ID) (network.Stream, error) {
	// If there is a direct connection to the peer (or there was a connection
	// recently) open the stream over the direct connection
	if p != fh.proxy {
		connectedness := fh.Host.Network().Connectedness(p)
		if connectedness == network.Connected {
			return fh.Host.NewStream(ctx, p, protocols...)
		}
	}

	// open a forwarding stream
	routedStream, err := fh.Host.NewStream(ctx, fh.proxy, ForwardingProtocolID)
	if err != nil {
		return nil, err
	}

	// write an outbound forwarding request with the remote peer and protocol
	err = messages.WriteOutboundForwardingRequest(routedStream, p, protocols)
	if err != nil {
		_ = routedStream.Close()
		return nil, err
	}

	// read the response
	outbound, err := messages.ReadForwardingResponse(routedStream)
	// check for error writing the response
	if err != nil {
		_ = routedStream.Close()
		return nil, err
	}
	// check the response was accepted
	if outbound.Code != messages.ResponseOk {
		_ = routedStream.Close()
		return nil, fmt.Errorf("opening forwarded stream: %s", outbound.Message)
	}

	// return a wrapped stream that appears like a normal stream with the original peer
	return &wrappedStream{routedStream, *outbound.ProtocolID, p}, nil
}

// RemoveStreamHandler removes a stream handler by shutting down registered route with the original host
func (fh *ForwardingHost) RemoveStreamHandler(pid protocol.ID) {
	// check if the handler exists
	fh.handlersLk.Lock()
	delete(fh.handlers, pid)
	fh.handlersLk.Unlock()
}

// Connect for now does nothing
func (fh *ForwardingHost) Connect(ctx context.Context, pi peer.AddrInfo) error {
	// for now, this does nothing -- see discussion/improvements
	return nil
}
