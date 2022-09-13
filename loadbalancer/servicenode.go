package loadbalancer

import (
	"context"
	"fmt"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/filecoin-project/boost/loadbalancer/messages"
	"github.com/hashicorp/go-multierror"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

type registeredHandler struct {
	closing    chan struct{}
	closeLk    sync.Mutex
	handler    network.StreamHandler
	renewTimer *clock.Timer
}

// ServiceNode is a host that behaves as a service node connected to a load balancer
// -- all traffic is routed through the load balancer for each registered protocol
type ServiceNode struct {
	ctx       context.Context
	ctxCancel func()
	// ensures we shutdown ONLY once
	closeSync sync.Once

	host.Host
	clock      clock.Clock
	balancer   peer.ID
	handlersLk sync.RWMutex
	handlers   map[protocol.ID]*registeredHandler

	// records protocols that are that are in the process of being setup
	// with the load balancer, to avoid setting up twice
	initializingChannelsLk sync.Mutex
	initializingChannels   map[protocol.ID]struct{}
}

// NewServiceNode node constructs a service node connected to the given load balancer on the passed
// in host. A service node behaves exactly like a host.Host but setting up new protocol handlers
// registers routes on the load balancer
func NewServiceNode(ctx context.Context, h host.Host, balancer peer.AddrInfo) (host.Host, error) {
	return newServiceNode(ctx, h, balancer, clock.New())
}

func newServiceNode(ctx context.Context, h host.Host, balancer peer.AddrInfo, clock clock.Clock) (host.Host, error) {
	err := h.Connect(ctx, balancer)
	if err != nil {
		return nil, err
	}
	ctx, ctxCancel := context.WithCancel(ctx)
	sn := &ServiceNode{
		ctx:                  ctx,
		ctxCancel:            ctxCancel,
		Host:                 h,
		balancer:             balancer.ID,
		clock:                clock,
		handlers:             make(map[protocol.ID]*registeredHandler),
		initializingChannels: make(map[protocol.ID]struct{}),
	}
	sn.Host.SetStreamHandler(ForwardingProtocolID, sn.handleForwarding)
	return sn, nil
}

// Close shuts down a service node host's forwarding
func (sn *ServiceNode) Close() error {
	var multiErr error
	sn.closeSync.Do(func() {

		// unregister all routes
		sn.handlersLk.Lock()
		for id, registeredHandler := range sn.handlers {

			// verify the handler is not already closing
			// closeLk protects against a double close
			registeredHandler.closeLk.Lock()
			select {
			case <-registeredHandler.closing:
				registeredHandler.closeLk.Unlock()
				continue
			default:
			}
			close(registeredHandler.closing)
			registeredHandler.closeLk.Unlock()

			// stop any renew timers
			registeredHandler.renewTimer.Stop()

			// attempt to cancel routing
			_, err := sn.sendRoutingRequest(messages.RoutingKindTerminate, id)

			multiErr = multierror.Append(multiErr, err)
		}
		sn.handlersLk.Unlock()

		// remove listening on the forwarding protocol
		sn.Host.RemoveStreamHandler(ForwardingProtocolID)

		// cancel the context
		sn.ctxCancel()
	})
	return multiErr
}

// SetStreamHandler interrupts the normal process of setting up stream handlers by instead
// registering a route on the connected load balancer. All traffic for this protocol
// will go through the forwarding handshake with load balancer, then the native handler will
// be called
func (sn *ServiceNode) SetStreamHandler(pid protocol.ID, handler network.StreamHandler) {
	// check that this route is not registered and or in the process of initialization
	err := sn.checkContention(pid)
	if err != nil {
		log.Info(err.Error())
		return
	}

	// release the initialization lock for this channel when the function ends
	defer func() {
		sn.initializingChannelsLk.Lock()
		delete(sn.initializingChannels, pid)
		sn.initializingChannelsLk.Unlock()
	}()

	response, err := sn.sendRoutingRequest(messages.RoutingKindNew, pid)
	if err != nil {
		log.Errorf(err.Error())
		return
	}

	// set a timeout to renew the connection
	rh := registeredHandler{handler: handler, closing: make(chan struct{})}
	if response.Expiry != nil {
		rh.renewTimer = sn.clock.AfterFunc(response.Expiry.Sub(sn.clock.Now())/2, func() {
			sn.renewConnection(pid)
		})
	}

	// only set the handler if we are successful in registering the route
	sn.handlersLk.Lock()
	sn.handlers[pid] = &rh
	sn.handlersLk.Unlock()
}

// sends a routing request ot the load blaancer and reads the response
func (sn *ServiceNode) sendRoutingRequest(kind messages.RoutingKind, pid protocol.ID) (*messages.RoutingResponse, error) {

	// open a routing request stream
	s, err := sn.Host.NewStream(sn.ctx, sn.balancer, RegisterRoutingProtocolID)
	defer s.Close()
	if err != nil {
		return nil, ErrUnableToOpenStream{err, sn.balancer}
	}
	// send a routing request
	err = messages.WriteRoutingRequest(s, kind, []protocol.ID{pid})
	if err != nil {
		return nil, ErrWritingRoutingRequest{err, pid}
	}
	s.CloseWrite()

	// read a response
	response, err := messages.ReadRoutingResponse(s)
	if err != nil {
		return nil, ErrReadingRoutingResponse{err, pid}
	}
	if response.Code != messages.ResponseOk {
		return nil, ErrRejectedRouting{pid, response.Message}
	}

	return response, nil
}

// when setting up a new protocol handler, makes sure we haven't already registered
// a handler and are not in the process of setting up a route for the handler
func (sn *ServiceNode) checkContention(pid protocol.ID) error {
	// check for existing handler
	sn.handlersLk.Lock()
	defer sn.handlersLk.Unlock()
	_, ok := sn.handlers[pid]
	if ok {
		return fmt.Errorf("attemped to SetStreamHandler twice for protocol: %s", pid)
	}
	// check to see if this channel is currently being initialized
	sn.initializingChannelsLk.Lock()
	defer sn.initializingChannelsLk.Unlock()
	_, ok = sn.initializingChannels[pid]
	if ok {
		return fmt.Errorf("attemped to SetStreamHandler while protocol was registering: %s", pid)
	}
	sn.initializingChannels[pid] = struct{}{}
	return nil
}

// renewConnection sends an extend request to the load balancer
func (sn *ServiceNode) renewConnection(pid protocol.ID) {
	sn.handlersLk.RLock()
	rh, ok := sn.handlers[pid]
	sn.handlersLk.RUnlock()
	if !ok {
		log.Errorf("attempting to renew handler that no longer exists")
		return
	}

	// make sure this channel is not closing or we haven't already shut down
	select {
	case <-rh.closing:
		log.Debugf("cannot renew a channcel that is closing")
		return
	case <-sn.ctx.Done():
		return
	default:
	}

	// request an extension
	response, err := sn.sendRoutingRequest(messages.RoutingKindExtend, pid)
	if err != nil {
		log.Errorf(err.Error())
		return
	}

	// add another handler for the enxt time we expire
	if response.Expiry != nil {
		rh.renewTimer = sn.clock.AfterFunc(response.Expiry.Sub(sn.clock.Now())/2, func() {
			sn.renewConnection(pid)
		})
	}
}

// these wrappings on the stream or conn make it SEEM like the request is coming
// from the original peer, so that it's processed as if it were
type wrappedStream struct {
	network.Stream
	protocol     protocol.ID
	remote       peer.ID
	remotePubKey crypto.PubKey
}

type wrappedConn struct {
	network.Conn
	remote       peer.ID
	remotePubKey crypto.PubKey
}

func (ws *wrappedStream) Protocol() protocol.ID {
	return ws.protocol
}

func (ws *wrappedStream) Conn() network.Conn {
	conn := ws.Stream.Conn()
	return &wrappedConn{conn, ws.remote, ws.remotePubKey}
}

func (wc *wrappedConn) RemotePeer() peer.ID {
	return wc.remote
}

func (wc *wrappedConn) RemotePublicKey() crypto.PubKey {
	return wc.remotePubKey
}

// handle inbound forwarding requests
func (sn *ServiceNode) handleForwarding(s network.Stream) {
	// only accept requests from the load balancer
	if s.Conn().RemotePeer() != sn.balancer {
		s.Reset()
		return
	}

	// read the forwarding request
	request, err := messages.ReadForwardingRequest(s)
	if err != nil {
		log.Warnf("reading forwarding request: %s", err)
		s.Reset()
		return
	}

	// validate the request
	registeredHandler, responseErr := sn.validateForwardingRequest(request)

	if responseErr != nil {
		err = messages.WriteForwardingResponseError(s, responseErr)
		if err != nil {
			log.Warnf("writing forwarding response: %s", err)
			s.Reset()
			return
		}
		s.Close()
		return
	}

	err = messages.WriteInboundForwardingResponseSuccess(s)
	if err != nil {
		log.Warnf("writing forwarding response: %s", err)
		s.Reset()
		return
	}
	// forward to regular handler, which will close stream
	registeredHandler.handler(&wrappedStream{s, request.Protocols[0], request.Remote, *request.RemotePubKey})
}

// validates a forwarding request is one we can accept
func (sn *ServiceNode) validateForwardingRequest(request *messages.ForwardingRequest) (*registeredHandler, error) {
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

// when opening an outbound forwarding request, makes sure we have registered all
// of the registered protocols, and none are in the process of closing
func (sn *ServiceNode) checkAllRoutesRegisteredAndReady(protocols []protocol.ID) error {
	sn.handlersLk.RLock()
	defer sn.handlersLk.RUnlock()
	for _, id := range protocols {
		// verify there is a registered handler
		rh, ok := sn.handlers[id]
		if !ok {
			return ErrNotRegistered{sn.ID(), id}
		}

		// verify the registered route is not in the process of closing
		select {
		case <-rh.closing:
			return ErrRouteClosing{id}
		case <-sn.ctx.Done():
			return sn.ctx.Err()
		default:
		}
	}
	return nil
}

// Calls to "NewStream" open an outbound forwarding request to the load balancer, that is then sent on
// the the specified peer
func (sn *ServiceNode) NewStream(ctx context.Context, p peer.ID, protocols ...protocol.ID) (network.Stream, error) {
	// check that we've registered routes for the given protocols and they're available
	err := sn.checkAllRoutesRegisteredAndReady(protocols)
	if err != nil {
		return nil, err
	}

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
	return &wrappedStream{routedStream, *outbound.ProtocolID, p, *outbound.RemotePubKey}, nil
}

// RemoveStreamHandler removes a stream handler by shutting down registered route with the original host
func (sn *ServiceNode) RemoveStreamHandler(pid protocol.ID) {

	// check if the handler is exists
	sn.handlersLk.RLock()
	handler, ok := sn.handlers[pid]
	sn.handlersLk.RUnlock()
	if !ok {
		log.Debugw(ErrNotRegistered{sn.ID(), pid}.Error())
		return
	}

	// verify the handler is not already closing
	// closeLk protects against a double close
	handler.closeLk.Lock()
	select {
	case <-handler.closing:
		log.Debugw("attemped to unlock handler for protocol")
		handler.closeLk.Unlock()
		return
	case <-sn.ctx.Done():
		return
	default:
	}
	close(handler.closing)
	handler.closeLk.Unlock()

	// stop any timers on the channel
	if handler.renewTimer != nil {
		handler.renewTimer.Stop()
	}

	// Terminate a route with the load balancer
	// TODO: What should we do if this erorrs? We may continue to get
	// messages from the load balancer
	_, err := sn.sendRoutingRequest(messages.RoutingKindTerminate, pid)
	if err != nil {
		log.Errorf(err.Error())
		return
	}

	// unregister the handler
	sn.handlersLk.Lock()
	delete(sn.handlers, pid)
	sn.handlersLk.Unlock()
}

// Connect for now does nothing
func (sn *ServiceNode) Connect(ctx context.Context, pi peer.AddrInfo) error {
	// for now, this does nothing -- see discussion/improvements
	return nil
}
