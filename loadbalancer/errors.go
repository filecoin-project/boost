package loadbalancer

import (
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

// ErrAlreadyRegistered indicates a protocol has already been registered as a route
type ErrAlreadyRegistered struct {
	protocolID protocol.ID
}

func (e ErrAlreadyRegistered) Error() string {
	return fmt.Sprintf("protocol already registered: %s", e.protocolID)
}

// ErrNotRegistered indicates a peer has not registered a given protocol but is
// trying to extend or terminate the registration
type ErrNotRegistered struct {
	p          peer.ID
	protocolID protocol.ID
}

func (e ErrNotRegistered) Error() string {
	return fmt.Sprintf("protocol %s is not registered to peer %s", e.protocolID, e.p)
}

type ErrRouteClosing struct {
	protocolID protocol.ID
}

func (e ErrRouteClosing) Error() string {
	return fmt.Sprintf("route is closing for protocol: %s", e.protocolID)
}

// ErrNoInboundRequests is thrown by the load balancer when it receives and inbound request
var ErrNoInboundRequests = errors.New("inbound requests not accepted")

// ErrNoOutboundRequests is thrown by the service node when it receives and outbound request
var ErrNoOutboundRequests = errors.New("outbound requests not accepted")

// ErrInboundRequestsAreSingleProtocol is thrown by the service node when it receives and outbound request
var ErrInboundRequestsAreSingleProtocol = errors.New("inbound requests are single protocol")

// ErrUnableToOpenStream occurs when we're unable to reach the load balancer
type ErrUnableToOpenStream struct {
	original error
	p        peer.ID
}

func (e ErrUnableToOpenStream) Unwrap() error {
	return e.original
}
func (e ErrUnableToOpenStream) Error() string {
	return fmt.Sprintf("opening stream to load balancer %s: %s", e.p, e.original)
}

// ErrWritingRoutingRequest occurs when we're unable to write a routing request
type ErrWritingRoutingRequest struct {
	original error
	pid      protocol.ID
}

func (e ErrWritingRoutingRequest) Unwrap() error {
	return e.original
}

func (e ErrWritingRoutingRequest) Error() string {
	return fmt.Sprintf("writing routing request for protocol %s: %s", e.pid, e.original)
}

// ErrReadingRoutingResponse occurs when we're unable to read a routing response
type ErrReadingRoutingResponse struct {
	original error
	pid      protocol.ID
}

func (e ErrReadingRoutingResponse) Unwrap() error {
	return e.original
}

func (e ErrReadingRoutingResponse) Error() string {
	return fmt.Sprintf("reading routing response for protocol %s: %s", e.pid, e.original)
}

// ErrRejectedRouting indicates a routing request was rejected
type ErrRejectedRouting struct {
	pid     protocol.ID
	message string
}

func (e ErrRejectedRouting) Error() string {
	return fmt.Sprintf("rejected routing for protocol %s, reason %s", e.pid, e.message)
}
