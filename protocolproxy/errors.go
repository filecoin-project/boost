package protocolproxy

import (
	"errors"
	"fmt"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// ErrNotRegistered indicates a peer has not registered a given protocol but is
// trying to extend or terminate the registration
type ErrNotRegistered struct {
	p          peer.ID
	protocolID protocol.ID
}

func (e ErrNotRegistered) Error() string {
	return fmt.Sprintf("protocol %s is not registered to peer %s", e.protocolID, e.p)
}

// ErrNoInboundRequests is thrown by the load balancer when it receives and inbound request
var ErrNoInboundRequests = errors.New("inbound requests not accepted")

// ErrNoOutboundRequests is thrown by the service node when it receives and outbound request
var ErrNoOutboundRequests = errors.New("outbound requests not accepted")

// ErrInboundRequestsAreSingleProtocol is thrown by the service node when it receives and outbound request
var ErrInboundRequestsAreSingleProtocol = errors.New("inbound requests are single protocol")
