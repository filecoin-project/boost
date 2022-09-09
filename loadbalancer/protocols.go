package loadbalancer

import (
	"github.com/libp2p/go-libp2p-core/protocol"
)

// RegisterRoutingProtocolID identifies the protocol for registering routes with a libp2p load balancer
const RegisterRoutingProtocolID protocol.ID = "/libp2p/balancer/register-routing/0.0.1"

// ForwardingProtocolID identifies the protocol for requesting forwarding of a protocol for the libp2p load balancer
const ForwardingProtocolID protocol.ID = "/libp2p/balancer/forwarding/0.0.1"
