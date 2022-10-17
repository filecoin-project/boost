package protocolproxy

import (
	"github.com/libp2p/go-libp2p/core/protocol"
)

// ForwardingProtocolID identifies the protocol for requesting forwarding of a protocol for the libp2p load balancer
const ForwardingProtocolID protocol.ID = "/libp2p/balancer/forwarding/0.0.1"
