package messages

import (
	// to embed schema
	_ "embed"
	"fmt"
	"time"

	"github.com/ipld/go-ipld-prime/node/bindnode"
	bindnoderegistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"
	"github.com/libp2p/go-libp2p-core/crypto"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
)

// RoutingKind indicates the type of routing request we are making
type RoutingKind string

const (
	// RoutingKindNew is a request to register a new protocol route
	RoutingKindNew RoutingKind = "New"
	// RoutingKindExtend is a request to extend an existing protocol route
	RoutingKindExtend RoutingKind = "Extend"
	// RoutingKindTerminate is a request to terminate an existing protocol route
	RoutingKindTerminate RoutingKind = "Terminate"
)

// RoutingRequest is a request to register a new route, extend a route, or terminate a route
type RoutingRequest struct {
	Kind      RoutingKind
	Protocols []protocol.ID
}

// ResponseCode indicates a success or failure on either the register-routing or the forwarding protocols
type ResponseCode string

const (
	// ResponseOk indicates a request was successful
	ResponseOk ResponseCode = "Ok"
	// ResponseRejected indicates something went wrong in the request -- more information is supplied in the Message field
	// of the response
	ResponseRejected ResponseCode = "Rejected"
)

// RoutingResponse is a response on the register-routing protocol
type RoutingResponse struct {
	Code    ResponseCode
	Message string // more info if rejected
	Expiry  *time.Time
}

// ForwardingKind indicates the direction of a forwarding request
type ForwardingKind string

const (
	// ForwardingInbound is a forwarding request initiated by the load balancer to a routed peer, requesting
	// forwarding of a public peers traffic inbound the routed peer
	ForwardingInbound ForwardingKind = "Inbound"
	// ForwardingOutbound is a forwarding request initaited by a routed peer to the load balancer requesting forward
	// of traffic outbound to a public peer
	ForwardingOutbound ForwardingKind = "Outbound"
)

// ForwardingRequest is a request to forward traffic through the load balancer
type ForwardingRequest struct {
	Kind         ForwardingKind
	Remote       peer.ID
	RemotePubKey *crypto.PubKey // only present for inbound
	Protocols    []protocol.ID  // Should always be length 1 for Inbound requests
}

// ForwardingResponse is a response indicating the result of a forwarding request
type ForwardingResponse struct {
	Code         ResponseCode
	Message      string         // more info if rejected
	ProtocolID   *protocol.ID   // only present in response to outbound request
	RemotePubKey *crypto.PubKey // only present in response to outbound request
}

//go:embed messages.ipldsch
var embedSchema []byte

func timeFromInt(i int64) (interface{}, error) {
	t := time.Unix(0, i)
	return &t, nil
}

func timeToInt(iface interface{}) (int64, error) {
	t, ok := iface.(*time.Time)
	if !ok {
		return 0, fmt.Errorf("expected time.Time value")
	}
	return t.UnixNano(), nil
}

func pubKeyToBytes(iface interface{}) ([]byte, error) {
	pk, ok := iface.(*crypto.PubKey)
	if !ok {
		return nil, fmt.Errorf("expected crypto.PubKey value")
	}
	return crypto.MarshalPublicKey(*pk)
}

func pubKeyFromBytes(b []byte) (interface{}, error) {
	pk, err := crypto.UnmarshalPublicKey(b)
	if err != nil {
		return (*crypto.PubKey)(nil), err
	}
	return &pk, err
}

// TimeBindnodeOption converts a time.Time to and from an encoded CBOR int
var TimeBindnodeOption = bindnode.TypedIntConverter(&time.Time{}, timeFromInt, timeToInt)

// PubKeyBindnodeOption converts a crypto.PubKey to and from bytes
var PubKeyBindnodeOption = bindnode.TypedBytesConverter(((*crypto.PubKey)(nil)), pubKeyFromBytes, pubKeyToBytes)

// BindnodeRegistry is the serialization/deserialization tool for messages
var BindnodeRegistry = bindnoderegistry.NewRegistry()

var messagesBindnodeOptions = []bindnode.Option{
	TimeBindnodeOption,
	PubKeyBindnodeOption,
}

func init() {
	for _, r := range []struct {
		typ     interface{}
		typName string
	}{
		{(*RoutingRequest)(nil), "RoutingRequest"},
		{(*RoutingResponse)(nil), "RoutingResponse"},
		{(*ForwardingRequest)(nil), "ForwardingRequest"},
		{(*ForwardingResponse)(nil), "ForwardingResponse"},
	} {
		if err := BindnodeRegistry.RegisterType(r.typ, string(embedSchema), r.typName, messagesBindnodeOptions...); err != nil {
			panic(err.Error())
		}
	}
}
