package messages

import (
	// to embed schema
	_ "embed"
	"fmt"

	"github.com/ipld/go-ipld-prime/node/bindnode"
	bindnoderegistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"
	"github.com/libp2p/go-libp2p-core/crypto"
	peer "github.com/libp2p/go-libp2p-core/peer"
	protocol "github.com/libp2p/go-libp2p-core/protocol"
)

// ResponseCode indicates a success or failure on either the register-routing or the forwarding protocols
type ResponseCode string

const (
	// ResponseOk indicates a request was successful
	ResponseOk ResponseCode = "Ok"
	// ResponseRejected indicates something went wrong in the request -- more information is supplied in the Message field
	// of the response
	ResponseRejected ResponseCode = "Rejected"
)

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

// ForwardingResponse is a response to and outbound forwarding request
type ForwardingResponse struct {
	Code         ResponseCode
	Message      string // more info if rejected
	ProtocolID   *protocol.ID
	RemotePubKey *crypto.PubKey
}

//go:embed messages.ipldsch
var embedSchema []byte

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

// PubKeyBindnodeOption converts a crypto.PubKey to and from bytes
var PubKeyBindnodeOption = bindnode.TypedBytesConverter(((*crypto.PubKey)(nil)), pubKeyFromBytes, pubKeyToBytes)

// BindnodeRegistry is the serialization/deserialization tool for messages
var BindnodeRegistry = bindnoderegistry.NewRegistry()

var messagesBindnodeOptions = []bindnode.Option{
	PubKeyBindnodeOption,
}

func init() {
	for _, r := range []struct {
		typ     interface{}
		typName string
	}{
		{(*ForwardingRequest)(nil), "ForwardingRequest"},
		{(*ForwardingResponse)(nil), "ForwardingResponse"},
	} {
		if err := BindnodeRegistry.RegisterType(r.typ, string(embedSchema), r.typName, messagesBindnodeOptions...); err != nil {
			panic(err.Error())
		}
	}
}
