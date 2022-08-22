package types

import (
	_ "embed"
	"fmt"

	"github.com/filecoin-project/go-address"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	bindnoderegistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"
	"github.com/multiformats/go-multiaddr"
)

type Protocol struct {
	// The name of the transport protocol eg "libp2p" or "http"
	Name string
	// The address of the endpoint in multiaddr format
	Endpoint multiaddr.Multiaddr
}

type QueryResponse struct {
	Protocols []Protocol
}

//go:embed transports.ipldsch
var embedSchema []byte

// MultiAddrBindnodeOption converts a filecoin Address type to and from a Bytes
// field in a schema
var MultiAddrBindnodeOption = bindnode.TypedBytesConverter(&address.Address{}, multiAddrFromBytes, multiAddrToBytes)

func multiAddrFromBytes(b []byte) (interface{}, error) {
	return multiaddr.NewMultiaddrBytes(b)
}

func multiAddrToBytes(iface interface{}) ([]byte, error) {
	var ma multiaddr.Multiaddr
	ma, ok := iface.(multiaddr.Multiaddr)
	if !ok {
		return nil, fmt.Errorf("expected *Multiaddr value")
	}

	return ma.Bytes(), nil
}

var BindnodeRegistry = bindnoderegistry.NewRegistry()
