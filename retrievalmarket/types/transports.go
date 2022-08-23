package types

import (
	_ "embed"
	"fmt"

	"github.com/ipld/go-ipld-prime/node/bindnode"
	bindnoderegistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"
	"github.com/multiformats/go-multiaddr"
)

type Protocol struct {
	// The name of the transport protocol eg "libp2p" or "http"
	Name string
	// The address of the endpoint in multiaddr format
	Addresses []multiaddr.Multiaddr
}

type QueryResponse struct {
	Protocols []Protocol
}

//go:embed transports.ipldsch
var embedSchema []byte

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

// MultiAddrBindnodeOption converts a filecoin Multiaddr type to and from a Bytes
// field in a schema
var dummyMa multiaddr.Multiaddr
var MultiAddrBindnodeOption = bindnode.TypedBytesConverter(&dummyMa, multiAddrFromBytes, multiAddrToBytes)

var bindnodeOptions = []bindnode.Option{
	MultiAddrBindnodeOption,
}

var BindnodeRegistry = bindnoderegistry.NewRegistry()

func init() {
	for _, r := range []struct {
		typ     interface{}
		typName string
	}{
		{(*QueryResponse)(nil), "QueryResponse"},
		{(*Protocol)(nil), "Protocol"},
		{(multiaddr.Multiaddr)(nil), "Multiaddr"},
	} {
		if err := BindnodeRegistry.RegisterType(r.typ, string(embedSchema), r.typName, bindnodeOptions...); err != nil {
			panic(err.Error())
		}
	}
}
