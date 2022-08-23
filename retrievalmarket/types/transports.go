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
	ma, err := multiaddr.NewMultiaddrBytes(b)
	if err != nil {
		return nil, err
	}
	return &ma, err
}

func multiAddrToBytes(iface interface{}) ([]byte, error) {
	ma, ok := iface.(*multiaddr.Multiaddr)
	if !ok {
		return nil, fmt.Errorf("expected *Multiaddr value")
	}

	return (*ma).Bytes(), nil
}

var BindnodeRegistry = bindnoderegistry.NewRegistry()

func init() {
	var dummyMa multiaddr.Multiaddr
	var bindnodeOptions = []bindnode.Option{
		bindnode.TypedBytesConverter(&dummyMa, multiAddrFromBytes, multiAddrToBytes),
	}
	if err := BindnodeRegistry.RegisterType((*QueryResponse)(nil), string(embedSchema), "QueryResponse", bindnodeOptions...); err != nil {
		panic(err.Error())
	}
}
