package types

import (
	_ "embed"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	bindnoderegistry "github.com/ipld/go-ipld-prime/node/bindnode/registry"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
)

// Query is a query to a given provider to determine information about a piece
// they may have available for retrieval
type Query struct {
	PayloadCID *cid.Cid // V0
	PieceCID   *cid.Cid
}

// QueryResponseStatus indicates whether a queried piece is available
type QueryResponseStatus uint64

const (
	// QueryResponseAvailable indicates a provider has a piece and is prepared to
	// return it
	QueryResponseAvailable QueryResponseStatus = iota

	// QueryResponseUnavailable indicates a provider either does not have or cannot
	// serve the queried piece to the client
	QueryResponseUnavailable

	// QueryResponseError indicates something went wrong generating a query response
	QueryResponseError
)

// QueryResponse is a miners response to a given retrieval query
type QueryResponse struct {
	Status    QueryResponseStatus
	Error     string
	Protocols QueryProtocols
}

// QueryProtocols indicates protocol specific information about retrieval availability
type QueryProtocols struct {
	GraphsyncFilecoinV1 *GraphsyncFilecoinV1Response
	HTTPFilecoinV1      *HTTPFilecoinV1Response
}

// GraphsyncFilecoinV1Response are the query response parameters when GraphSync retrieval is supported
type GraphsyncFilecoinV1Response struct {
	Size uint64 // Total size of piece in bytes
	//ExpectedPayloadSize uint64 // V1 - optional, if PayloadCID + selector are specified and miner knows, can offer an expected size

	PaymentAddress             address.Address // address to send funds to -- may be different than miner addr
	MinPricePerByte            abi.TokenAmount
	MaxPaymentInterval         uint64
	MaxPaymentIntervalIncrease uint64
	Message                    string
	UnsealPrice                abi.TokenAmount
}

// HTTPFilecoinV1Response are the query response parameters when HTTP retrieval is supported
type HTTPFilecoinV1Response struct {
	URL  string
	Size uint64
}

//go:embed queryaskv2.ipldsch
var embedSchema []byte

// BigIntBindnodeOption converts a big.Int type to and from a Bytes field in a
// schema
var BigIntBindnodeOption = bindnode.TypedBytesConverter(&big.Int{}, bigIntFromBytes, bigIntToBytes)

// TokenAmountBindnodeOption converts a filecoin abi.TokenAmount type to and
// from a Bytes field in a schema
var TokenAmountBindnodeOption = bindnode.TypedBytesConverter(&abi.TokenAmount{}, tokenAmountFromBytes, tokenAmountToBytes)

// AddressBindnodeOption converts a filecoin Address type to and from a Bytes
// field in a schema
var AddressBindnodeOption = bindnode.TypedBytesConverter(&address.Address{}, addressFromBytes, addressToBytes)

var filecoinBindnodeOptions = []bindnode.Option{
	BigIntBindnodeOption,
	TokenAmountBindnodeOption,
	AddressBindnodeOption,
}

func tokenAmountFromBytes(b []byte) (interface{}, error) {
	return bigIntFromBytes(b)
}

func bigIntFromBytes(b []byte) (interface{}, error) {
	if len(b) == 0 {
		return big.NewInt(0), nil
	}
	return big.FromBytes(b)
}

func tokenAmountToBytes(iface interface{}) ([]byte, error) {
	return bigIntToBytes(iface)
}

func bigIntToBytes(iface interface{}) ([]byte, error) {
	bi, ok := iface.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("expected *big.Int value")
	}
	if bi == nil || bi.Int == nil {
		*bi = big.Zero()
	}
	return bi.Bytes()
}

func addressFromBytes(b []byte) (interface{}, error) {
	return address.NewFromBytes(b)
}

func addressToBytes(iface interface{}) ([]byte, error) {
	addr, ok := iface.(*address.Address)
	if !ok {
		return nil, fmt.Errorf("expected *Address value")
	}
	return addr.Bytes(), nil
}

var BindnodeRegistry = bindnoderegistry.NewRegistry()

func init() {
	for _, r := range []struct {
		typ     interface{}
		typName string
	}{
		{(*QueryResponse)(nil), "QueryResponse"},
		{(*QueryProtocols)(nil), "QueryProtocols"},
		{(*GraphsyncFilecoinV1Response)(nil), "GraphsyncFilecoinV1Response"},
		{(*HTTPFilecoinV1Response)(nil), "HTTPFilecoinV1Response"},
		{(*Query)(nil), "Query"},
	} {
		if err := BindnodeRegistry.RegisterType(r.typ, string(embedSchema), r.typName, filecoinBindnodeOptions...); err != nil {
			panic(err.Error())
		}
	}
}
