package types

import (
	"fmt"
	"math/big"

	stbig "github.com/filecoin-project/go-state-types/big"
)

// BigInt is a custom GraphQL type to represent a big integer.
// It has to be added to a schema via "scalar BigInt" since it is not a
// predeclared GraphQL type like "ID".
type BigInt struct {
	stbig.Int
}

// ImplementsGraphQLType maps this custom Go type
// to the graphql scalar type in the schema.
func (BigInt) ImplementsGraphQLType(name string) bool {
	return name == "BigInt"
}

// UnmarshalGraphQL is a custom unmarshaler for BigInt
//
// This function will be called whenever you use the
// BigInt scalar as an input
func (n *BigInt) UnmarshalGraphQL(input interface{}) error {
	switch input := input.(type) {
	case uint64:
		n.Int.SetUint64(input)
		return nil
	case string:
		var err error
		n.Int, err = stbig.FromString(input)
		return err
	case []byte:
		var err error
		n.Int, err = stbig.FromBytes(input)
		return err
	case int32:
		n.Int.SetInt64(int64(input))
		return nil
	case int64:
		n.Int.SetInt64(input)
		return nil
	case float64:
		f64 := new(big.Float).SetFloat64(input)
		bi := new(big.Int)
		f64.Int(bi)
		n.Int = stbig.Int{Int: bi}
		return nil
	default:
		return fmt.Errorf("wrong type for BigInt: %T", input)
	}
}

// MarshalJSON is a custom marshaler for BigInt
//
// This function will be called whenever you
// query for fields that use the BigInt type
func (n BigInt) MarshalJSON() ([]byte, error) {
	json := fmt.Sprintf(`{"__typename": "BigInt", "n": "%d"}`, n.Int)
	return []byte(json), nil
}
