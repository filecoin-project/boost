package types

import (
	"fmt"
	"math"
	"strconv"
)

// Uint64 is a custom GraphQL type to represent a uint64.
// It has to be added to a schema via "scalar Uint64" since it is not a
// predeclared GraphQL type like "ID".
type Uint64 uint64

// ImplementsGraphQLType maps this custom Go type
// to the graphql scalar type in the schema.
func (Uint64) ImplementsGraphQLType(name string) bool {
	return name == "Uint64"
}

// UnmarshalGraphQL is a custom unmarshaler for Uint64
//
// This function will be called whenever you use the
// Uint64 scalar as an input
func (n *Uint64) UnmarshalGraphQL(input interface{}) error {
	switch input := input.(type) {
	case uint64:
		*n = Uint64(input)
		return nil
	case string:
		pn, err := strconv.ParseUint(input, 10, 64)
		*n = Uint64(pn)
		return err
	case []byte:
		pn, err := strconv.ParseUint(string(input), 10, 64)
		*n = Uint64(pn)
		return err
	case int32:
		if input < 0 {
			return fmt.Errorf("cannot cast negative number %d to uint64", input)
		}
		*n = Uint64(input)
		return nil
	case int64:
		if input < 0 {
			return fmt.Errorf("cannot cast negative number %d to uint64", input)
		}
		*n = Uint64(input)
		return nil
	case float64:
		if input > math.MaxUint64 {
			return fmt.Errorf("float64 %f too large to fit in uint64", input)
		}
		*n = Uint64(input)
		return nil
	default:
		return fmt.Errorf("wrong type for uint64: %T", input)
	}
}

// MarshalJSON is a custom marshaler for Uint64
//
// This function will be called whenever you
// query for fields that use the Uint64 type
func (n Uint64) MarshalJSON() ([]byte, error) {
	json := fmt.Sprintf(`{"__typename": "BigInt", "n": "%d"}`, n)
	return []byte(json), nil
}
