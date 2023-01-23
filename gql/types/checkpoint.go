package types

import (
	"fmt"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
)

type Checkpoint struct {
	String string
}

// ImplementsGraphQLType maps this custom Go type
// to the graphql scalar type in the schema.
func (Checkpoint) ImplementsGraphQLType(name string) bool {
	return name == "Checkpoint"
}

// UnmarshalGraphQL is a custom unmarshaler for Checkpoint
//
// This function will be called whenever you use the
// Checkpoint scalar as an input
func (n *Checkpoint) UnmarshalGraphQL(input interface{}) error {
	cp, err := dealcheckpoints.FromString(input.(string))
	n.String = cp.String()
	return err
}

// MarshalJSON is a custom marshaler for Checkpoint
//
// This function will be called whenever you
// query for fields that use the Checkpoint type
func (n Checkpoint) MarshalJSON() ([]byte, error) {
	json := fmt.Sprintf(`{"__typename": "Checkpoint", "n": "%s"}`, n.String)
	return []byte(json), nil
}
