package types

import (
	"encoding/json"
	"fmt"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
)

type Checkpoint struct {
	Value *string
	Set   bool
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
func (cp *Checkpoint) UnmarshalGraphQL(input interface{}) error {
	cp.Set = true

	if input == nil {
		return nil
	}

	switch v := input.(type) {
	case string:
		_, err := dealcheckpoints.FromString(input.(string))
		if err != nil {
			return fmt.Errorf("invalid Checkpoint value: %T", v)
		}
		cp.Value = &v
		return nil
	default:
		return fmt.Errorf("wrong type for Checkpoint: %T", v)
	}
}

// MarshalJSON is a custom marshaler for Checkpoint
//
// This function will be called whenever you
// query for fields that use the Checkpoint type
func (cp Checkpoint) MarshalJSON() ([]byte, error) {
	return json.Marshal(*cp.Value)
}

func (cp *Checkpoint) Nullable() {}
