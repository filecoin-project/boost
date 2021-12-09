package types

import (
	"testing"

	stbig "github.com/filecoin-project/go-state-types/big"

	"github.com/stretchr/testify/require"
)

func TestBigIntMarshall(t *testing.T) {
	num := BigInt{Int: stbig.NewInt(12345)}
	exp := `{"__typename": "BigInt", "n": "12345"}`
	json, err := num.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, exp, string(json))

	var num2 BigInt
	err = num2.UnmarshalGraphQL("12345")
	require.NoError(t, err)
	require.EqualValues(t, 12345, num2.Uint64())
}
