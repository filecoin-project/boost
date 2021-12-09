package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUint64Marshall(t *testing.T) {
	num := Uint64(12345)
	exp := `{"__typename": "BigInt", "n": "12345"}`
	json, err := num.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, exp, string(json))

	var num2 Uint64
	err = num2.UnmarshalGraphQL(uint64(12345))
	require.NoError(t, err)
	require.EqualValues(t, 12345, num2)
}
