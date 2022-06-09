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

	var num3 BigInt
	err = num3.UnmarshalGraphQL(int32(1234))
	require.NoError(t, err)
	require.EqualValues(t, 1234, num3.Uint64())

	var num4 BigInt
	err = num4.UnmarshalGraphQL(int64(1234))
	require.NoError(t, err)
	require.EqualValues(t, 1234, num4.Uint64())

	var num5 BigInt
	err = num5.UnmarshalGraphQL(uint32(1234))
	require.NoError(t, err)
	require.EqualValues(t, 1234, num5.Uint64())

	var num6 BigInt
	err = num6.UnmarshalGraphQL(uint64(1234))
	require.NoError(t, err)
	require.EqualValues(t, 1234, num6.Uint64())

	var num7 BigInt
	err = num7.UnmarshalGraphQL(float32(1234))
	require.NoError(t, err)
	require.EqualValues(t, 1234, num7.Uint64())

	var num8 BigInt
	err = num8.UnmarshalGraphQL(float64(1234))
	require.NoError(t, err)
	require.EqualValues(t, 1234, num8.Uint64())
}
