package model

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDBEncodeOffsetSize(t *testing.T) {
	ofszs := []OffsetSize{{
		Offset: 0,
		Size:   0,
	}, {
		Offset: math.MaxUint64 / 2,
		Size:   math.MaxUint64 / 2,
	}, {
		Offset: math.MaxUint64,
		Size:   math.MaxUint64,
	}, {
		Offset: 3039040395,
		Size:   5345435245,
	}}

	for _, ofsz := range ofszs {
		jsonbz, err := json.Marshal(ofsz)
		require.NoError(t, err)

		t.Logf("%d / %d\n", ofsz.Offset, ofsz.Size)
		t.Logf("json encoded: %d bytes\n", len(jsonbz))

		compressed := ofsz.MarshallBase64()
		t.Logf("compressed:   %d bytes\n", len(compressed))

		var res OffsetSize
		err = json.Unmarshal(jsonbz, &res)
		require.NoError(t, err)

		require.Equal(t, ofsz.Offset, res.Offset)
		require.Equal(t, ofsz.Size, res.Size)

		var decompressed OffsetSize
		err = decompressed.UnmarshallBase64(compressed)
		require.NoError(t, err)
		require.Equal(t, ofsz.Offset, decompressed.Offset)
		require.Equal(t, ofsz.Size, decompressed.Size)
	}
}
