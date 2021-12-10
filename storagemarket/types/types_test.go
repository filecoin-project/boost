package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	cborgen "github.com/whyrusleeping/cbor-gen"
)

func TestCBORGen(t *testing.T) {
	out := "/Users/dirk/go/src/github.com/filecoin-project/boost/storagemarket/types/types_cbor_gen.go"
	err := cborgen.WriteMapEncodersToFile(out, "types", DealParams{}, DealResponse{}, Transfer{}, StorageAsk{})
	require.NoError(t, err)
}
