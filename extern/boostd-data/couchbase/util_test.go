package couchbase

import (
	"github.com/filecoin-project/boost/testutil"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCidsToBytes(t *testing.T) {
	c1 := testutil.GenerateCid()
	c2 := testutil.GenerateCid()

	bz := cidsToBytes([]cid.Cid{c1})
	res, err := bytesToCids(bz)
	require.NoError(t, err)
	require.Equal(t, c1, res[0])

	bz = cidsToBytes([]cid.Cid{c1, c2})
	res, err = bytesToCids(bz)
	require.NoError(t, err)
	require.Equal(t, c1, res[0])
	require.Equal(t, c2, res[1])
}
