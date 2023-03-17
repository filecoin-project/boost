package couchbase

import (
	"fmt"
	"github.com/filecoin-project/boost/testutil"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"unicode/utf8"
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

func TestEncodeMultihashAsPath(t *testing.T) {
	for i := 0; i < 1000; i++ {
		c := testutil.GenerateCid()
		mhstr := encodeMultihashAsPath(c.Hash())
		mh, err := decodeMultihashAsPath(mhstr)
		if err != nil {
			fmt.Printf("%d: %s", i, mhstr)
		}
		require.NoError(t, err)
		require.Equal(t, c.Hash(), mh)
	}
}

// Verifies that encoded paths meet the couchbase requirements:
// https://docs.couchbase.com/go-sdk/current/howtos/subdocument-operations.html#path-syntax
func TestEncodeMultihashAsPathCouchbaseFriendly(t *testing.T) {
	for i := 0; i < 1000; i++ {
		c := testutil.GenerateCid()
		mhstr := encodeMultihashAsPath(c.Hash())

		// Couchbase paths must be utf8 conformant
		isValid := utf8.Valid([]byte(mhstr))
		require.True(t, isValid)

		// Couchbase keys must not contain these characters: space . \ ` [ ]
		for _, c := range " .`[]\\" {
			has := strings.Contains(mhstr, fmt.Sprintf("%c", c))
			require.False(t, has, "encoded string %s is invalid: contains %c", mhstr, c)
		}
	}
}

// Verifies that encoded keys meet the couchbase requirements:
// https://docs.couchbase.com/server/current/learn/data/data.html#keys
func TestEncodeMultihashAsKeyCouchbaseFriendly(t *testing.T) {
	for i := 0; i < 1000; i++ {
		c := testutil.GenerateCid()
		mhstr := encodeMultihashAsKey(c.Hash())

		// Couchbase keys must be utf8 conformant
		isValid := utf8.Valid([]byte(mhstr))
		if !isValid {
			fmt.Println("not valid")
		}
		require.True(t, isValid)

		// Couchbase keys must not contain spaces
		hasSpaces := strings.Contains(mhstr, " ")
		require.False(t, hasSpaces)
	}
}
