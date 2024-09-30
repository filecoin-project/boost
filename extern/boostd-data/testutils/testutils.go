package testutils

import (
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
)

// GenerateCids produces n content identifiers.
func GenerateCids(n int) []cid.Cid {
	return random.Cids(n)
}

func GenerateCid() cid.Cid {
	return random.Cids(1)[0]
}
