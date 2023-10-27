package testutils

import (
	"github.com/ipfs/go-cid"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
)

var blockGenerator = blocksutil.NewBlockGenerator()

// GenerateCids produces n content identifiers.
func GenerateCids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := blockGenerator.Next().Cid()
		cids = append(cids, c)
	}
	return cids
}

func GenerateCid() cid.Cid {
	return GenerateCids(1)[0]
}
