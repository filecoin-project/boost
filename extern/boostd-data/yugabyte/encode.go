package yugabyte

import (
	"github.com/multiformats/go-multihash"
)

// Probability of a collision in two 24 byte hashes (birthday problem):
// 2^(24*8/2) = 8 x 10^28
const multihashLimitBytes = 24

// trimMultihash trims the multihash to the last multihashLimitBytes bytes
func trimMultihash(mh multihash.Multihash) []byte {
	var idx int
	if len(mh) > multihashLimitBytes {
		idx = len(mh) - multihashLimitBytes
	}
	return mh[idx:]
}
