package yugabyte

import (
	"fmt"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

func bytesToCids(bz []byte) ([]cid.Cid, error) {
	var bytesIdx int
	var pieceCids []cid.Cid
	for bytesIdx < len(bz) {
		readCount, pcid, err := cid.CidFromBytes(bz[bytesIdx:])
		if err != nil {
			return nil, fmt.Errorf("parsing bytes to cid: %w", err)
		}

		bytesIdx += readCount
		pieceCids = append(pieceCids, pcid)
	}
	return pieceCids, nil
}

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
