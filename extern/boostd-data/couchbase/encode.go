package couchbase

import (
	"encoding/ascii85"
	"encoding/base64"
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

func cidsToBytes(cids []cid.Cid) []byte {
	var bz []byte
	for _, c := range cids {
		bz = append(bz, c.Bytes()...)
	}
	return bz
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

// encodeMultihashAsKey encodes the multihash as a utf8-conformant string so that it
// can be used as a couchbase key:
// https://docs.couchbase.com/server/current/learn/data/data.html#keys
func encodeMultihashAsKey(mh []byte) string {
	// ascii85 encodes to 25% larger than the original bytes and doesn't include
	// the space character (which is forbidden by couchbase key syntax)
	dest := make([]byte, ascii85.MaxEncodedLen(len(mh)))
	n := ascii85.Encode(dest, mh)
	return string(dest[:n])
}

// encodeMultihashAsPath encodes the multihash as a string that can be used
// as a couchbase path (so that it can be used as a map key):
// https://docs.couchbase.com/go-sdk/current/howtos/subdocument-operations.html#path-syntax
func encodeMultihashAsPath(mh []byte) string {
	// base64 encodes to 33% larger than the original bytes and doesn't include
	// any characters forbidden by couchbase path syntax
	return base64.RawStdEncoding.EncodeToString(mh)
}

// decodeMultihashAsPath decodes an encoded path string into a multihash
func decodeMultihashAsPath(mhenc string) (multihash.Multihash, error) {
	mhbz, err := base64.RawStdEncoding.DecodeString(mhenc)
	if err != nil {
		return nil, fmt.Errorf("decoding multihash as path: %w", err)
	}

	_, mh, err := multihash.MHFromBytes(mhbz)
	if err != nil {
		return nil, fmt.Errorf("parsing multihash as path from bytes: %w", err)
	}

	return mh, nil
}
