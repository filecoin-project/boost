package couchbase

import (
	"fmt"
	"github.com/ipfs/go-cid"
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
