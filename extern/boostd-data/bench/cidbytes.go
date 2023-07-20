package main

import (
	"fmt"
	"github.com/ipfs/go-cid"
	"math/rand"
	"sync"
)

func cidsFromBytes(bz []byte) ([]cid.Cid, error) {
	var bytesIdx int
	var cids []cid.Cid
	for bytesIdx < len(bz) {
		readCount, pcid, err := cid.CidFromBytes(bz[bytesIdx:])
		if err != nil {
			return nil, fmt.Errorf("parsing bytes to cid: %w", err)
		}

		bytesIdx += readCount
		cids = append(cids, pcid)
	}
	return cids, nil
}

var rndlk sync.Mutex

func generateRandomCid(baseCid []byte) (cid.Cid, error) {
	buff := make([]byte, len(baseCid))
	copy(buff, baseCid)

	rndlk.Lock()
	_, err := rand.Read(buff[len(buff)-8:])
	rndlk.Unlock()
	if err != nil {
		return cid.Undef, err
	}

	_, c, err := cid.CidFromBytes(buff)
	if err != nil {
		return cid.Undef, fmt.Errorf("generating cid: %w", err)
	}

	return c, nil
}
