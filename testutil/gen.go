package testutil

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-test/random"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RandomBytes returns a byte array of the given size with random values.
func RandomBytes(n int64) []byte {
	return random.Bytes(int(n))
}

// GenerateBlocksOfSize generates a series of blocks of the given byte size
func GenerateBlocksOfSize(n int, size int64) []blocks.Block {
	return random.BlocksOfSize(n, int(size))
}

// GenerateCids produces n content identifiers.
func GenerateCids(n int) []cid.Cid {
	return random.Cids(n)
}

func GenerateCid() cid.Cid {
	return random.Cids(1)[0]
}

// GeneratePeers creates n peer ids.
func GeneratePeers(n int) []peer.ID {
	return random.Peers(n)
}

func GeneratePeer() peer.ID {
	return random.Peers(1)[0]
}
