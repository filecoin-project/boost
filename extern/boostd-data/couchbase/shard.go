package couchbase

import (
	"fmt"
	"github.com/multiformats/go-multihash"
)

// Couchbase has an upper limit on the size of a value: 20mb
// A JSON-encoded map with 128k keys results in a value of about 8mb in size
// so this is well under the 20mb limit.
// See TestJSONMarshalledMapSize
var maxRecsPerShard = 128 * 1024

// Limit the number of shards to 2048. This means there is an upper limit of
// ~270 million blocks per piece, which should be more than enough:
// 64 Gib piece / (2048 * 128 * 1024) = 238 bytes per block
const maxShardsPerPiece = 2048

var maxRecsPerPiece = maxShardsPerPiece * maxRecsPerShard

func getShardPrefixBitCount(recordCount int) (int, int) {
	// The number of shards required to store all the keys
	requiredShards := (recordCount / maxRecsPerShard) + 1
	// The number of shards that will be created must be a power of 2,
	// so get the first power of two that's >= requiredShards
	shardPrefixBits := 0
	totalShards := 1
	for totalShards < requiredShards {
		shardPrefixBits += 1
		totalShards *= 2
	}

	return shardPrefixBits, totalShards
}

func getShardPrefix(shardIndex int) (string, error) {
	if shardIndex >= 1<<16 {
		return "", fmt.Errorf("shard index of size %d does not fit into 2 byte prefix", shardIndex)
	}

	shardPrefix := []byte{0, 0}
	shardPrefix[1] = byte(shardIndex)
	shardPrefix[0] = byte(shardIndex >> 8)
	return string(shardPrefix), nil
}

// Create a 2 byte mask of the required number of bits
// eg 3 bit mask = 0000 0000 0000 0111
func get2ByteMask(bits int) [2]byte {
	buf := [2]byte{0, 0}
	buf[1] = (1 << bits) - 1
	if bits >= 8 {
		buf[0] = (1 << (bits - 8)) - 1
	}
	return buf
}

// Apply a mask to the last two bytes of the hash to use as the shard prefix
func hashToShardPrefix(hash multihash.Multihash, mask [2]byte) string {
	return string([]byte{
		hash[len(hash)-2] & mask[0],
		hash[len(hash)-1] & mask[1],
	})
}
