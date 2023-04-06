package filters

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multibase"
)

var log = logging.Logger("booster-bitswap")

// BlockFilter manages updating a deny list and checking for CID inclusion in that list
type BlockFilter struct {
	filteredHashesLk sync.RWMutex
	filteredHashes   map[string]struct{}
}

func NewBlockFilter() *BlockFilter {
	return &BlockFilter{
		filteredHashes: make(map[string]struct{}),
	}
}

// FulfillRequest checks if a given CID is in the deny list, per the rules
// of hashing cids (convert to base32, add "/" to path, then sha256 hash)
func (bf *BlockFilter) FulfillRequest(p peer.ID, c cid.Cid) (bool, error) {
	// convert CIDv0 to CIDv1
	if c.Version() == 0 {
		c = cid.NewCidV1(cid.DagProtobuf, c.Hash())
	}
	// get base32 string
	cidStr, err := c.StringOfBase(multibase.Base32)
	if err != nil {
		return false, err
	}
	// add "/"
	cidStr += "/"
	// sha256 sum the bytes
	shaBytes := sha256.Sum256([]byte(cidStr))
	// encode to a hex string
	shaString := hex.EncodeToString(shaBytes[:])

	// check for set inclusion
	bf.filteredHashesLk.RLock()
	_, has := bf.filteredHashes[shaString]
	bf.filteredHashesLk.RUnlock()
	return !has, nil
}

// fetch deny list fetches and parses a deny list to get a new set of filtered hashes
// it uses streaming JSON decoding to avoid an intermediate copy of the entire response
// lenSuggestion is used to avoid a large number of allocations as the list grows
func (bf *BlockFilter) parseDenyList(denyListStream io.Reader, lenSuggestion int) (map[string]struct{}, error) {
	// first fetch the reading for the deny list
	type blockedCid struct {
		Anchor string `json:"anchor"`
	}
	// initialize a json decoder
	jsonDenyList := json.NewDecoder(denyListStream)

	// read open bracket
	_, err := jsonDenyList.Token()
	if err != nil {
		return nil, fmt.Errorf("parsing denylist: %w", err)
	}

	filteredHashes := make(map[string]struct{}, lenSuggestion)
	// while the array contains values
	for jsonDenyList.More() {
		var b blockedCid
		// decode an array value (Message)
		err = jsonDenyList.Decode(&b)
		if err != nil {
			return nil, fmt.Errorf("parsing denylist: %w", err)
		}
		// save it in the filtered hash set
		filteredHashes[b.Anchor] = struct{}{}
	}

	// read closing bracket
	_, err = jsonDenyList.Token()
	if err != nil {
		return nil, fmt.Errorf("parsing denylist: %w", err)
	}

	return filteredHashes, nil
}

// ParseUpdate parses and updates the block filter list based on an endpoint response
func (bf *BlockFilter) ParseUpdate(stream io.Reader) error {
	filteredHashes, err := bf.parseDenyList(stream, len(bf.filteredHashes)+expectedListGrowth)
	if err != nil {
		return err
	}
	bf.filteredHashesLk.Lock()
	bf.filteredHashes = filteredHashes
	bf.filteredHashesLk.Unlock()
	return nil
}
