package blockfilter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multibase"
)

var log = logging.Logger("booster-bitswap")

// BadBitsDenyList is the URL for well known bad bits list
const BadBitsDenyList string = "https://badbits.dwebops.pub/denylist.json"

// UpdateInterval is the default interval at which the public list is refected and updated
const UpdateInterval = 5 * time.Minute

// DenyListFetcher is a function that fetches a deny list in the json style of the BadBits list
// The first return value indicates whether any update has occurred since the last fetch time
// The second return is a stream of data if an update has occurred
// The third is any error
type DenyListFetcher func(lastFetchTime time.Time) (bool, io.ReadCloser, error)

const expectedListGrowth = 128

// FetchBadBitsList is the default function used to get the BadBits list
func FetchBadBitsList(ifModifiedSince time.Time) (bool, io.ReadCloser, error) {
	req, err := http.NewRequest("GET", BadBitsDenyList, nil)
	if err != nil {
		return false, nil, err
	}

	req.Header.Set("If-Modified-Since", ifModifiedSince.Format(http.TimeFormat))
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, nil, err
	}
	if response.StatusCode == http.StatusNotModified {
		return false, nil, nil
	}
	if response.StatusCode < 200 && response.StatusCode > 299 {
		return false, nil, errors.New("http status code was not success")
	}
	return true, response.Body, nil
}

// BlockFilter manages updating a deny list and checking for CID inclusion in that list
type BlockFilter struct {
	lastUpdated      time.Time
	denyListFetcher  DenyListFetcher
	filteredHashesLk sync.RWMutex
	filteredHashes   map[string]struct{}
	ctx              context.Context
	cancel           context.CancelFunc
	clock            clock.Clock
	onTimerSet       func()
}

func newBlockFilter(denyListFetcher DenyListFetcher, clock clock.Clock, onTimerSet func()) *BlockFilter {
	return &BlockFilter{
		denyListFetcher: denyListFetcher,
		filteredHashes:  make(map[string]struct{}),
		clock:           clock,
		onTimerSet:      onTimerSet,
	}
}

// NewBlockFilter returns a block filter
func NewBlockFilter() *BlockFilter {
	return newBlockFilter(FetchBadBitsList, clock.New(), nil)
}

// Start initializes asynchronous updates to the deny list filter
// It blocks to confirm at least one synchronous update of the denylist
func (bf *BlockFilter) Start(ctx context.Context) {
	bf.ctx, bf.cancel = context.WithCancel(ctx)
	bf.updateDenyList()
	go bf.run()
}

// Close shuts down asynchronous updating
func (bf *BlockFilter) Close() {
	bf.cancel()
}

// IsFiltered checks if a given CID is in the deny list, per the rules
// of hashing cids (convert to base32, add "/" to path, then sha256 hash)
func (bf *BlockFilter) IsFiltered(c cid.Cid) (bool, error) {
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
	return has, nil
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

// updateDenyList replaces the current filtered hashes after successfully
// fetching and parsing the latest deny list
func (bf *BlockFilter) updateDenyList() {
	fetchTime := time.Now()
	updated, denyListStream, err := bf.denyListFetcher(bf.lastUpdated)
	if err != nil {
		log.Errorf("fetching deny list: %s", err)
		return
	}
	if !updated {
		return
	}
	defer denyListStream.Close()
	bf.lastUpdated = fetchTime
	filteredHashes, err := bf.parseDenyList(denyListStream, len(bf.filteredHashes)+expectedListGrowth)
	if err != nil {
		log.Errorf("parsing deny list: %s", err)
		return
	}
	bf.filteredHashesLk.Lock()
	bf.filteredHashes = filteredHashes
	bf.filteredHashesLk.Unlock()
}

// run periodically updates the deny list asynchronously
func (bf *BlockFilter) run() {
	updater := bf.clock.Timer(UpdateInterval)
	// call the callback if set
	if bf.onTimerSet != nil {
		bf.onTimerSet()
	}
	for {
		select {
		case <-bf.ctx.Done():
			return
		case <-updater.C:
			// when timer expires, update deny list
			bf.updateDenyList()

			// then reset the timer
			updater.Reset(UpdateInterval)
			// call the callback if set
			if bf.onTimerSet != nil {
				bf.onTimerSet()
			}
		}
	}
}
