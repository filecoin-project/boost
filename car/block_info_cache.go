package car

import (
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
)

// BlockInfoCacheManager gets a BlockInfoCache for a given payload cid.
// Implementations may wish to share the same cache between multiple
// requests for the same payload.
type BlockInfoCacheManager interface {
	// Get the BlockInfoCache by payload cid
	Get(payloadCid cid.Cid) *BlockInfoCache
	// Unref is called when a request no longer needs the cache
	Unref(payloadCid cid.Cid)
}

type cacheRefs struct {
	refs  int
	cache *BlockInfoCache
}

// RefCountBICM keeps track of references to a BlockInfoCache by payload cid,
// and deletes the cache once references reach zero.
type RefCountBICM struct {
	lk    sync.Mutex
	cache map[cid.Cid]*cacheRefs
}

var _ BlockInfoCacheManager = (*RefCountBICM)(nil)

func NewRefCountBICM() *RefCountBICM {
	return &RefCountBICM{cache: make(map[cid.Cid]*cacheRefs)}
}

func (bm *RefCountBICM) Get(payloadCid cid.Cid) *BlockInfoCache {
	bm.lk.Lock()
	defer bm.lk.Unlock()

	bic, ok := bm.cache[payloadCid]
	if ok {
		bic.refs++
	} else {
		bic = &cacheRefs{cache: NewBlockInfoCache()}
		bm.cache[payloadCid] = bic
	}
	return bic.cache
}

func (bm *RefCountBICM) Unref(payloadCid cid.Cid) {
	bm.lk.Lock()
	defer bm.lk.Unlock()

	bic, ok := bm.cache[payloadCid]
	if !ok {
		return
	}

	bic.refs--
	if bic.refs == 0 {
		delete(bm.cache, payloadCid)
	}
}

// DelayedUnrefBICM is like RefCountBCIM but when the last reference is
// unrefed, the cache is not removed for some delay. This is useful for the
// case where a connection goes down momentarily and then a new request
// is made (and we want to keep the cache for the new request).
type DelayedUnrefBICM struct {
	*RefCountBICM
	unrefDelay time.Duration
	timers     map[cid.Cid]*time.Timer
}

var _ BlockInfoCacheManager = (*DelayedUnrefBICM)(nil)

func NewDelayedUnrefBICM(unrefDelay time.Duration) *DelayedUnrefBICM {
	return &DelayedUnrefBICM{
		RefCountBICM: NewRefCountBICM(),
		unrefDelay:   unrefDelay,
		timers:       make(map[cid.Cid]*time.Timer),
	}
}

func (d *DelayedUnrefBICM) Unref(payloadCid cid.Cid) {
	d.unref(payloadCid, true)
}

func (d *DelayedUnrefBICM) unref(payloadCid cid.Cid, withDelay bool) {
	d.lk.Lock()
	defer d.lk.Unlock()

	bic, ok := d.cache[payloadCid]
	if !ok {
		return
	}

	bic.refs--

	// If there are no more references to the cache for this payload cid
	if bic.refs == 0 {
		if withDelay {
			// If there's already a timer running, stop it
			timer, ok := d.timers[payloadCid]
			if ok {
				timer.Stop()
			}

			// Start a new timer
			timer = time.AfterFunc(d.unrefDelay, func() {
				// When the timer expires, remove the cache for this payload cid
				d.unref(payloadCid, false)
			})
			d.timers[payloadCid] = timer
		} else {
			// Delete the timer for this payload cid
			delete(d.timers, payloadCid)
		}

		// Delete the cache for this payload cid
		delete(d.cache, payloadCid)
	}
}

func (d *DelayedUnrefBICM) Close() {
	// Stop any running timers
	for _, timer := range d.timers {
		timer.Stop()
	}
}

// BlockInfo keeps track of blocks in a CAR file
type BlockInfo struct {
	// The offset into the CAR file
	offset uint64
	// Note: size is the size of the block and metadata in the CAR file
	size uint64
	// Links to child nodes in the DAG
	links []*format.Link
}

// BlockInfoCache keeps block info by cid
type BlockInfoCache struct {
	lk    sync.RWMutex
	cache map[cid.Cid]*BlockInfo
}

func NewBlockInfoCache() *BlockInfoCache {
	return &BlockInfoCache{cache: make(map[cid.Cid]*BlockInfo)}
}

func (bic *BlockInfoCache) Get(c cid.Cid) (*BlockInfo, bool) {
	bic.lk.RLock()
	defer bic.lk.RUnlock()

	bi, ok := bic.cache[c]
	return bi, ok
}

func (bic *BlockInfoCache) Put(c cid.Cid, bi *BlockInfo) {
	bic.lk.Lock()
	defer bic.lk.Unlock()

	bic.cache[c] = bi
}
