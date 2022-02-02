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
	// Unref is called when a request no longer needs the cache.
	// If err is not nil, the data transfer failed.
	Unref(payloadCid cid.Cid, err error)
	// Close the block info cache manager
	Close() error
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
		bic = &cacheRefs{cache: NewBlockInfoCache(), refs: 1}
		bm.cache[payloadCid] = bic
	}
	return bic.cache
}

func (bm *RefCountBICM) Unref(payloadCid cid.Cid, err error) {
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

func (bm *RefCountBICM) Close() error {
	return nil
}

// DelayedUnrefBICM is like RefCountBCIM but if there was a data transfer
// error, the cache is not unrefed until after some delay.
// This is useful for the case where a connection goes down momentarily and
// then a new request is made (and we want to keep the cache for the new
// request).
type DelayedUnrefBICM struct {
	*RefCountBICM
	unrefDelay time.Duration

	lk     sync.Mutex
	timers map[*time.Timer]struct{}
}

var _ BlockInfoCacheManager = (*DelayedUnrefBICM)(nil)

func NewDelayedUnrefBICM(unrefDelay time.Duration) *DelayedUnrefBICM {
	return &DelayedUnrefBICM{
		RefCountBICM: NewRefCountBICM(),
		unrefDelay:   unrefDelay,
		timers:       make(map[*time.Timer]struct{}),
	}
}

func (d *DelayedUnrefBICM) Unref(payloadCid cid.Cid, err error) {
	if err == nil {
		// No delay, just unref immediately
		d.RefCountBICM.Unref(payloadCid, nil)
	}

	// There should be a delay before the unref.
	// Start a new timer
	var timer *time.Timer
	timer = time.AfterFunc(d.unrefDelay, func() {
		// When the timer expires, unref the cache
		d.lk.Lock()
		delete(d.timers, timer)
		d.lk.Unlock()

		d.RefCountBICM.Unref(payloadCid, nil)
	})

	d.lk.Lock()
	d.timers[timer] = struct{}{}
	d.lk.Unlock()
}

func (d *DelayedUnrefBICM) Close() error {
	d.lk.Lock()
	defer d.lk.Unlock()

	// Stop any running timers
	for timer := range d.timers {
		timer.Stop()
	}

	return nil
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
