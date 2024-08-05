package channels

import (
	"sync"
	"sync/atomic"

	"github.com/filecoin-project/boost/datatransfer"
)

type readOriginalFn func(datatransfer.ChannelID) (int64, error)

type blockIndexKey struct {
	evt  datatransfer.EventCode
	chid datatransfer.ChannelID
}
type blockIndexCache struct {
	lk     sync.RWMutex
	values map[blockIndexKey]*int64
}

func newBlockIndexCache() *blockIndexCache {
	return &blockIndexCache{
		values: make(map[blockIndexKey]*int64),
	}
}

func (bic *blockIndexCache) getValue(evt datatransfer.EventCode, chid datatransfer.ChannelID, readFromOriginal readOriginalFn) (*int64, error) {
	idxKey := blockIndexKey{evt, chid}
	bic.lk.RLock()
	value := bic.values[idxKey]
	bic.lk.RUnlock()
	if value != nil {
		return value, nil
	}
	bic.lk.Lock()
	defer bic.lk.Unlock()
	value = bic.values[idxKey]
	if value != nil {
		return value, nil
	}
	newValue, err := readFromOriginal(chid)
	if err != nil {
		return nil, err
	}
	bic.values[idxKey] = &newValue
	return &newValue, nil
}

func (bic *blockIndexCache) updateIfGreater(evt datatransfer.EventCode, chid datatransfer.ChannelID, newIndex int64, readFromOriginal readOriginalFn) (bool, error) {
	value, err := bic.getValue(evt, chid, readFromOriginal)
	if err != nil {
		return false, err
	}
	for {
		currentIndex := atomic.LoadInt64(value)
		if newIndex <= currentIndex {
			return false, nil
		}
		if atomic.CompareAndSwapInt64(value, currentIndex, newIndex) {
			return true, nil
		}
	}
}
