package sectoraccessor

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/storage/sealer"
	"github.com/filecoin-project/lotus/storage/sectorblocks"
	"github.com/jellydator/ttlcache/v2"
	"sync"
	"time"
)

// sync.Mutex uses 8 bytes of memory
// 16 * 1024 * 8 bytes = 128k memory used
const stripedLockSize = 16 * 1024

// CachingSectorAccessor caches calls to isUnsealed
type CachingSectorAccessor struct {
	dagstore.SectorAccessor
	cache       *ttlcache.Cache
	stripedLock [stripedLockSize]sync.Mutex
}

func (c *CachingSectorAccessor) IsUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error) {
	// Check the cache for this sector
	cacheKey := fmt.Sprintf("%d", sectorID)
	val, err := c.cache.Get(cacheKey)
	if err == nil {
		return val.(bool), nil
	}

	// Cache miss:
	// IsUnsealed is an expensive operation, so wait for any other threads
	// that are calling IsUnsealed for the same sector to complete
	stripedLockIndex := sectorID % stripedLockSize
	c.stripedLock[stripedLockIndex].Lock()
	defer c.stripedLock[stripedLockIndex].Unlock()

	// Check if any other threads updated the cache while this thread waited
	// for the lock
	val, err = c.cache.Get(cacheKey)
	if err == nil {
		return val.(bool), nil
	}

	// Nothing in the cache, so make the call to IsUnsealed
	isUnsealed, err := c.SectorAccessor.IsUnsealed(ctx, sectorID, offset, length)
	if err == nil {
		// Save the results in the cache
		_ = c.cache.Set(cacheKey, isUnsealed)
	}
	return isUnsealed, err
}

type SectorAccessorConstructor func(maddr dtypes.MinerAddress, secb sectorblocks.SectorBuilder, pp sealer.PieceProvider, full v1api.FullNode) dagstore.SectorAccessor

func NewCachingSectorAccessor(maxCacheSize int, cacheExpire time.Duration) SectorAccessorConstructor {
	return func(maddr dtypes.MinerAddress, secb sectorblocks.SectorBuilder, pp sealer.PieceProvider, full v1api.FullNode) dagstore.SectorAccessor {
		sa := NewSectorAccessor(maddr, secb, pp, full)
		cache := ttlcache.NewCache()
		_ = cache.SetTTL(cacheExpire)
		cache.SetCacheSizeLimit(maxCacheSize)
		return &CachingSectorAccessor{SectorAccessor: sa, cache: cache}
	}
}
