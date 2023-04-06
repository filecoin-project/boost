package lib

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/boost-gfm/piecestore"
	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/dagstore/indexbs"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/go-state-types/abi"
	lru "github.com/hnlq715/golang-lru"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var sslog = logging.Logger("shardselect")

// ShardSelector is used by the dagstore's index-backed blockstore to select
// the best shard from which to retrieve a particular cid.
// It chooses the first shard that is unsealed and free (zero cost).
// It caches the results per-shard.
type ShardSelector struct {
	ctx context.Context
	ps  piecestore.PieceStore
	sa  retrievalmarket.SectorAccessor
	rp  retrievalmarket.RetrievalProvider

	// The striped lock protects against multiple threads doing a lookup
	// against the sealing subsystem / retrieval ask for the same shard
	stripedLock [256]sync.Mutex
	cache       *lru.Cache
}

func NewShardSelector(ctx context.Context, ps piecestore.PieceStore, sa retrievalmarket.SectorAccessor, rp retrievalmarket.RetrievalProvider) (*ShardSelector, error) {
	cache, err := lru.New(2048)
	if err != nil {
		return nil, fmt.Errorf("creating shard selector cache: %w", err)
	}
	return &ShardSelector{ctx: ctx, ps: ps, sa: sa, rp: rp, cache: cache}, nil
}

var selectorCacheDuration = 10 * time.Minute
var selectorCacheErrorDuration = time.Minute

type shardSelectResult struct {
	available bool
	err       error
}

// ShardSelectorF chooses the first shard that is unsealed and free (zero cost)
func (s *ShardSelector) ShardSelectorF(c cid.Cid, shards []shard.Key) (shard.Key, error) {
	// If no shards are selected, return ErrNoShardSelected
	lastErr := indexbs.ErrNoShardSelected

	sslog.Debugw("shard selection", "shards", shards)
	for _, sk := range shards {
		lkidx := s.stripedLockIndex(sk)
		s.stripedLock[lkidx].Lock()
		available, err := s.isAvailable(sk)
		s.stripedLock[lkidx].Unlock()

		if available {
			// We found an available shard, return it
			sslog.Debugw("shard selected", "shard", sk)
			return sk, nil
		}
		if err != nil {
			sslog.Debugw("shard error", "shard", sk, "err", err)
			lastErr = err
		}
	}

	// None of the shards are available
	sslog.Debugw("no shard selected", "shards", shards, "err", lastErr)
	return shard.Key{}, lastErr
}

func (s *ShardSelector) isAvailable(sk shard.Key) (bool, error) {
	// Check if the shard key is in the cache
	var res *shardSelectResult
	resi, cached := s.cache.Get(sk)
	if cached {
		res = resi.(*shardSelectResult)
		sslog.Debugw("shard cache hit", "shard", sk)
		return res.available, res.err
	}
	sslog.Debugw("shard cache miss", "shard", sk)

	// Check if the shard is available
	res = &shardSelectResult{}
	res.available, res.err = s.checkIsAvailable(sk)
	expireIn := selectorCacheDuration
	if res.err != nil {
		// If there's an error, cache for a short duration so that we
		// don't wait too long to try again.
		expireIn = selectorCacheErrorDuration
		res.available = false
		res.err = fmt.Errorf("running shard selection for shard %s: %w", sk, res.err)
		sslog.Warnw("checking shard availability", "shard", sk, "err", res.err)
	}
	// Add the result to the cache
	s.cache.AddEx(sk, res, expireIn)

	return res.available, res.err
}

func (s *ShardSelector) checkIsAvailable(sk shard.Key) (bool, error) {
	// Parse piece CID
	pieceCid, err := cid.Parse(sk.String())
	if err != nil {
		return false, fmt.Errorf("parsing shard key as cid: %w", err)
	}

	// Read piece info from piece store
	sslog.Debugw("getting piece info", "shard", sk)
	pieceInfo, err := s.ps.GetPieceInfo(pieceCid)
	if err != nil {
		return false, fmt.Errorf("get piece info: %w", err)
	}

	// Filter for deals that are unsealed
	sslog.Debugw("filtering for unsealed deals", "shard", sk, "deals", len(pieceInfo.Deals))
	unsealedDeals := make([]piecestore.DealInfo, 0, len(pieceInfo.Deals))
	var lastErr error
	for _, di := range pieceInfo.Deals {
		isUnsealed, err := s.sa.IsUnsealed(s.ctx, di.SectorID, di.Offset.Unpadded(), di.Length.Unpadded())
		if err != nil {
			sslog.Warnf("checking if sector is unsealed", "shard", sk, "sector", di.SectorID, sk, "err", err)
			lastErr = err
			continue
		}

		if isUnsealed {
			sslog.Debugw("sector is unsealed", "shard", sk, "sector", di.SectorID)
			unsealedDeals = append(unsealedDeals, di)
		} else {
			sslog.Debugw("sector is sealed", "shard", sk, "sector", di.SectorID)
		}
	}

	if len(unsealedDeals) == 0 {
		// It wasn't possible to find an unsealed sector
		sslog.Debugw("no unsealed deals found", "shard", sk)
		return false, lastErr
	}

	// Check if the piece is available for free (zero-cost) retrieval
	input := retrievalmarket.PricingInput{
		// Piece from which the payload will be retrieved
		PieceCID: pieceInfo.PieceCID,
		Unsealed: true,
	}

	var dealsIds []abi.DealID
	for _, d := range unsealedDeals {
		dealsIds = append(dealsIds, d.DealID)
	}

	sslog.Debugw("getting dynamic asking price for unsealed deals", "shard", sk, "deals", len(unsealedDeals))
	ask, err := s.rp.GetDynamicAsk(s.ctx, input, dealsIds)
	if err != nil {
		return false, fmt.Errorf("getting retrieval ask: %w", err)
	}

	// The piece is available for free retrieval
	if ask.PricePerByte.NilOrZero() {
		sslog.Debugw("asking price for unsealed deals is zero", "shard", sk)
		return true, nil
	}

	sslog.Debugw("asking price-per-byte for unsealed deals is non-zero", "shard", sk, "price", ask.PricePerByte.String())
	return false, nil
}

func (s *ShardSelector) stripedLockIndex(sk shard.Key) int {
	skstr := sk.String()
	return int(skstr[len(skstr)-1])
}
