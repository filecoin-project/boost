package lib

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/dagstore/indexbs"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
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
	ctx   context.Context
	ps    lotus_dtypes.ProviderPieceStore
	sa    retrievalmarket.SectorAccessor
	rp    retrievalmarket.RetrievalProvider
	cache *lru.Cache
}

func NewShardSelector(ctx context.Context, ps lotus_dtypes.ProviderPieceStore, sa retrievalmarket.SectorAccessor, rp retrievalmarket.RetrievalProvider) (*ShardSelector, error) {
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
		// Check if the shard key is in the cache
		var res *shardSelectResult
		resi, cached := s.cache.Get(sk)
		if cached {
			res = resi.(*shardSelectResult)
			sslog.Debugw("shard cache hit", "shard", sk)
		} else {
			sslog.Debugw("shard cache miss", "shard", sk)

			// Check if the shard is available
			res = &shardSelectResult{}
			res.available, res.err = s.isAvailable(sk)
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
		}

		if res.available {
			// We found an available shard, return it
			sslog.Debugw("shard selected", "shard", sk)
			return sk, nil
		}
		if res.err != nil {
			sslog.Debugw("shard error", "shard", sk, "err", res.err)
			lastErr = res.err
		}
	}

	// None of the shards are available
	sslog.Debugw("no shard selected", "shards", shards, "err", lastErr)
	return shard.Key{}, lastErr
}

func (s *ShardSelector) isAvailable(sk shard.Key) (bool, error) {
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
			sslog.Warnf("checking if sector is unsealed", "shard", "sector", di.SectorID, sk, "err", err)
			lastErr = err
			continue
		}

		if isUnsealed {
			sslog.Debugw("sector is unsealed", "shard", "sector", di.SectorID)
			unsealedDeals = append(unsealedDeals, di)
		} else {
			sslog.Debugw("sector is sealed", "shard", "sector", di.SectorID)
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
