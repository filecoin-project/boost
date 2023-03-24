package lib

import (
	"context"
	"github.com/filecoin-project/boost-gfm/piecestore"
	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/boost/retrievalmarket/mock"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/dagstore/indexbs"
	"github.com/filecoin-project/dagstore/shard"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"testing"
)

func TestShardSelector(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name         string
		deals        []piecestore.DealInfo
		isUnsealed   []bool
		pricePerByte int64
		expectErr    error
	}{{
		name:      "no deals",
		deals:     nil,
		expectErr: indexbs.ErrNoShardSelected,
	}, {
		name:       "only sealed deals",
		deals:      []piecestore.DealInfo{{SectorID: 0}, {SectorID: 1}},
		isUnsealed: []bool{false, false}, // index corresponds to sector ID
		expectErr:  indexbs.ErrNoShardSelected,
	}, {
		name:         "one unsealed deal but non-zero price",
		deals:        []piecestore.DealInfo{{SectorID: 0}, {SectorID: 1}},
		isUnsealed:   []bool{false, true}, // index corresponds to sector ID
		pricePerByte: 1,
		expectErr:    indexbs.ErrNoShardSelected,
	}, {
		name:         "one unsealed deal with zero price",
		deals:        []piecestore.DealInfo{{SectorID: 0}, {SectorID: 1}},
		isUnsealed:   []bool{false, true}, // index corresponds to sector ID
		pricePerByte: 0,
		expectErr:    nil,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			pieceStore := mock.NewMockPieceStore(ctrl)
			sectorAccessor := mock.NewMockSectorAccessor(ctrl)
			retrievalProv := mock.NewMockRetrievalProvider(ctrl)
			ss, err := NewShardSelector(ctx, pieceStore, sectorAccessor, retrievalProv)
			require.NoError(t, err)

			blockCid := testutil.GenerateCid()
			require.NoError(t, err)
			sk1cid := testutil.GenerateCid()
			sk1 := shard.KeyFromCID(sk1cid)
			shards := []shard.Key{sk1}

			pi := piecestore.PieceInfo{
				PieceCID: testutil.GenerateCid(),
				Deals:    tc.deals,
			}
			pieceStore.EXPECT().GetPieceInfo(sk1cid).AnyTimes().Return(pi, nil)

			for _, dl := range tc.deals {
				isUnsealed := tc.isUnsealed[dl.SectorID]
				sectorAccessor.EXPECT().IsUnsealed(gomock.Any(), dl.SectorID, gomock.Any(), gomock.Any()).AnyTimes().Return(isUnsealed, nil)
			}

			ask := retrievalmarket.Ask{PricePerByte: abi.NewTokenAmount(tc.pricePerByte)}
			retrievalProv.EXPECT().GetDynamicAsk(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(ask, nil)

			sk, err := ss.ShardSelectorF(blockCid, shards)
			if tc.expectErr != nil {
				require.ErrorIs(t, err, tc.expectErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, sk1, sk)
			}
		})
	}
}

func TestShardSelectorCache(t *testing.T) {
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	pieceStore := mock.NewMockPieceStore(ctrl)
	sectorAccessor := mock.NewMockSectorAccessor(ctrl)
	retrievalProv := mock.NewMockRetrievalProvider(ctrl)
	ss, err := NewShardSelector(ctx, pieceStore, sectorAccessor, retrievalProv)
	require.NoError(t, err)

	blockCid := testutil.GenerateCid()
	require.NoError(t, err)
	sk1cid := testutil.GenerateCid()
	sk1 := shard.KeyFromCID(sk1cid)
	shards := []shard.Key{sk1}

	pi := piecestore.PieceInfo{
		PieceCID: testutil.GenerateCid(),
		Deals:    []piecestore.DealInfo{{SectorID: 1}},
	}
	pieceStore.EXPECT().GetPieceInfo(sk1cid).
		// Expect there to be only one call to GetPieceInfo because after the first
		// call the result should be cached
		MinTimes(1).MaxTimes(1).
		Return(pi, nil)

	sectorAccessor.EXPECT().IsUnsealed(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(true, nil)

	ask := retrievalmarket.Ask{PricePerByte: abi.NewTokenAmount(0)}
	retrievalProv.EXPECT().GetDynamicAsk(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(ask, nil)

	var wg errgroup.Group
	for i := 0; i < 100; i++ {
		wg.Go(func() error {
			_, err := ss.ShardSelectorF(blockCid, shards)
			return err
		})
	}
	require.NoError(t, wg.Wait())
}
