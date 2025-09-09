package filters

import (
	"context"
	"testing"

	"github.com/filecoin-project/boost/cmd/lib/filters/mock"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/lotus/blockstore"
	"github.com/golang/mock/gomock"
	ds "github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/require"
)

func TestFilteredBlockstore(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	filter := mock.NewMockFilter(ctrl)
	bs := blockstore.FromDatastore(ds.NewMapDatastore())
	fbs := NewFilteredBlockstore(bs, filter)

	blks := testutil.GenerateBlocksOfSize(1, 1024)
	blk := blks[0]
	err := bs.Put(ctx, blk)
	require.NoError(t, err)

	t.Run("Has", func(t *testing.T) {
		// Filter accepts cid
		filter.EXPECT().FulfillRequest(gomock.Any(), blk.Cid()).Return(true, nil)
		ok, err := fbs.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.True(t, ok)

		// Filter rejects cid
		filter.EXPECT().FulfillRequest(gomock.Any(), blk.Cid()).Return(false, nil)
		ok, err = fbs.Has(ctx, blk.Cid())
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("Get", func(t *testing.T) {
		// Filter accepts cid
		filter.EXPECT().FulfillRequest(gomock.Any(), blk.Cid()).Return(true, nil)
		gotBlk, err := fbs.Get(ctx, blk.Cid())
		require.NoError(t, err)
		require.Equal(t, blk.RawData(), gotBlk.RawData())

		// Filter rejects cid
		filter.EXPECT().FulfillRequest(gomock.Any(), blk.Cid()).Return(false, nil)
		_, err = fbs.Get(ctx, blk.Cid())
		require.Error(t, err)
		require.ErrorIs(t, err, ipld.ErrNotFound{})
	})

	t.Run("GetSize", func(t *testing.T) {
		// Filter accepts cid
		filter.EXPECT().FulfillRequest(gomock.Any(), blk.Cid()).Return(true, nil)
		size, err := fbs.GetSize(ctx, blk.Cid())
		require.NoError(t, err)
		require.Equal(t, len(blk.RawData()), size)

		// Filter rejects cid
		filter.EXPECT().FulfillRequest(gomock.Any(), blk.Cid()).Return(false, nil)
		_, err = fbs.GetSize(ctx, blk.Cid())
		require.Error(t, err)
		require.ErrorIs(t, err, ipld.ErrNotFound{})
	})
}
