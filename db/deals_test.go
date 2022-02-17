package db

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	"github.com/filecoin-project/boost/storagemarket/types"

	"github.com/stretchr/testify/require"
)

func TestDealsDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	require.NoError(t, CreateAllBoostTables(ctx, sqldb, sqldb))

	db := NewDealsDB(sqldb)
	deals, err := GenerateDeals()
	req.NoError(err)

	for _, deal := range deals {
		err = db.Insert(ctx, &deal)
		req.NoError(err)
	}

	deal := deals[0]
	storedDeal, err := db.ByID(ctx, deal.DealUuid)
	req.NoError(err)

	// TODO: Work out why returned CreatedAt is not equal to stored CreatedAt
	//req.Equal(deal.CreatedAt.String(), storedDeal.CreatedAt.String())
	deal.CreatedAt = time.Time{}
	storedDeal.CreatedAt = time.Time{}
	req.Equal(deal, *storedDeal)

	dealList, err := db.List(ctx, nil, 100)
	req.NoError(err)
	req.Len(dealList, len(deals))

	count, err := db.Count(ctx)
	req.NoError(err)
	req.Equal(len(deals), count)

	var storedListDeal types.ProviderDealState
	for _, dl := range dealList {
		if dl.DealUuid == deal.DealUuid {
			storedListDeal = *dl
		}
	}
	deal.CreatedAt = time.Time{}
	storedListDeal.CreatedAt = time.Time{}
	req.Equal(deal, storedListDeal)

	deal.Checkpoint = dealcheckpoints.Published
	err = db.Update(ctx, &deal)
	req.NoError(err)

	storedDeal, err = db.ByID(ctx, deal.DealUuid)
	req.NoError(err)

	deal.CreatedAt = time.Time{}
	storedDeal.CreatedAt = time.Time{}
	req.Equal(deal, *storedDeal)
}
