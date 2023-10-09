package db

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/stretchr/testify/require"
)

func TestDirectDealsDB(t *testing.T) {
	t.Skip("direct deals is disabled until it gets into a Filecoin network upgrade")

	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	require.NoError(t, CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))

	db := NewDirectDealsDB(sqldb)
	deals, err := GenerateDirectDeals()
	req.NoError(err)

	for _, deal := range deals {
		err = db.Insert(ctx, &deal)
		req.NoError(err)
	}

	deal := deals[0]
	storedDeal, err := db.ByID(ctx, deal.ID)
	req.NoError(err)

	// TODO: Work out why returned CreatedAt is not equal to stored CreatedAt
	//req.Equal(deal.CreatedAt.String(), storedDeal.CreatedAt.String())
	deal.CreatedAt = time.Time{}
	storedDeal.CreatedAt = time.Time{}
	req.Equal(deal, *storedDeal)

	dealList, err := db.List(ctx, "", nil, nil, 0, 0)
	req.NoError(err)
	req.Len(dealList, len(deals))

	limitedDealList, err := db.List(ctx, "", nil, nil, 1, 1)
	req.NoError(err)
	req.Len(limitedDealList, 1)
	req.Equal(dealList[1].ID, limitedDealList[0].ID)

	count, err := db.Count(ctx, "", nil)
	req.NoError(err)
	req.Equal(len(deals), count)

	var storedListDeal types.DirectDeal
	for _, dl := range dealList {
		if dl.ID == deal.ID {
			storedListDeal = *dl
		}
	}
	deal.CreatedAt = time.Time{}
	storedListDeal.CreatedAt = time.Time{}
	req.Equal(deal, storedListDeal)

	deal.Checkpoint = dealcheckpoints.AddedPiece
	err = db.Update(ctx, &deal)
	req.NoError(err)

	storedDeal, err = db.ByID(ctx, deal.ID)
	req.NoError(err)

	deal.CreatedAt = time.Time{}
	storedDeal.CreatedAt = time.Time{}
	req.Equal(deal, *storedDeal)

	finished, err := GenerateDirectDeals()
	require.NoError(t, err)
	for _, deal := range finished {
		deal.Checkpoint = dealcheckpoints.Complete
		err = db.Insert(ctx, &deal)
		req.NoError(err)
	}

	fds, err := db.ListCompleted(ctx)
	req.NoError(err)
	req.Len(fds, len(finished))
}

func TestDirectDealsDBSearch(t *testing.T) {
	t.Skip("direct deals is disabled until it gets into a Filecoin network upgrade")

	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	req.NoError(CreateAllBoostTables(ctx, sqldb, sqldb))
	req.NoError(migrations.Migrate(sqldb))

	start := time.Now()
	db := NewDirectDealsDB(sqldb)
	deals, err := GenerateDirectDeals()
	req.NoError(err)
	t.Logf("generated %d deals in %s", len(deals), time.Since(start))

	insertStart := time.Now()
	for _, deal := range deals {
		err := db.Insert(ctx, &deal)
		req.NoError(err)
	}
	t.Logf("inserted deals in %s", time.Since(insertStart))

	req.NoError(err)
	tcs := []struct {
		name   string
		value  string
		filter *FilterOptions
		count  int
	}{{
		name:   "search error",
		value:  "data-transfer failed",
		filter: nil,
		count:  1,
	}, {
		name:   "search error with padding",
		value:  "  data-transfer failed\n\t ",
		filter: nil,
		count:  1,
	}, {
		name:   "Deal ID",
		value:  deals[0].ID.String(),
		filter: nil,
		count:  1,
	}, {
		name:   "piece CID",
		value:  deals[0].PieceCID.String(),
		filter: nil,
		count:  1,
	}, {
		name:   "client address",
		value:  deals[0].Client.String(),
		filter: nil,
		count:  1,
	}, {
		name:   "provider address",
		value:  deals[0].Provider.String(),
		filter: nil,
		count:  len(deals),
	}, {
		name:  "filter for checkpoint Accepted",
		value: "",
		filter: ToFilterOptions(map[string]interface{}{
			"Checkpoint": dealcheckpoints.Accepted.String(),
		}),
		count: len(deals),
	}, {
		name:  "filter for checkpoint IndexedAndAnnounced (in sealing)",
		value: "",
		filter: ToFilterOptions(map[string]interface{}{
			"Checkpoint": dealcheckpoints.IndexedAndAnnounced.String(),
		}),
		count: 0,
	}}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			count, err := db.Count(ctx, tc.value, tc.filter)
			req.NoError(err)
			req.Equal(tc.count, count)

			searchStart := time.Now()
			searchRes, err := db.List(ctx, tc.value, tc.filter, nil, 0, 0)
			searchElapsed := time.Since(searchStart)
			req.NoError(err)
			req.Len(searchRes, tc.count)
			t.Logf("searched in %s", searchElapsed)
			if tc.count == 1 {
				req.Equal(searchRes[0].ID, deals[0].ID)
			}
		})
	}
}
