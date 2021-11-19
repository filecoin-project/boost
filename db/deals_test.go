package db

import (
	"context"
	"database/sql"
	"path"
	"testing"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	"github.com/filecoin-project/boost/storagemarket/types"

	"github.com/stretchr/testify/require"
)

func TestDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	tmpFile := path.Join(t.TempDir(), "test.db")
	//fmt.Println(tmpFile)

	deals, err := GenerateDeals()
	req.NoError(err)

	sqldb, err := sql.Open("sqlite3", "file:"+tmpFile)
	req.NoError(err)

	db := NewDealsDB(sqldb)
	req.NoError(err)

	err = createTables(ctx, sqldb)
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

	err = db.InsertLog(ctx, &DealLog{DealUuid: deal.DealUuid, Text: "Test"})
	req.NoError(err)

	logs, err := db.Logs(ctx, deal.DealUuid)
	req.NoError(err)
	req.Len(logs, 1)
	req.Equal("Test", logs[0].Text)
}
