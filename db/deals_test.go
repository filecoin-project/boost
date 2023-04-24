package db

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/stretchr/testify/require"
)

func ToFilterOptions(filters map[string]interface{}) *FilterOptions {
	filter := &FilterOptions{}

	cp, ok := filters["Checkpoint"].(string)
	if ok {
		filter.Checkpoint = &cp
	}
	io, ok := filters["IsOffline"].(bool)
	if ok {
		filter.IsOffline = &io
	}
	tt, ok := filters["TransferType"].(string)
	if ok {
		filter.TransferType = &tt
	}
	vd, ok := filters["IsVerified"].(bool)
	if ok {
		filter.IsVerified = &vd
	}

	return filter
}

func TestDealsDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	require.NoError(t, CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))

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

	propnd, err := cborutil.AsIpld(&deal.ClientDealProposal)
	req.NoError(err)
	storedDealBySignedPropCid, err := db.BySignedProposalCID(ctx, propnd.Cid())
	req.NoError(err)
	req.Equal(deal.DealUuid, storedDealBySignedPropCid.DealUuid)

	dealList, err := db.List(ctx, "", nil, nil, 0, 0)
	req.NoError(err)
	req.Len(dealList, len(deals))

	limitedDealList, err := db.List(ctx, "", nil, nil, 1, 1)
	req.NoError(err)
	req.Len(limitedDealList, 1)
	req.Equal(dealList[1].DealUuid, limitedDealList[0].DealUuid)

	count, err := db.Count(ctx, "", nil)
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
	req.True(deal.IsOffline)

	finished, err := GenerateDeals()
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

func TestDealsDBSearch(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	req.NoError(CreateAllBoostTables(ctx, sqldb, sqldb))
	req.NoError(migrations.Migrate(sqldb))

	start := time.Now()
	db := NewDealsDB(sqldb)
	deals, err := GenerateNDeals(5)
	req.NoError(err)
	t.Logf("generated %d deals in %s", len(deals), time.Since(start))

	insertStart := time.Now()
	for _, deal := range deals {
		err := db.Insert(ctx, &deal)
		req.NoError(err)
	}
	t.Logf("inserted deals in %s", time.Since(insertStart))

	signedPropCid, err := deals[0].SignedProposalCid()
	req.NoError(err)

	label, err := deals[0].ClientDealProposal.Proposal.Label.ToString()
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
		name:   "Deal UUID",
		value:  deals[0].DealUuid.String(),
		filter: nil,
		count:  1,
	}, {
		name:   "piece CID",
		value:  deals[0].ClientDealProposal.Proposal.PieceCID.String(),
		filter: nil,
		count:  1,
	}, {
		name:   "client address",
		value:  deals[0].ClientDealProposal.Proposal.Client.String(),
		filter: nil,
		count:  1,
	}, {
		name:   "provider address",
		value:  deals[0].ClientDealProposal.Proposal.Provider.String(),
		filter: nil,
		count:  len(deals),
	}, {
		name:   "client peer ID",
		value:  deals[0].ClientPeerID.String(),
		filter: nil,
		count:  1,
	}, {
		name:   "deal data root",
		value:  deals[0].DealDataRoot.String(),
		filter: nil,
		count:  1,
	}, {
		name:   "publish CID",
		value:  deals[0].PublishCID.String(),
		filter: nil,
		count:  1,
	}, {
		name:   "signed proposal CID",
		value:  signedPropCid.String(),
		filter: nil,
		count:  1,
	}, {
		name:   "label",
		value:  label,
		filter: nil,
		count:  1,
	}, {
		name:  "filter out isOffline",
		value: "",
		filter: ToFilterOptions(map[string]interface{}{
			"IsOffline": false,
		}),
		count: 0,
	}, {
		name:  "filter isOffline",
		value: "",
		filter: ToFilterOptions(map[string]interface{}{
			"IsOffline": true,
		}),
		count: 5,
	}, {
		name:  "filter isOffline and IndexedAndAnnounced (in sealing)",
		value: "",
		filter: ToFilterOptions(map[string]interface{}{
			"IsOffline":  true,
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
				req.Equal(searchRes[0].DealUuid, deals[0].DealUuid)
			}
		})
	}
}

func TestWithSearchFilter(t *testing.T) {
	req := require.New(t)

	fo := ToFilterOptions(map[string]interface{}{
		"Checkpoint":      "Accepted",
		"IsOffline":       true,
		"NotAValidFilter": 123,
	})
	where, whereArgs := withSearchFilter(*fo)
	expectedArgs := []interface{}{
		"Accepted",
		true,
	}
	req.Equal("(Checkpoint = ? AND IsOffline = ?)", where)
	req.Equal(expectedArgs, whereArgs)

	fo = ToFilterOptions(map[string]interface{}{
		"IsOffline":       nil,
		"NotAValidFilter": nil,
	})
	where, whereArgs = withSearchFilter(*fo)

	req.Equal("", where)
	req.Equal(0, len(whereArgs))
}

func TestSqlDbBkp(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	require.NoError(t, CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))

	db := NewDealsDB(sqldb)
	deals, err := GenerateDeals()
	req.NoError(err)

	for _, deal := range deals {
		err = db.Insert(ctx, &deal)
		req.NoError(err)
	}

	f, err := os.CreateTemp(t.TempDir(), "*.db")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	dir := t.TempDir()
	err = SqlBackup(ctx, sqldb, dir, "test_db.db")
	require.NoError(t, err)

	bdb, err := SqlDB(path.Join(dir, "test_db.db"))
	require.NoError(t, err)

	bkdb := NewDealsDB(bdb)

	dealList, err := bkdb.List(ctx, "", nil, nil, 0, 0)
	req.NoError(err)
	req.Len(dealList, len(deals))

}
