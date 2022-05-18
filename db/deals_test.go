package db

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/stretchr/testify/require"
)

func TestDealsDB(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := CreateTestTmpDB(t)
	require.NoError(t, CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, Migrate(sqldb))

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

	dealList, err := db.List(ctx, nil, 0, 0)
	req.NoError(err)
	req.Len(dealList, len(deals))

	limitedDealList, err := db.List(ctx, nil, 1, 1)
	req.NoError(err)
	req.Len(limitedDealList, 1)
	req.Equal(dealList[1].DealUuid, limitedDealList[0].DealUuid)

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
	require.NoError(t, CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, Migrate(sqldb))

	db := NewDealsDB(sqldb)
	deals, err := GenerateDeals()
	req.NoError(err)

	for _, deal := range deals {
		err = db.Insert(ctx, &deal)
		req.NoError(err)
	}

	tcs := []struct {
		name  string
		value string
	}{{
		name:  "PieceCID",
		value: deals[0].ClientDealProposal.Proposal.PieceCID.String(),
	}}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			searchRes, err := db.Search(ctx, tc.value, nil, 0, 0)
			req.NoError(err)
			require.Len(t, searchRes, 1)
			require.Equal(t, searchRes[0].DealUuid, deals[0].DealUuid)
		})
	}
}
