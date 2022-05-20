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

	dealList, err := db.List(ctx, "", nil, 0, 0)
	req.NoError(err)
	req.Len(dealList, len(deals))

	limitedDealList, err := db.List(ctx, "", nil, 1, 1)
	req.NoError(err)
	req.Len(limitedDealList, 1)
	req.Equal(dealList[1].DealUuid, limitedDealList[0].DealUuid)

	count, err := db.Count(ctx, "")
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
	req.NoError(Migrate(sqldb))

	start := time.Now()
	db := NewDealsDB(sqldb)
	deals, err := generateDeals(5)
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

	tcs := []struct {
		name  string
		value string
		count int
	}{{
		name:  "search error",
		value: "data-transfer failed",
		count: 1,
	}, {
		name:  "search error with padding",
		value: "  data-transfer failed\n\t ",
		count: 1,
	}, {
		name:  "Deal UUID",
		value: deals[0].DealUuid.String(),
		count: 1,
	}, {
		name:  "piece CID",
		value: deals[0].ClientDealProposal.Proposal.PieceCID.String(),
		count: 1,
	}, {
		name:  "client address",
		value: deals[0].ClientDealProposal.Proposal.Client.String(),
		count: 1,
	}, {
		name:  "provider address",
		value: deals[0].ClientDealProposal.Proposal.Provider.String(),
		count: len(deals),
	}, {
		name:  "client peer ID",
		value: deals[0].ClientPeerID.String(),
		count: 1,
	}, {
		name:  "deal data root",
		value: deals[0].DealDataRoot.String(),
		count: 1,
	}, {
		name:  "publish CID",
		value: deals[0].PublishCID.String(),
		count: 1,
	}, {
		name:  "signed proposal CID",
		value: signedPropCid.String(),
		count: 1,
	}, {
		name:  "label",
		value: deals[0].ClientDealProposal.Proposal.Label,
		count: 1,
	}}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			count, err := db.Count(ctx, tc.value)
			req.NoError(err)
			req.Equal(tc.count, count)

			searchStart := time.Now()
			searchRes, err := db.List(ctx, tc.value, nil, 0, 0)
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
