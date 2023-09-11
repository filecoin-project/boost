package migrations_tests

import (
	"context"
	"testing"
	"unicode/utf8"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/stretchr/testify/require"
)

func TestDealsLabelv8(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := db.CreateTestTmpDB(t)
	require.NoError(t, db.CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))

	dealsDB := db.NewDealsDB(sqldb)
	deals, err := db.GenerateDeals()
	req.NoError(err)

	nonUtf8 := []byte{66, 250}
	require.False(t, utf8.Valid(nonUtf8))

	testCases := []struct {
		name             string
		label            string
		expectMarshalled string
	}{{
		name:             "empty string",
		label:            "",
		expectMarshalled: "'",
	}, {
		name:             "string",
		label:            "label",
		expectMarshalled: "'label",
	}, {
		name:             "string",
		label:            string(nonUtf8),
		expectMarshalled: "x42fa",
	}}

	for i, tc := range testCases {
		if utf8.Valid([]byte(tc.label)) {
			deals[i].ClientDealProposal.Proposal.Label, err = market.NewLabelFromString(tc.label)
		} else {
			deals[i].ClientDealProposal.Proposal.Label, err = market.NewLabelFromBytes([]byte(tc.label))
		}
		req.NoError(err)
	}

	for _, deal := range deals {
		err = dealsDB.Insert(ctx, &deal)
		req.NoError(err)
	}

	// Check that the label is marshalled into the expected string format
	for i, tc := range testCases {
		var label string
		err = sqldb.QueryRowContext(ctx, "SELECT Label FROM Deals WHERE ID = ?", deals[i].DealUuid).Scan(&label)
		req.NoError(err)
		req.Equal(tc.expectMarshalled, label)
	}

	// Migrate down
	tx, err := sqldb.BeginTx(ctx, nil)
	req.NoError(err)
	err = migrations.DownDealsLabelV8(ctx, tx)
	req.NoError(err)
	err = tx.Commit()
	req.NoError(err)

	// Check that after migrating down, the label is in the expected format
	for i, tc := range testCases {
		var label string
		err = sqldb.QueryRowContext(ctx, "SELECT Label FROM Deals WHERE ID = ?", deals[i].DealUuid).Scan(&label)
		req.NoError(err)
		req.Equal(tc.label, label)
	}

	// Migrate up
	tx, err = sqldb.BeginTx(ctx, nil)
	req.NoError(err)
	err = migrations.UpDealsLabelV8(ctx, tx)
	req.NoError(err)
	err = tx.Commit()
	req.NoError(err)

	// Check that after migrating back up, the label is in the expected format
	for i, tc := range testCases {
		var label string
		err = sqldb.QueryRowContext(ctx, "SELECT Label FROM Deals WHERE ID = ?", deals[i].DealUuid).Scan(&label)
		req.NoError(err)
		req.Equal(tc.expectMarshalled, label)
	}
}
