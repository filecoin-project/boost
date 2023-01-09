package migrations_tests

import (
	"context"
	"testing"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

func TestDealFastRetrieval(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	sqldb := db.CreateTestTmpDB(t)
	req.NoError(db.CreateAllBoostTables(ctx, sqldb, sqldb))

	// Run migrations up to the one that adds the FastRetrieval field to Deals
	goose.SetBaseFS(migrations.EmbedMigrations)
	req.NoError(goose.SetDialect("sqlite3"))
	req.NoError(goose.UpTo(sqldb, ".", 20221124191002))

	// Generate 2 deals
	dealsDB := db.NewDealsDB(sqldb)
	deals, err := db.GenerateNDeals(1)
	req.NoError(err)

	// Insert the deals in DB
	err = dealsDB.Insert(ctx, &deals[0])
	require.NoError(t, err)

	// Get deal state
	dealState, err := dealsDB.ByID(ctx, deals[0].DealUuid)
	require.NoError(t, err)
	require.False(t, dealState.FastRetrieval)

	//Run migration
	req.NoError(goose.UpByOne(sqldb, "."))

	// Check the deal state again
	dealState, err = dealsDB.ByID(ctx, deals[0].DealUuid)
	require.NoError(t, err)
	require.True(t, dealState.FastRetrieval)
}
