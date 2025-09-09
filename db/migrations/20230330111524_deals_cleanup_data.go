package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upDealsCleanupData, downDealsCleanupData)
}

func upDealsCleanupData(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "UPDATE Deals SET CleanupData = NOT IsOffline")
	if err != nil {
		return err
	}
	return nil
}

func downDealsCleanupData(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
