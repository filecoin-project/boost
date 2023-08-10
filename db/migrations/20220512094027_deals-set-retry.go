package migrations

import (
	"context"
	"database/sql"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upSetdealsretry, downSetdealsretry)
}

func upSetdealsretry(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "UPDATE Deals SET Retry=?;", types.DealRetryAuto)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE Deals SET Retry=? WHERE Checkpoint='Complete' AND Error != '';", types.DealRetryFatal)
	if err != nil {
		return err
	}
	return nil
}

func downSetdealsretry(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
