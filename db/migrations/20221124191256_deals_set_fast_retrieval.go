package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upSetdealsfastretrieval, downSetdealsfastretrieval)
}

func upSetdealsfastretrieval(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "UPDATE Deals SET FastRetrieval=?;", true)
	if err != nil {
		return err
	}
	return nil
}

func downSetdealsfastretrieval(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
