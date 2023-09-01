package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upSetcheckpointattounix0, downSetcheckpointattounix0)
}

func upSetcheckpointattounix0(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "UPDATE Deals SET CheckpointAt=datetime(0, 'unixepoch');")
	if err != nil {
		return err
	}
	return nil
}

func downSetcheckpointattounix0(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
