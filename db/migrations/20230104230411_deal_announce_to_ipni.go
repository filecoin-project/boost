package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upSetdealsAnnounceToIPNI, downSetdealsAnnounceToIPNI)
}

func upSetdealsAnnounceToIPNI(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "UPDATE Deals SET AnnounceToIPNI=?;", true)
	if err != nil {
		return err
	}
	return nil
}

func downSetdealsAnnounceToIPNI(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
