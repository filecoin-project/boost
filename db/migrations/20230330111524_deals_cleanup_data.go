package migrations

import (
	"database/sql"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upDealsCleanupData, downDealsCleanupData)
}

func upDealsCleanupData(tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE Deals SET CleanupData = NOT IsOffline")
	if err != nil {
		return err
	}
	return nil
}

func downDealsCleanupData(tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
