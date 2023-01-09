package migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upSetdealsfastretrieval, downSetdealsfastretrieval)
}

func upSetdealsfastretrieval(tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE Deals SET FastRetrieval=?;", true)
	if err != nil {
		return err
	}
	return nil
}

func downSetdealsfastretrieval(tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
