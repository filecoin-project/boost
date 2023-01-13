package migrations

import (
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upSetdealsAnnounceToIPNI, downSetdealsAnnounceToIPNI)
}

func upSetdealsAnnounceToIPNI(tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE Deals SET AnnounceToIPNI=?;", true)
	if err != nil {
		return err
	}
	return nil
}

func downSetdealsAnnounceToIPNI(tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
