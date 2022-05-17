package migrations

import (
	"database/sql"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upSetdealsretry, downSetdealsretry)
}

func upSetdealsretry(tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE Deals SET Retry=?;", types.DealRetryAuto)
	if err != nil {
		return err
	}
	_, err = tx.Exec("UPDATE Deals SET Retry=? WHERE Checkpoint='Complete' AND Error != '';", types.DealRetryFatal)
	if err != nil {
		return err
	}
	return nil
}

func downSetdealsretry(tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
