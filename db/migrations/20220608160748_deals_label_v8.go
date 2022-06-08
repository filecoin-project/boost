package migrations

import (
	"database/sql"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upDealsLabelV8, downDealsLabelV8)
}

// Add a ' character at the start of the Label (to indicate that it's a string, not a hex-encoded byte array)
func upDealsLabelV8(tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE Deals SET Label = '''' || Label WHERE Label IS NOT NULL")
	return err
}

// Remove the first character from the Label
func downDealsLabelV8(tx *sql.Tx) error {
	_, err := tx.Exec("UPDATE Deals SET Label = substr(Label, 2) WHERE Label IS NOT NULL")
	return err
}
