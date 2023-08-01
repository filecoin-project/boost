package migrations

import (
	"database/sql"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upAddMinerAddr, downAddMinerAddr)
}

func upAddMinerAddr(tx *sql.Tx) error {
	_, err := tx.Exec("ALTER TABLE PieceTracker ADD COLUMN MinerAddr TEXT")
	if err != nil {
		return err
	}
	_, err = tx.Exec("ALTER TABLE PieceFlagged ADD COLUMN MinerAddr TEXT")
	if err != nil {
		return err
	}

	minerAddr := migrationParams.MinerAddress.String()
	_, err = tx.Exec("UPDATE PieceTracker SET MinerAddr=?", minerAddr)
	if err != nil {
		return err
	}
	_, err = tx.Exec("UPDATE PieceFlagged SET MinerAddr=?", minerAddr)
	if err != nil {
		return err
	}

	return nil
}

func downAddMinerAddr(tx *sql.Tx) error {
	_, err := tx.Exec("ALTER TABLE PieceTracker DROP COLUMN MinerAddr")
	if err != nil {
		return err
	}
	_, err = tx.Exec("ALTER TABLE PieceFlagged DROP COLUMN MinerAddr")
	if err != nil {
		return err
	}
	return nil
}
