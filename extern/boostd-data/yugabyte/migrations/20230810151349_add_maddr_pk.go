package migrations

import (
	"context"
	"database/sql"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upPieceTrackerAddPk, downPieceTrackerAddPk)
}

// Add a primary key constraint to the PieceTracker & PieceFlagged tables
func upPieceTrackerAddPk(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "ALTER TABLE PieceTracker ADD CONSTRAINT piecetracker_pkey PRIMARY KEY (MinerAddr, PieceCid)")
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged ADD CONSTRAINT pieceflagged_pkey PRIMARY KEY (MinerAddr, PieceCid)")
	if err != nil {
		return err
	}

	return nil
}

func downPieceTrackerAddPk(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "ALTER TABLE PieceTracker DROP CONSTRAINT piecetracker_pkey")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged DROP CONSTRAINT pieceflagged_pkey")
	if err != nil {
		return err
	}
	return nil
}
