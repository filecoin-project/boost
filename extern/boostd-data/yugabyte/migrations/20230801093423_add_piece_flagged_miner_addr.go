package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddPieceFlaggedMinerAddr, downAddPieceFlaggedMinerAddr)
}

// Add a MinerAddr column to the PieceFlagged table.
// Set MinerAddr to the default miner address for the node (the miner
// address that the node stores data on).
//
// We should also add MinerAddr to the primary key constraint, however due to
// the way that transactions work in the migration library we have to do this in a
// separate migration (20230810151349_piece_tracker_add_maddr_pk)
func upAddPieceFlaggedMinerAddr(ctx context.Context, tx *sql.Tx) error {
	// If the PieceTracker table has rows, we need to set the MinerAddr to
	// a default value. So if the user has not specified a miner address,
	// return an error
	if migrationParams.PieceTrackerHasRows && migrationParams.MinerAddress == DisabledMinerAddr {
		return ErrMissingMinerAddr
	}

	_, err := tx.ExecContext(ctx, "ALTER TABLE PieceFlagged DROP CONSTRAINT pieceflagged_pkey")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged ADD COLUMN MinerAddr TEXT NOT NULL DEFAULT ''")
	if err != nil {
		return err
	}
	if migrationParams.PieceTrackerHasRows {
		minerAddr := migrationParams.MinerAddress.String()
		_, err = tx.ExecContext(ctx, fmt.Sprintf("UPDATE PieceFlagged SET MinerAddr='%s'", minerAddr))
		if err != nil {
			return err
		}
	}

	// Note that due to the way that transactions work, we add the primary
	// key back in a later migration: 20230810151349_piece_tracker_add_maddr_pk

	return nil
}

func downAddPieceFlaggedMinerAddr(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "ALTER TABLE PieceFlagged DROP COLUMN MinerAddr")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged ADD CONSTRAINT pieceflagged_pkey PRIMARY KEY (PieceCid)")
	if err != nil {
		return err
	}
	return nil
}
