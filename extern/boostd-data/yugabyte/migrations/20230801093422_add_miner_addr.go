package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddMinerAddr, downAddMinerAddr)
}

// DisabledMinerAddr indicates that no miner address was set by the user.
// In that case, the migration will throw an error to remind the user to
// set it.
var DisabledMinerAddr address.Address

func init() {
	a, err := address.NewFromString("")
	if err != nil {
		panic(err)
	}
	DisabledMinerAddr = a
}

var ErrMissingMinerAddr = errors.New("cannot perform migration: miner address has not been set")

// Add a MinerAddr column to the PieceTracker and PieceFlagged tables.
// Set MinerAddr to the default miner address for the node (the miner
// address that the node stores data on).
//
// We should also add MinerAddr to the primary key constraint for each table,
// however due to a bug in the migration library we have to do this in a
// separate migration (20230810151349_piece_tracker_add_maddr_pk)
func upAddMinerAddr(ctx context.Context, tx *sql.Tx) error {
	if migrationParams.MinerAddress == DisabledMinerAddr {
		return ErrMissingMinerAddr
	}
	minerAddr := migrationParams.MinerAddress.String()

	_, err := tx.ExecContext(ctx, "ALTER TABLE PieceTracker DROP CONSTRAINT piecetracker_pkey")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceTracker ADD COLUMN MinerAddr TEXT NOT NULL DEFAULT ''")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf("UPDATE PieceTracker SET MinerAddr='%s'", minerAddr))
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged DROP CONSTRAINT pieceflagged_pkey")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged ADD COLUMN MinerAddr TEXT NOT NULL DEFAULT ''")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf("UPDATE PieceFlagged SET MinerAddr='%s'", minerAddr))
	if err != nil {
		return err
	}

	// Note that due to a bug in the migration library, we add the primary
	// key back in the next migration: 20230810151349_piece_tracker_add_maddr_pk

	return nil
}

func downAddMinerAddr(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "ALTER TABLE PieceTracker DROP COLUMN MinerAddr")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged DROP COLUMN MinerAddr")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceTracker ADD CONSTRAINT piecetracker_pkey PRIMARY KEY (PieceCid)")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged ADD CONSTRAINT pieceflagged_pkey PRIMARY KEY (PieceCid)")
	if err != nil {
		return err
	}
	return nil
}
