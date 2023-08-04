package migrations

import (
	"context"
	"database/sql"
	"errors"
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
// Add MinerAddr to the primary key constraint for each table.
// Set MinerAddr to the default miner address for the node (the miner
// address that the node stores data on).
func upAddMinerAddr(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "ALTER TABLE PieceTracker ADD COLUMN MinerAddr TEXT")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceTracker DROP CONSTRAINT piecetracker_pkey")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceTracker ADD CONSTRAINT piecetracker_pkey PRIMARY KEY (MinerAddr, PieceCid)")
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged ADD COLUMN MinerAddr TEXT")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged DROP CONSTRAINT pieceflagged_pkey")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged ADD CONSTRAINT pieceflagged_pkey PRIMARY KEY (MinerAddr, PieceCid)")
	if err != nil {
		return err
	}

	if migrationParams.MinerAddress == DisabledMinerAddr {
		return ErrMissingMinerAddr
	}

	minerAddr := migrationParams.MinerAddress.String()

	_, err = tx.ExecContext(ctx, "UPDATE PieceTracker SET MinerAddr=$1", minerAddr)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE PieceFlagged SET MinerAddr=$1", minerAddr)
	if err != nil {
		return err
	}

	return nil
}

func downAddMinerAddr(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "ALTER TABLE PieceTracker DROP CONSTRAINT piecetracker_pkey")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceTracker DROP COLUMN MinerAddr")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceTracker ADD CONSTRAINT piecetracker_pkey PRIMARY KEY (PieceCid)")
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged DROP CONSTRAINT pieceflagged_pkey")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged DROP COLUMN MinerAddr")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceFlagged ADD CONSTRAINT pieceflagged_pkey PRIMARY KEY (PieceCid)")
	if err != nil {
		return err
	}

	return nil
}
