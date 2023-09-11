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
	goose.AddMigrationContext(upAddPieceTrackerMinerAddr, downAddPieceTrackerMinerAddr)
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

// Add a MinerAddr column to the PieceTracker table.
// Set MinerAddr to the default miner address for the node (the miner
// address that the node stores data on).
//
// We should also add MinerAddr to the primary key constraint, however due to
// the way that transactions work in the migration library we have to do this in a
// separate migration (20230810151349_piece_tracker_add_maddr_pk)
func upAddPieceTrackerMinerAddr(ctx context.Context, tx *sql.Tx) error {
	// If the PieceTracker table has rows, we need to set the MinerAddr to
	// a default value. So if the user has not specified a miner address,
	// return an error
	if migrationParams.PieceTrackerHasRows && migrationParams.MinerAddress == DisabledMinerAddr {
		return ErrMissingMinerAddr
	}

	_, err := tx.ExecContext(ctx, "ALTER TABLE PieceTracker DROP CONSTRAINT piecetracker_pkey")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceTracker ADD COLUMN MinerAddr TEXT NOT NULL DEFAULT ''")
	if err != nil {
		return err
	}
	if migrationParams.PieceTrackerHasRows {
		minerAddr := migrationParams.MinerAddress.String()
		_, err = tx.ExecContext(ctx, fmt.Sprintf("UPDATE PieceTracker SET MinerAddr='%s'", minerAddr))
		if err != nil {
			return err
		}
	}

	// Note that due to the way that transactions work, we add the primary
	// key back in a later migration: 20230810151349_piece_tracker_add_maddr_pk

	return nil
}

func downAddPieceTrackerMinerAddr(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "ALTER TABLE PieceTracker DROP COLUMN MinerAddr")
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "ALTER TABLE PieceTracker ADD CONSTRAINT piecetracker_pkey PRIMARY KEY (PieceCid)")
	if err != nil {
		return err
	}
	return nil
}
