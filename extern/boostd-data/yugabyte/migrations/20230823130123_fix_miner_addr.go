package migrations

import (
	"context"
	"crypto/rand"
	"database/sql"

	"github.com/filecoin-project/go-address"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upPieceTrackerFixMaddr, downPieceTrackerFixMaddr)
}

// Convert test network miner addresses to mainnet miner address and vice-versa based on AddressNetwork
func upPieceTrackerFixMaddr(ctx context.Context, tx *sql.Tx) error {
	addr, err := makeRandomAddress()
	if err != nil {
		return err
	}

	if string(addr[0]) != address.TestnetPrefix {
		_, err := tx.ExecContext(ctx, "UPDATE idx.piecedeal SET MinerAddr = REPLACE(MinerAddr, 'f', 't') WHERE MinerAddr LIKE 'f%'")
		if err != nil {
			return err
		}
	}
	if string(addr[0]) != address.MainnetPrefix {
		_, err := tx.ExecContext(ctx, "UPDATE idx.piecedeal SET MinerAddr = REPLACE(MinerAddr, 't', 'f') WHERE MinerAddr LIKE 't%'")
		if err != nil {
			return err
		}
	}

	return nil
}

// Reverting above changes is not possible for selective columns
func downPieceTrackerFixMaddr(ctx context.Context, tx *sql.Tx) error {
	return nil
}

func makeRandomAddress() (string, error) {
	bytes := make([]byte, 32)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}

	addr, err := address.NewActorAddress(bytes)
	if err != nil {
		return "", err
	}

	return addr.String(), nil
}