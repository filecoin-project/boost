package migrations

import (
	"context"
	"database/sql"

	"github.com/filecoin-project/go-address"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upDealsAddrBinaryToString, downDealsAddrBinaryToString)
}

// Convert format of deals in the database from binary to string
func upDealsAddrBinaryToString(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, "SELECT ID, ClientAddress, ProviderAddress FROM Deals")
	if err != nil {
		return err
	}
	for rows.Next() {
		var id string
		var clientAddr []byte
		var providerAddr []byte
		err := rows.Scan(&id, &clientAddr, &providerAddr)
		if err != nil {
			return err
		}

		updatedClientAddr, err := addrToString(clientAddr)
		if err != nil {
			log.Warnf("could not migrate row with id %s: could not parse client address %s: %w", id, string(clientAddr), err)
			continue
		}

		updatedProviderAddr, err := addrToString(providerAddr)
		if err != nil {
			log.Warnf("could not migrate row with id %s: could not parse provider address %s: %w", id, string(providerAddr), err)
			continue
		}

		_, err = tx.ExecContext(ctx, "UPDATE Deals SET ClientAddress=?, ProviderAddress=? WHERE ID=?", updatedClientAddr, updatedProviderAddr, id)
		if err != nil {
			log.Warnf("could not migrate row with id %s: could not save row: %w", id, err)
			continue
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func addrToString(input []byte) (*string, error) {
	if input == nil {
		return nil, nil
	}
	addr, err := address.NewFromBytes(input)
	if err != nil {
		addr, err = address.NewFromString(string(input))
		if err != nil {
			return nil, err
		}
	}

	updated := addr.String()
	return &updated, nil
}

func downDealsAddrBinaryToString(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
