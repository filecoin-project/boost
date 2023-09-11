package migrations

import (
	"bytes"
	"context"
	"database/sql"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upDealsPeeridBinaryToString, downDealsPeeridBinaryToString)
}

func upDealsPeeridBinaryToString(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, "SELECT ID, ClientPeerID FROM Deals")
	if err != nil {
		return err
	}
	for rows.Next() {
		var id string
		var pid []byte
		err := rows.Scan(&id, &pid)
		if err != nil {
			return err
		}

		updated, err := pidToString(pid)
		if err != nil {
			log.Warnf("could not migrate row with id %s: could not parse peer id %s: %w", id, string(pid), err)
			continue
		}

		_, err = tx.ExecContext(ctx, "UPDATE Deals SET ClientPeerID=? WHERE ID=?", updated, id)
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

func pidToString(input []byte) (*string, error) {
	if input == nil {
		return nil, nil
	}

	if bytes.Equal(input, []byte("")) {
		emptyStr := ""
		return &emptyStr, nil
	}

	if bytes.Equal(input, []byte("dummy")) {
		str := "dummy"
		return &str, nil
	}

	var pid peer.ID
	err := pid.UnmarshalBinary(input)
	if err != nil {
		err := pid.UnmarshalText(input)
		if err != nil {
			return nil, err
		}
	}

	updated := pid.String()
	return &updated, nil
}

func downDealsPeeridBinaryToString(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
