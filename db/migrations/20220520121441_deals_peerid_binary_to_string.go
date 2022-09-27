package migrations

import (
	"bytes"
	"database/sql"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigration(upDealsPeeridBinaryToString, downDealsPeeridBinaryToString)
}

func upDealsPeeridBinaryToString(tx *sql.Tx) error {
	rows, err := tx.Query("SELECT ID, ClientPeerID FROM Deals")
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

		_, err = tx.Exec("UPDATE Deals SET ClientPeerID=? WHERE ID=?", updated, id)
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

func downDealsPeeridBinaryToString(tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
