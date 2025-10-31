package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(UpSetStorageTaggedTransferHost, DownSetStorageTaggedTransferHost)
}

func UpSetStorageTaggedTransferHost(ctx context.Context, tx *sql.Tx) error {
	errPrefix := "setting StorageTagged.TransferHost: "
	qry := "SELECT Deals.ID, Deals.TransferType, Deals.TransferParams " +
		"FROM Deals INNER JOIN StorageTagged " +
		"ON Deals.ID = StorageTagged.DealUUID"
	rows, err := tx.QueryContext(ctx, qry)
	if err != nil {
		return fmt.Errorf(errPrefix+"getting deals from DB: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var id string
		var xferType string
		var params []byte
		err := rows.Scan(&id, &xferType, &params)
		if err != nil {
			return fmt.Errorf(errPrefix+"scanning Deals row: %w", err)
		}

		dealErrPrefix := fmt.Sprintf(errPrefix+"deal %s: ", id)

		xfer := types.Transfer{
			Type:   xferType,
			Params: params,
		}
		host, err := xfer.Host()
		if err != nil {
			log.Warnw(dealErrPrefix+"ignoring - couldn't parse transfer params %s: '%s': %s", xferType, params, err)
			continue
		}

		_, err = tx.ExecContext(ctx, "UPDATE StorageTagged SET TransferHost = ? WHERE DealUUID = ?", host, id)
		if err != nil {
			return fmt.Errorf(dealErrPrefix+"saving TransferHost to DB: %w", err)
		}
	}
	return rows.Err()
}

func DownSetStorageTaggedTransferHost(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	// Do nothing because sqlite doesn't support removing a column.
	return nil
}
