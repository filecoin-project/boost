package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"unicode/utf8"

	"github.com/filecoin-project/boost/db/fielddef"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(UpDealsLabelV8, DownDealsLabelV8)
}

// Change the deal label format from a string to a marshalled DealLabel
func UpDealsLabelV8(ctx context.Context, tx *sql.Tx) error {
	errPrefix := "migrating deal label up from string to DealLabel: "
	rows, err := tx.QueryContext(ctx, "SELECT ID, Label from Deals WHERE Label IS NOT NULL")
	if err != nil {
		return fmt.Errorf(errPrefix+"getting deals from DB: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var id string
		var label string
		err := rows.Scan(&id, &label)
		if err != nil {
			return fmt.Errorf(errPrefix+"scanning Deals row: %w", err)
		}

		dealErrPrefix := fmt.Sprintf(errPrefix+"deal %s: ", id)
		var marshalled interface{}
		if utf8.Valid([]byte(label)) {
			l, err := market.NewLabelFromString(label)
			if err != nil {
				return fmt.Errorf(dealErrPrefix+"parsing label %s as string: %w", label, err)
			}

			marshalled, err = fielddef.LabelFieldDefMarshall(&l)
			if err != nil {
				return fmt.Errorf(dealErrPrefix+"marshalling DealLabel (string) '%s': %w", label, err)
			}
		} else {
			l, err := market.NewLabelFromBytes([]byte(label))
			if err != nil {
				return fmt.Errorf(dealErrPrefix+"parsing label as bytes: %w", err)
			}

			marshalled, err = fielddef.LabelFieldDefMarshall(&l)
			if err != nil {
				return fmt.Errorf(dealErrPrefix+"marshalling DealLabel (bytes): %w", err)
			}
		}
		_, err = tx.ExecContext(ctx, "UPDATE Deals SET Label = ? WHERE ID = ?", marshalled, id)
		if err != nil {
			return fmt.Errorf(dealErrPrefix+"saving DealLabel to DB: %w", err)
		}
	}
	return rows.Err()
}

// Change the deal label format from a marshalled DealLabel to a string
func DownDealsLabelV8(ctx context.Context, tx *sql.Tx) error {
	errPrefix := "migrating deal label down from DealLabel to string: "
	rows, err := tx.QueryContext(ctx, "SELECT ID, Label from Deals WHERE Label IS NOT NULL")
	if err != nil {
		return fmt.Errorf(errPrefix+"getting deals from DB: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	for rows.Next() {
		var id string
		var label sql.NullString
		err := rows.Scan(&id, &label)
		if err != nil {
			return fmt.Errorf(errPrefix+"scanning Deals row: %w", err)
		}

		dealErrPrefix := fmt.Sprintf(errPrefix+"deal %s: ", id)

		dealLabel, err := fielddef.LabelFieldDefUnmarshall(label)
		if err != nil {
			return fmt.Errorf(dealErrPrefix+"unmarshalling deal label %s as string: %w", label, err)
		}

		var asString string
		if dealLabel.IsString() {
			asString, err = dealLabel.ToString()
			if err != nil {
				return fmt.Errorf(dealErrPrefix+"converting deal label %s to string: %w", label, err)
			}
		} else {
			asBytes, err := dealLabel.ToBytes()
			if err != nil {
				return fmt.Errorf(dealErrPrefix+"converting deal label %s to bytes: %w", label, err)
			}
			asString = string(asBytes)
		}

		_, err = tx.ExecContext(ctx, "UPDATE Deals SET Label = ? WHERE ID = ?", asString, id)
		if err != nil {
			return fmt.Errorf(dealErrPrefix+"saving deal label string to DB: %w", err)
		}
	}
	return rows.Err()
}
