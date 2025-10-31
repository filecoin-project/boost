package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/db/fielddef"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/google/uuid"
)

type StorageLog struct {
	DealUUID     uuid.UUID
	CreatedAt    time.Time
	TransferSize uint64
	Text         string
}

type StorageDB struct {
	db *sql.DB
}

func NewStorageDB(db *sql.DB) *StorageDB {
	return &StorageDB{db: db}
}

func (s *StorageDB) Tag(ctx context.Context, dealUuid uuid.UUID, size uint64, host string) error {
	qry := "INSERT INTO StorageTagged (DealUUID, CreatedAt, TransferSize, TransferHost) "
	qry += "VALUES (?, ?, ?, ?)"
	values := []interface{}{dealUuid, time.Now(), fmt.Sprintf("%d", size), host}
	_, err := s.db.ExecContext(ctx, qry, values...)
	return err
}

func (s *StorageDB) Untag(ctx context.Context, dealUuid uuid.UUID) (uint64, error) {
	qry := "SELECT TransferSize FROM StorageTagged WHERE DealUUID = ?"
	row := s.db.QueryRowContext(ctx, qry, dealUuid)

	ps := &fielddef.BigIntFieldDef{F: new(big.Int)}
	err := row.Scan(&ps.Marshalled)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("getting untagged amount: %w", err)
	}
	err = ps.Unmarshall()
	if err != nil {
		return 0, fmt.Errorf("unmarshalling untagged TransferSize")
	}

	_, err = s.db.ExecContext(ctx, "DELETE FROM StorageTagged WHERE DealUUID = ?", dealUuid)
	return (*ps.F).Uint64(), err
}

func (s *StorageDB) InsertLog(ctx context.Context, logs ...*StorageLog) error {
	now := time.Now()
	for _, l := range logs {
		if l.CreatedAt.IsZero() {
			l.CreatedAt = now
		}

		qry := "INSERT INTO StorageLogs (DealUUID, CreatedAt, TransferSize, LogText) "
		qry += "VALUES (?, ?, ?, ?)"
		values := []interface{}{l.DealUUID, l.CreatedAt, fmt.Sprintf("%d", l.TransferSize), l.Text}
		_, err := s.db.ExecContext(ctx, qry, values...)
		if err != nil {
			return fmt.Errorf("inserting storage log: %w", err)
		}
	}

	return nil
}

func (s *StorageDB) Logs(ctx context.Context) ([]StorageLog, error) {
	qry := "SELECT DealUUID, CreatedAt, TransferSize, LogText FROM StorageLogs"
	rows, err := s.db.QueryContext(ctx, qry)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	storageLogs := make([]StorageLog, 0, 16)
	for rows.Next() {
		ps := &fielddef.BigIntFieldDef{F: new(big.Int)}

		var storageLog StorageLog
		err := rows.Scan(
			&storageLog.DealUUID,
			&storageLog.CreatedAt,
			&ps.Marshalled,
			&storageLog.Text)

		if err != nil {
			return nil, err
		}

		err = ps.Unmarshall()
		if err != nil {
			return nil, fmt.Errorf("unmarshalling TransferSize: %w", err)
		}

		storageLog.TransferSize = (*ps.F).Uint64()
		storageLogs = append(storageLogs, storageLog)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return storageLogs, nil
}

func (s *StorageDB) TotalTaggedForHost(ctx context.Context, host string) (uint64, error) {
	return s.totalTagged(ctx, host)
}

func (s *StorageDB) TotalTagged(ctx context.Context) (uint64, error) {
	return s.totalTagged(ctx, "")
}

func (s *StorageDB) totalTagged(ctx context.Context, host string) (uint64, error) {
	qry := "SELECT TransferSize FROM StorageTagged"
	var args []interface{}
	if host != "" {
		qry += " WHERE TransferHost = ?"
		args = append(args, host)
	}
	rows, err := s.db.QueryContext(ctx, qry, args...)
	if err != nil {
		return 0, fmt.Errorf("getting total tagged: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	total := big.NewIntUnsigned(0)

	for rows.Next() {
		val := &fielddef.BigIntFieldDef{F: new(big.Int)}
		err := rows.Scan(&val.Marshalled)
		if err != nil {
			return 0, fmt.Errorf("getting TransferSize: %w", err)
		}

		err = val.Unmarshall()
		if err != nil {
			return 0, fmt.Errorf("unmarshalling untagged TransferSize: %w", err)
		}
		if val.F.Int != nil {
			total = big.Add(total, *val.F)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("getting total tagged: %w", err)
	}

	return total.Uint64(), nil
}
