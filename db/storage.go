package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/google/uuid"
)

type StorageLog struct {
	DealUUID  uuid.UUID
	CreatedAt time.Time
	PieceSize abi.PaddedPieceSize
	Text      string
}

type StorageDB struct {
	db *sql.DB
}

func NewStorageDB(db *sql.DB) *StorageDB {
	return &StorageDB{db: db}
}

func (s *StorageDB) Tag(ctx context.Context, dealUuid uuid.UUID, pieceSize abi.PaddedPieceSize) error {
	ps := big.NewIntUnsigned(uint64(pieceSize))

	qry := "INSERT INTO StorageTagged (DealUUID, CreatedAt, PieceSize) "
	qry += "VALUES (?, ?, ?)"
	values := []interface{}{dealUuid, time.Now(), ps.String()}
	_, err := s.db.ExecContext(ctx, qry, values...)
	return err
}

func (s *StorageDB) Untag(ctx context.Context, dealUuid uuid.UUID) (abi.PaddedPieceSize, error) {
	qry := "SELECT PieceSize FROM StorageTagged WHERE DealUUID = ?"
	row := s.db.QueryRowContext(ctx, qry, dealUuid)

	ps := &bigIntFieldDef{f: new(big.Int)}
	err := row.Scan(&ps.marshalled)
	if err != nil {
		if err == sql.ErrNoRows {
			return abi.PaddedPieceSize(0), nil
		}
		return abi.PaddedPieceSize(0), fmt.Errorf("getting untagged amount: %w", err)
	}
	err = ps.unmarshall()
	if err != nil {
		return abi.PaddedPieceSize(0), fmt.Errorf("unmarshalling untagged PieceSize")
	}

	_, err = s.db.ExecContext(ctx, "DELETE FROM StorageTagged WHERE DealUUID = ?", dealUuid)
	return abi.PaddedPieceSize((*ps.f).Uint64()), err
}

func (s *StorageDB) InsertLog(ctx context.Context, logs ...*StorageLog) error {
	now := time.Now()
	for _, l := range logs {
		if l.CreatedAt.IsZero() {
			l.CreatedAt = now
		}

		ps := big.NewIntUnsigned(uint64(l.PieceSize))

		qry := "INSERT INTO StorageLogs (DealUUID, CreatedAt, PieceSize, LogText) "
		qry += "VALUES (?, ?, ?, ?)"
		values := []interface{}{l.DealUUID, l.CreatedAt, ps.String(), l.Text}
		_, err := s.db.ExecContext(ctx, qry, values...)
		if err != nil {
			return fmt.Errorf("inserting storage log: %w", err)
		}
	}

	return nil
}

func (s *StorageDB) Logs(ctx context.Context) ([]StorageLog, error) {
	qry := "SELECT DealUUID, CreatedAt, PieceSize, LogText FROM StorageLogs"
	rows, err := s.db.QueryContext(ctx, qry)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	storageLogs := make([]StorageLog, 0, 16)
	for rows.Next() {
		ps := &bigIntFieldDef{f: new(big.Int)}

		var storageLog StorageLog
		err := rows.Scan(
			&storageLog.DealUUID,
			&storageLog.CreatedAt,
			&ps.marshalled,
			&storageLog.Text)

		if err != nil {
			return nil, err
		}

		err = ps.unmarshall()
		if err != nil {
			return nil, fmt.Errorf("unmarshalling PieceSize: %w", err)
		}

		storageLog.PieceSize = abi.PaddedPieceSize((*ps.f).Uint64())
		storageLogs = append(storageLogs, storageLog)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return storageLogs, nil
}

func (s *StorageDB) TotalTagged(ctx context.Context) (abi.PaddedPieceSize, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT PieceSize FROM StorageTagged")
	if err != nil {
		return 0, fmt.Errorf("getting total tagged: %w", err)
	}
	defer rows.Close()

	total := big.NewIntUnsigned(0)

	for rows.Next() {
		val := &bigIntFieldDef{f: new(big.Int)}
		err := rows.Scan(&val.marshalled)
		if err != nil {
			return 0, fmt.Errorf("getting piece size: %w", err)
		}

		err = val.unmarshall()
		if err != nil {
			return 0, fmt.Errorf("unmarshalling untagged PieceSize: %w", err)
		}
		if val.f.Int != nil {
			total = big.Add(total, *val.f)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("getting total tagged: %w", err)
	}

	return abi.PaddedPieceSize(total.Uint64()), nil
}
