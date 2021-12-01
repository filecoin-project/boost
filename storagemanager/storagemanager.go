package storagemanager

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("storagemanager")

type Config struct {
	MaxStagingDealsBytes uint64
}

type StorageManager struct {
	db  *db.StorageDB
	cfg Config

	lk sync.RWMutex
}

func New(cfg Config) func(sqldb *sql.DB) *StorageManager {
	return func(sqldb *sql.DB) *StorageManager {
		return &StorageManager{
			db:  db.NewStorageDB(sqldb),
			cfg: cfg,
		}
	}
}

// Tag
func (m *StorageManager) Tag(ctx context.Context, dealUuid uuid.UUID, pieceSize abi.PaddedPieceSize) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	// Check that the provider has enough funds in escrow to cover the
	// collateral requirement for the deal
	tagged, err := m.totalTagged(ctx)
	if err != nil {
		return fmt.Errorf("getting total tagged: %w", err)
	}

	if m.cfg.MaxStagingDealsBytes != 0 {
		if uint64(tagged)+uint64(pieceSize) >= m.cfg.MaxStagingDealsBytes {
			return fmt.Errorf("miner overloaded, staging area is full")
		}
	}

	err = m.persistTagged(ctx, dealUuid, pieceSize)
	if err != nil {
		return fmt.Errorf("saving total tagged storage: %w", err)
	}

	return nil
}

// Untag
func (m *StorageManager) Untag(ctx context.Context, dealUuid uuid.UUID) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	pieceSize, err := m.db.Untag(ctx, dealUuid)
	if err != nil {
		return fmt.Errorf("persisting untag storage for deal to DB: %w", err)
	}

	storageLog := &db.StorageLog{
		DealUUID:  dealUuid,
		Text:      "Untag staging storage for deal",
		PieceSize: pieceSize,
	}
	err = m.db.InsertLog(ctx, storageLog)
	if err != nil {
		return fmt.Errorf("persisting untag storage log to DB: %w", err)
	}

	log.Infow("untag storage", "id", dealUuid, "pieceSize", pieceSize)
	return nil
}

// unlocked
func (m *StorageManager) totalTagged(ctx context.Context) (abi.PaddedPieceSize, error) {
	total, err := m.db.TotalTagged(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting total tagged from DB: %w", err)
	}
	return total, nil
}

func (m *StorageManager) persistTagged(ctx context.Context, dealUuid uuid.UUID, pieceSize abi.PaddedPieceSize) error {
	err := m.db.Tag(ctx, dealUuid, pieceSize)
	if err != nil {
		return fmt.Errorf("persisting tagged storage for deal to DB: %w", err)
	}

	storageLog := &db.StorageLog{
		DealUUID:  dealUuid,
		PieceSize: pieceSize,
		Text:      "Tag staging storage",
	}
	err = m.db.InsertLog(ctx, storageLog)
	if err != nil {
		return fmt.Errorf("persisting tag storage log to DB: %w", err)
	}

	log.Infow("tag storage", "id", dealUuid, "pieceSize", pieceSize)
	return nil
}
