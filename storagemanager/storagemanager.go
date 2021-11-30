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
}

type StorageManager struct {
	db *db.StorageDB

	lk sync.RWMutex
}

func New(cfg Config) func(sqldb *sql.DB) *StorageManager {
	return func(sqldb *sql.DB) *StorageManager {
		return &StorageManager{
			db: db.NewStorageDB(sqldb),
			//cfg: cfg,
		}
	}
}

// Tag
func (m *StorageManager) Tag(ctx context.Context, dealUuid uuid.UUID, pieceSize abi.PaddedPieceSize) error {
	m.lk.Lock()
	defer m.lk.Unlock()

	err := m.persistTagged(ctx, dealUuid, pieceSize)
	if err != nil {
		return fmt.Errorf("saving total tagged storage: %w", err)
	}

	return nil
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

	log.Infow("tag", "id", dealUuid, "piecesize", pieceSize)
	return nil
}
