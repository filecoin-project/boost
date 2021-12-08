package storagemanager

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/node/repo"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("storagemanager")

var (
	StagingAreaDirName = "deal-staging"
)

type Config struct {
	MaxStagingDealsBytes uint64
}

type StorageManager struct {
	lr                 repo.LockedRepo
	db                 *db.StorageDB
	cfg                Config
	StagingAreaDirPath string
}

func New(cfg Config) func(lr repo.LockedRepo, sqldb *sql.DB) *StorageManager {
	return func(lr repo.LockedRepo, sqldb *sql.DB) *StorageManager {
		return &StorageManager{
			db:                 db.NewStorageDB(sqldb),
			cfg:                cfg,
			lr:                 lr,
			StagingAreaDirPath: filepath.Join(lr.Path(), StagingAreaDirName),
		}
	}
}

// Free
func (m *StorageManager) Free(ctx context.Context) (uint64, error) {
	m.Lock()
	defer m.Unlock()

	// Get the total tagged storage, so that we know how much is available.
	tagged, err := m.totalTagged(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting total tagged: %w", err)
	}

	if m.cfg.MaxStagingDealsBytes != 0 {
		return m.cfg.MaxStagingDealsBytes - tagged, nil
	} else {
		// we don't have a max staging area configured, so
		// return the actual free disk space

		s, err := m.lr.Stat(m.StagingAreaDirPath)
		if err != nil {
			return 0, err
		}

		return uint64(s.Available), nil
	}
}

// Tag
func (m *StorageManager) Tag(ctx context.Context, dealUuid uuid.UUID, pieceSize abi.PaddedPieceSize) error {
	// Get the total tagged storage, so that we know how much is available.
	tagged, err := m.totalTagged(ctx)
	if err != nil {
		return fmt.Errorf("getting total tagged: %w", err)
	}

	if m.cfg.MaxStagingDealsBytes != 0 {
		if tagged+uint64(pieceSize) >= m.cfg.MaxStagingDealsBytes {
			return fmt.Errorf("cannot accept piece of size %d, on top of already allocated %d bytes, because it would exceed max staging area size %d", uint64(pieceSize), uint64(tagged), m.cfg.MaxStagingDealsBytes)
		}
	}

	err = m.persistTagged(ctx, dealUuid, uint64(pieceSize))
	if err != nil {
		return fmt.Errorf("saving total tagged storage: %w", err)
	}

	return nil
}

// Untag
func (m *StorageManager) Untag(ctx context.Context, dealUuid uuid.UUID) error {
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

func (m *StorageManager) TotalTagged(ctx context.Context) (uint64, error) {
	m.RLock()
	defer m.RUnlock()

	return m.totalTagged(ctx)
}

// unlocked
func (m *StorageManager) totalTagged(ctx context.Context) (uint64, error) {
	total, err := m.db.TotalTagged(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting total tagged from DB: %w", err)
	}
	return total, nil
}

func (m *StorageManager) persistTagged(ctx context.Context, dealUuid uuid.UUID, pieceSize uint64) error {
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
