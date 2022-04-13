package storagemanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/filecoin-project/boost/db"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("storagemanager")

var (
	StagingAreaDirName = "incoming"
)

type Config struct {
	MaxStagingDealsBytes uint64
}

type StorageManager struct {
	lr                 lotus_repo.LockedRepo
	db                 *db.StorageDB
	cfg                Config
	StagingAreaDirPath string
}

func New(cfg Config) func(lr lotus_repo.LockedRepo, sqldb *sql.DB) *StorageManager {
	return func(lr lotus_repo.LockedRepo, sqldb *sql.DB) *StorageManager {
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
	// Get the total tagged storage, so that we know how much is available.
	tagged, err := m.TotalTagged(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting total tagged: %w", err)
	}

	return m.cfg.MaxStagingDealsBytes - tagged, nil
}

// ErrNoSpaceLeft indicates that there is insufficient storage to accept a deal
var ErrNoSpaceLeft = errors.New("no space left")

// Tags storage space for the deal.
// If there is not enough space left, returns ErrNoSpaceLeft.
func (m *StorageManager) Tag(ctx context.Context, dealUuid uuid.UUID, size uint64) error {
	// Get the total tagged storage, so that we know how much is available.
	log.Debugw("tagging", "id", dealUuid, "size", size, "maxbytes", m.cfg.MaxStagingDealsBytes)

	tagged, err := m.TotalTagged(ctx)
	if err != nil {
		return fmt.Errorf("getting total tagged: %w", err)
	}

	if m.cfg.MaxStagingDealsBytes != 0 {
		if tagged+size >= m.cfg.MaxStagingDealsBytes {
			err := fmt.Errorf("%w: cannot accept piece of size %d, on top of already allocated %d bytes, because it would exceed max staging area size %d",
				ErrNoSpaceLeft, size, tagged, m.cfg.MaxStagingDealsBytes)
			return err
		}
	}

	err = m.persistTagged(ctx, dealUuid, size)
	if err != nil {
		return fmt.Errorf("saving total tagged storage: %w", err)
	}

	return nil
}

// Untag
func (m *StorageManager) Untag(ctx context.Context, dealUuid uuid.UUID) error {
	size, err := m.db.Untag(ctx, dealUuid)
	if err != nil {
		return fmt.Errorf("persisting untag storage for deal to DB: %w", err)
	}

	storageLog := &db.StorageLog{
		DealUUID:     dealUuid,
		Text:         "Untag staging storage for deal",
		TransferSize: size,
	}
	err = m.db.InsertLog(ctx, storageLog)
	if err != nil {
		return fmt.Errorf("persisting untag storage log to DB: %w", err)
	}

	log.Infow("untag storage", "id", dealUuid, "size", size)
	return nil
}

func (m *StorageManager) TotalTagged(ctx context.Context) (uint64, error) {
	total, err := m.db.TotalTagged(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting total tagged from DB: %w", err)
	}
	return total, nil
}

func (m *StorageManager) persistTagged(ctx context.Context, dealUuid uuid.UUID, size uint64) error {
	err := m.db.Tag(ctx, dealUuid, size)
	if err != nil {
		return fmt.Errorf("persisting tagged storage for deal to DB: %w", err)
	}

	storageLog := &db.StorageLog{
		DealUUID:     dealUuid,
		TransferSize: size,
		Text:         "Tag staging storage",
	}
	err = m.db.InsertLog(ctx, storageLog)
	if err != nil {
		return fmt.Errorf("persisting tag storage log to DB: %w", err)
	}

	log.Infow("tag storage", "id", dealUuid, "size", size)
	return nil
}
