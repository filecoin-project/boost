package storagemanager

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
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
	MaxStagingDealsBytes          uint64
	MaxStagingDealsPercentPerHost uint64
}

type StorageManager struct {
	lr                 lotus_repo.LockedRepo
	db                 *db.StorageDB
	Cfg                Config
	StagingAreaDirPath string
}

func New(cfg Config) func(lr lotus_repo.LockedRepo, sqldb *sql.DB) (*StorageManager, error) {
	return func(lr lotus_repo.LockedRepo, sqldb *sql.DB) (*StorageManager, error) {
		if cfg.MaxStagingDealsPercentPerHost > 100 {
			return nil, fmt.Errorf("MaxStagingDealsPercentPerHost is %d but it must be a percentage between 0 - 100", cfg.MaxStagingDealsPercentPerHost)
		}

		stagingPath := filepath.Join(lr.Path(), StagingAreaDirName)
		err := os.MkdirAll(stagingPath, os.ModePerm)
		if err != nil {
			return nil, err
		}

		return &StorageManager{
			db:                 db.NewStorageDB(sqldb),
			Cfg:                cfg,
			lr:                 lr,
			StagingAreaDirPath: stagingPath,
		}, nil
	}
}

// Free
func (m *StorageManager) Free(ctx context.Context) (uint64, error) {
	// Get the total tagged storage, so that we know how much is available.
	tagged, err := m.TotalTagged(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting total tagged: %w", err)
	}

	//Return 0 if user sets this value to lower than currently occupied by deals
	if m.Cfg.MaxStagingDealsBytes < tagged {
		return 0, nil
	}

	return m.Cfg.MaxStagingDealsBytes - tagged, nil
}

// ErrNoSpaceLeft indicates that there is insufficient storage to accept a deal
var ErrNoSpaceLeft = errors.New("no space left")

// Tags storage space for the deal.
// If there is not enough space left, returns ErrNoSpaceLeft.
func (m *StorageManager) Tag(ctx context.Context, dealUuid uuid.UUID, size uint64, host string) error {
	// Get the total tagged storage, so that we know how much is available.
	log.Debugw("tagging", "id", dealUuid, "size", size, "host", host, "maxbytes", m.Cfg.MaxStagingDealsBytes)

	if m.Cfg.MaxStagingDealsBytes != 0 {
		if m.Cfg.MaxStagingDealsPercentPerHost != 0 {
			// Get the total amount tagged for download from the host
			tagged, err := m.TotalTaggedForHost(ctx, host)
			if err != nil {
				return fmt.Errorf("getting total tagged for host: %w", err)
			}

			// Check the amount tagged + the size of the proposed deal against the limit
			limit := (m.Cfg.MaxStagingDealsBytes * m.Cfg.MaxStagingDealsPercentPerHost) / 100
			if tagged+size >= limit {
				return fmt.Errorf("%w: cannot accept piece of size %d from host %s, "+
					"on top of already allocated %d bytes, because it would exceed max %d bytes: "+
					"staging area size %d x per host limit %d%%",
					ErrNoSpaceLeft, size, host, tagged, limit, m.Cfg.MaxStagingDealsBytes, m.Cfg.MaxStagingDealsPercentPerHost)
			}
		}

		// Get the total amount tagged for download from all hosts
		tagged, err := m.TotalTagged(ctx)
		if err != nil {
			return fmt.Errorf("getting total tagged: %w", err)
		}

		// Check the amount tagged + the size of the proposed deal against the limit
		if tagged+size >= m.Cfg.MaxStagingDealsBytes {
			err := fmt.Errorf("%w: cannot accept piece of size %d, on top of already allocated %d bytes, because it would exceed max staging area size %d",
				ErrNoSpaceLeft, size, tagged, m.Cfg.MaxStagingDealsBytes)
			return err
		}
	}

	err := m.persistTagged(ctx, dealUuid, size, host)
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

func (m *StorageManager) TotalTaggedForHost(ctx context.Context, host string) (uint64, error) {
	total, err := m.db.TotalTaggedForHost(ctx, host)
	if err != nil {
		return 0, fmt.Errorf("getting total tagged for host from DB: %w", err)
	}
	return total, nil
}

func (m *StorageManager) persistTagged(ctx context.Context, dealUuid uuid.UUID, size uint64, host string) error {
	err := m.db.Tag(ctx, dealUuid, size, host)
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

// DownloadFilePath creates a file in the download staging area for the deal
// with the given uuid
func (m *StorageManager) DownloadFilePath(dealUuid uuid.UUID) (string, error) {
	path := path.Join(m.StagingAreaDirPath, dealUuid.String()+".download")
	file, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("failed to create download file %s", path)
	}
	defer func() {
		_ = file.Close()
	}()

	return file.Name(), nil
}
