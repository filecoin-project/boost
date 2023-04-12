package storagespace

import (
	"context"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
)

type Status struct {
	// The total number of bytes allocated for incoming data
	TotalAvailable uint64
	// The number of bytes reserved for accepted deals
	Tagged uint64
	// The number of bytes that have been downloaded and are waiting to be added to a sector
	Staged uint64
	// The number of bytes that are not tagged
	Free uint64
}

func GetStatus(ctx context.Context, mgr *storagemanager.StorageManager, db *db.DealsDB) (*Status, error) {
	tagged, err := mgr.TotalTagged(ctx)
	if err != nil {
		return nil, err
	}

	free, err := mgr.Free(ctx)
	if err != nil {
		return nil, err
	}

	activeDeals, err := db.ListActive(ctx)
	if err != nil {
		return nil, err
	}

	staged := uint64(0)
	for _, deal := range activeDeals {
		if deal.IsOffline {
			continue
		}
		if deal.Checkpoint < dealcheckpoints.AddedPiece {
			staged += deal.Transfer.Size
		}
	}

	return &Status{
		TotalAvailable: mgr.Cfg.MaxStagingDealsBytes,
		Staged:         staged,
		Tagged:         tagged,
		Free:           free,
	}, nil
}
