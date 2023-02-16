package indexprovider

import (
	"context"
	"fmt"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	logging "github.com/ipfs/go-log/v2"
	"time"
)

var usmlog = logging.Logger("unsmgr")

type UnsealedStateManager struct {
	sdb db.SectorStateDB
	api api.StorageMiner
}

func (m *UnsealedStateManager) Start(ctx context.Context) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := m.checkForUpdates(ctx)
			if err != nil {
				usmlog.Errorf("error checking for unsealed state updates: %s", err)
			}
		}
	}
}

func (m *UnsealedStateManager) checkForUpdates(ctx context.Context) error {
	sectorIsUnsealedUpdates, err := m.updateState(ctx)
	if err != nil {
		return err
	}

	// TODO: Update indexer state
	_ = sectorIsUnsealedUpdates

	return nil
}

func (m *UnsealedStateManager) updateState(ctx context.Context) (map[abi.SectorID]bool, error) {
	// Get the current unsealed state of all sectors from lotus
	storageList, err := m.api.StorageList(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting sectors state from lotus: %w", err)
	}

	// Convert to a map of <sector id> => <is unsealed>
	sectorIsUnsealed := make(map[abi.SectorID]bool, len(storageList))
	for _, storageStates := range storageList {
		sectorIsUnsealed[storageStates[0].SectorID] = false
		for _, storageState := range storageStates {
			if storageState.SectorFileType.Has(storiface.FTUnsealed) {
				sectorIsUnsealed[storageState.SectorID] = true
			}
		}
	}

	// Get the state of all sectors in the database
	sectors, err := m.sdb.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting sectors state from database: %w", err)
	}

	// Check which sectors have changed state since the last time we checked
	sectorIsUnsealedUpdates := make(map[abi.SectorID]bool)
	for _, sector := range sectors {
		isUnsealed, ok := sectorIsUnsealed[sector.SectorID]
		if ok {
			if sector.Unsealed != isUnsealed {
				sectorIsUnsealedUpdates[sector.SectorID] = isUnsealed
			}
			// Delete the sector from the map - at the end the remaining
			// sectors in the map are ones we didn't know about before
			delete(sectorIsUnsealed, sector.SectorID)
		}
	}

	// The remaining sectors in the map are ones we didn't know about before
	for sectorID, isUnsealed := range sectorIsUnsealed {
		sectorIsUnsealedUpdates[sectorID] = isUnsealed
	}

	// Update the database
	err = m.sdb.Update(ctx, sectorIsUnsealedUpdates)
	if err != nil {
		return nil, fmt.Errorf("updating sectors state in database: %w", err)
	}

	return sectorIsUnsealedUpdates, nil
}
