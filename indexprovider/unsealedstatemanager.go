package indexprovider

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	logging "github.com/ipfs/go-log/v2"
	provider "github.com/ipni/index-provider"
	"time"
)

var usmlog = logging.Logger("unsmgr")

type UnsealedStateManager struct {
	idxprov Wrapper
	dealsDB db.DealsDB
	sdb     db.SectorStateDB
	api     api.StorageMiner
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
	sectorIsUnsealedUpdates, err := m.getStateUpdates(ctx)
	if err != nil {
		return err
	}

	// Get the deals in each sector that are now
	// - unsealed
	// - no longer unsealed
	sectorsUnsealed := make([]abi.SectorID, 0, len(sectorIsUnsealedUpdates))
	sectorsNotUnsealed := make([]abi.SectorID, 0, len(sectorIsUnsealedUpdates))
	for sectorID, isUnsealed := range sectorIsUnsealedUpdates {
		if isUnsealed {
			sectorsUnsealed = append(sectorsUnsealed, sectorID)
		} else {
			sectorsNotUnsealed = append(sectorsNotUnsealed, sectorID)
		}
	}

	dealsUnsealed, err := m.dealsDB.BySectorIDs(ctx, sectorsUnsealed)
	if err != nil {
		return fmt.Errorf("getting deals for unsealed sectors: %w", err)
	}
	dealsNotUnsealed, err := m.dealsDB.BySectorIDs(ctx, sectorsNotUnsealed)
	if err != nil {
		return fmt.Errorf("getting deals for sealed sectors: %w", err)
	}

	// Announce deals that are now unsealed to indexer
	for _, deal := range dealsUnsealed {
		announceCid, err := m.idxprov.AnnounceBoostDeal(ctx, deal)
		if err != nil && !errors.Is(err, provider.ErrAlreadyAdvertised) {
			return fmt.Errorf("announcing deal %s to index provider: %w", deal.DealUuid, err)
		}

		log.Infow("announced to index provider that deal is now unsealed",
			"deal id", deal.DealUuid, "sector id", deal.SectorID, "announce cid", announceCid)
	}

	// Announce deals that are no longer unsealed to indexer
	for _, deal := range dealsNotUnsealed {
		announceCid, err := m.idxprov.AnnounceBoostDealRemoved(ctx, deal)
		if err != nil && !errors.Is(err, provider.ErrContextIDNotFound) {
			return fmt.Errorf("announcing deal %s no longer unsealed to index provider: %w", deal.DealUuid, err)
		}

		log.Infof("announced to index provider that deal no longer has unsealed sector",
			"deal id", deal.DealUuid, "sector id", deal.SectorID, "announce cid", announceCid)
	}

	// Update the sector unseal state in the database
	err = m.sdb.Update(ctx, sectorIsUnsealedUpdates)
	if err != nil {
		return fmt.Errorf("updating sectors unseal state in database: %w", err)
	}

	return nil
}

func (m *UnsealedStateManager) getStateUpdates(ctx context.Context) (map[abi.SectorID]bool, error) {
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

	return sectorIsUnsealedUpdates, nil
}
