package sectorstatemgr

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/node/config"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
)

var log = logging.Logger("sectorstatemgr")

type SectorStateMgr struct {
	sync.Mutex

	cfg           config.StorageConfig
	fullnodeApi   api.FullNode
	minerApi      api.StorageMiner
	Maddr         address.Address
	stateUpdates  map[abi.SectorID]db.SealState
	sectorStates  map[abi.SectorID]db.SealState
	activeSectors map[abi.SectorID]struct{}
	latestUpdate  time.Time

	sdb *db.SectorStateDB
}

func NewSectorStateMgr(cfg *config.Boost) func(lc fx.Lifecycle, sdb *db.SectorStateDB, minerApi lotus_modules.MinerStorageService, fullnodeApi api.FullNode, maddr lotus_dtypes.MinerAddress) *SectorStateMgr {
	return func(lc fx.Lifecycle, sdb *db.SectorStateDB, minerApi lotus_modules.MinerStorageService, fullnodeApi api.FullNode, maddr lotus_dtypes.MinerAddress) *SectorStateMgr {
		mgr := &SectorStateMgr{
			cfg:           cfg.Storage,
			minerApi:      minerApi,
			fullnodeApi:   fullnodeApi,
			Maddr:         address.Address(maddr),
			stateUpdates:  make(map[abi.SectorID]db.SealState),
			activeSectors: make(map[abi.SectorID]struct{}),

			sdb: sdb,
		}

		cctx, cancel := context.WithCancel(context.Background())
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				go mgr.Run(cctx)
				return nil
			},
			OnStop: func(ctx context.Context) error {
				cancel()
				return nil
			},
		})

		return mgr
	}
}

func (m *SectorStateMgr) Run(ctx context.Context) {
	duration := time.Duration(m.cfg.StorageListRefreshDuration)
	log.Infof("starting sector state manager running on interval %s", duration.String())
	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	// Check immediately
	err := m.checkForUpdates(ctx)
	if err != nil {
		log.Errorw("checking for state updates", "err", err)
	}

	// Check every tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := m.checkForUpdates(ctx)
			if err != nil {
				log.Errorw("checking for state updates", "err", err)
			}
		}
	}
}

func (m *SectorStateMgr) GetStateUpdates() (r map[abi.SectorID]db.SealState) {
	m.Lock()
	r = m.stateUpdates
	m.Unlock()
	return
}

func (m *SectorStateMgr) GetSectorStates() (r map[abi.SectorID]db.SealState) {
	m.Lock()
	r = m.sectorStates
	m.Unlock()
	return
}

func (m *SectorStateMgr) GetActiveSectors() (r map[abi.SectorID]struct{}) {
	m.Lock()
	r = m.activeSectors
	m.Unlock()
	return
}

func (m *SectorStateMgr) GetLatestUpdate() (r time.Time) {
	m.Lock()
	r = m.latestUpdate
	m.Unlock()
	return
}

func (m *SectorStateMgr) checkForUpdates(ctx context.Context) error {
	log.Debug("checking for sector state updates")

	defer func(start time.Time) { log.Debugw("checkForUpdates", "took", time.Since(start)) }(time.Now())

	// Tell lotus to update it's storage list and remove any removed sectors
	if m.cfg.RedeclareOnStorageListRefresh {
		log.Info("redeclaring storage")
		err := m.minerApi.StorageRedeclareLocal(ctx, nil, true)
		if err != nil {
			log.Errorw("redeclaring local storage on lotus miner", "err", err)
		}
	}

	su, ss, err := m.refreshState(ctx)
	if err != nil {
		return err
	}

	head, err := m.fullnodeApi.ChainHead(ctx)
	if err != nil {
		return err
	}

	activeSet, err := m.fullnodeApi.StateMinerActiveSectors(ctx, m.Maddr, head.Key())
	if err != nil {
		return err
	}

	mid, err := address.IDFromAddress(m.Maddr)
	if err != nil {
		return err
	}
	as := make(map[abi.SectorID]struct{}, len(activeSet))
	for _, info := range activeSet {
		sectorID := abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: info.SectorNumber,
		}

		as[sectorID] = struct{}{}
	}

	m.Lock()
	m.stateUpdates = su
	m.sectorStates = ss
	m.activeSectors = as
	m.latestUpdate = time.Now()
	m.Unlock()

	for sectorID, sectorSealState := range su {
		// Update the sector seal state in the database
		err = m.sdb.Update(ctx, sectorID, sectorSealState)
		if err != nil {
			return fmt.Errorf("updating sectors unseal state in database for miner %d / sector %d: %w", sectorID.Miner, sectorID.Number, err)
		}
	}

	return nil
}

func (m *SectorStateMgr) refreshState(ctx context.Context) (map[abi.SectorID]db.SealState, map[abi.SectorID]db.SealState, error) {
	defer func(start time.Time) { log.Debugw("refreshState", "took", time.Since(start)) }(time.Now())

	// Get the current unsealed state of all sectors from lotus
	storageList, err := m.minerApi.StorageList(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("getting sectors state from lotus: %w", err)
	}

	// Convert to a map of <sector id> => <seal state>
	sectorStates := make(map[abi.SectorID]db.SealState)
	allSectorStates := make(map[abi.SectorID]db.SealState)
	for _, storageStates := range storageList {
		for _, storageState := range storageStates {
			// Explicity set the sector state if its Sealed or Unsealed
			switch {
			case storageState.SectorFileType.Has(storiface.FTUnsealed):
				sectorStates[storageState.SectorID] = db.SealStateUnsealed
			case storageState.SectorFileType.Has(storiface.FTSealed):
				if state, ok := sectorStates[storageState.SectorID]; !ok || state != db.SealStateUnsealed {
					sectorStates[storageState.SectorID] = db.SealStateSealed
				}
			}

			// If the state hasnt been set it should be in the cache, mark it so we dont remove
			// This may get overriden by the sealed status if it comes after in the list, which is fine
			if _, ok := sectorStates[storageState.SectorID]; !ok {
				sectorStates[storageState.SectorID] = db.SealStateCache
			}
			allSectorStates[storageState.SectorID] = sectorStates[storageState.SectorID]
		}
	}

	// Get the previously known state of all sectors in the database
	previousSectorStates, err := m.sdb.List(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("getting sectors state from database: %w", err)
	}

	// Check which sectors have changed state since the last time we checked
	sealStateUpdates := make(map[abi.SectorID]db.SealState)
	for _, previousSectorState := range previousSectorStates {
		sealState, ok := sectorStates[previousSectorState.SectorID]
		if ok {
			// Check if the state has changed, ignore if the new state is cache
			if previousSectorState.SealState != sealState && sealState != db.SealStateCache {
				sealStateUpdates[previousSectorState.SectorID] = sealState
			}
			// Delete the sector from the map - at the end the remaining
			// sectors in the map are ones we didn't know about before
			delete(sectorStates, previousSectorState.SectorID)
		} else {
			// The sector is no longer in the list, so it must have been removed
			sealStateUpdates[previousSectorState.SectorID] = db.SealStateRemoved
		}
	}

	// The remaining sectors in the map are ones we didn't know about before
	for sectorID, sealState := range sectorStates {
		sealStateUpdates[sectorID] = sealState
	}

	return sealStateUpdates, allSectorStates, nil
}
