package sectorstatemgr

//go:generate go run github.com/golang/mock/mockgen -destination=mock/sectorstatemgr.go -package=mock . StorageAPI

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/node/config"
	sectorstatemgr_types "github.com/filecoin-project/boost/sectorstatemgr/types"
	storagemarket_types "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/fx"
)

var log = logging.Logger("sectorstatemgr")

type SectorStateUpdates struct {
	Maddr           address.Address
	Updates         map[abi.SectorID]db.SealState
	ActiveSectors   map[abi.SectorID]struct{}
	SectorWithDeals map[abi.SectorID]struct{}
	SectorStates    map[abi.SectorID]db.SealState
	UpdatedAt       time.Time
}

type SectorStateMgr struct {
	sync.Mutex

	cfg         config.StorageConfig
	fullnodeApi api.FullNode
	minerApis   []sectorstatemgr_types.StorageAPI
	Maddrs      []address.Address

	PubSub *PubSub

	LatestUpdateMus map[address.Address]*sync.Mutex
	LatestUpdates   map[address.Address]*SectorStateUpdates

	sdb *db.SectorStateDB
}

func NewSectorStateMgr(cfg *config.Boost) func(lc fx.Lifecycle, sdb *db.SectorStateDB, fullnodeApi api.FullNode, maddr lotus_dtypes.MinerAddress, me storagemarket_types.MinerEndpoints) (*SectorStateMgr, error) {
	return func(lc fx.Lifecycle, sdb *db.SectorStateDB, fullnodeApi api.FullNode, maddr lotus_dtypes.MinerAddress, me storagemarket_types.MinerEndpoints) (*SectorStateMgr, error) {
		addrs := me.Actors()
		apis := make([]sectorstatemgr_types.StorageAPI, 0, len(addrs))
		mus := make(map[address.Address]*sync.Mutex)
		for _, addr := range addrs {
			sApi, err := me.StorageAPI(addr)
			if err != nil {
				return nil, err
			}
			apis = append(apis, sApi)
			mus[addr] = &sync.Mutex{}
		}

		mgr := &SectorStateMgr{
			cfg:         cfg.Storage,
			minerApis:   apis,
			fullnodeApi: fullnodeApi,
			Maddrs:      addrs,

			PubSub: NewPubSub(),

			sdb:             sdb,
			LatestUpdates:   make(map[address.Address]*SectorStateUpdates),
			LatestUpdateMus: mus,
		}

		cctx, cancel := context.WithCancel(context.Background())
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				go mgr.Run(cctx)
				return nil
			},
			OnStop: func(ctx context.Context) error {
				cancel()
				mgr.PubSub.Close()
				return nil
			},
		})

		return mgr, nil
	}
}

func (m *SectorStateMgr) UpdateLatest(ctx context.Context) {
	go func() {
		sub := m.PubSub.Subscribe()

		for {
			select {
			case u, ok := <-sub:
				if !ok {
					log.Debugw("state updates subscription closed")
					return
				}
				log.Debugw("got state updates from SectorStateMgr", "len(u.updates)", len(u.Updates), "len(u.active)", len(u.ActiveSectors), "u.updatedAt", u.UpdatedAt)

				mu := m.LatestUpdateMus[u.Maddr]
				if mu == nil {
					log.Errorf("Received state update for an unknown miner %s", u.Maddr.String())
					return
				}
				mu.Lock()
				m.LatestUpdates[u.Maddr] = u
				mu.Unlock()

			case <-ctx.Done():
				return
			}
		}
	}()
}

func (m *SectorStateMgr) Run(ctx context.Context) {
	duration := time.Duration(m.cfg.StorageListRefreshDuration)
	log.Infof("starting sector state manager running on interval %s", duration.String())

	m.UpdateLatest(ctx)

	// Check immediately
	err := m.checkForUpdates(ctx)
	if err != nil {
		log.Errorw("checking for state updates", "err", err)
	}

	ticker := time.NewTicker(duration)
	defer ticker.Stop()

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

func (m *SectorStateMgr) checkForUpdates(ctx context.Context) error {
	log.Debug("checking for sector state updates")

	defer func(start time.Time) { log.Debugw("checkForUpdates", "took", time.Since(start)) }(time.Now())

	ssus, err := m.refreshState(ctx)
	if err != nil {
		return err
	}

	for _, ssu := range ssus {
		for sectorID, sectorSealState := range ssu.Updates {
			// Update the sector seal state in the database
			err = m.sdb.Update(ctx, sectorID, sectorSealState)
			if err != nil {
				return fmt.Errorf("updating sectors unseal state in database for miner %d / sector %d: %w", sectorID.Miner, sectorID.Number, err)
			}
		}

		m.PubSub.Publish(ssu)
	}

	return nil
}

func (m *SectorStateMgr) refreshState(ctx context.Context) ([]*SectorStateUpdates, error) {
	var wg sync.WaitGroup
	results := make(chan *SectorStateUpdates, len(m.Maddrs))

	for i, addr := range m.Maddrs {
		addr := addr
		mapi := m.minerApis[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			ssu, err := m.refreshMinerState(ctx, addr, mapi)
			if err != nil {
				log.Errorf("Error refrshing miner %s sectors state: %w", addr.String(), err)
				return
			}
			results <- ssu
		}()
	}
	wg.Wait()

	close(results)

	ssus := make([]*SectorStateUpdates, 0, len(m.Maddrs))
	for ssu := range results {
		ssus = append(ssus, ssu)
	}
	return ssus, nil
}

func (m *SectorStateMgr) refreshMinerState(ctx context.Context, maddr address.Address, minerApi sectorstatemgr_types.StorageAPI) (*SectorStateUpdates, error) {
	defer func(start time.Time) { log.Debugw("refreshState", "took", time.Since(start)) }(time.Now())

	// Tell lotus to update it's storage list and remove any removed sectors
	if m.cfg.RedeclareOnStorageListRefresh {
		log.Info("redeclaring storage")
		err := minerApi.StorageRedeclareLocal(ctx, nil, true)
		if err != nil {
			log.Errorw("redeclaring local storage on lotus miner", "err", err)
		}
	}

	// Get the current unsealed state of all sectors from lotus
	storageList, err := minerApi.StorageList(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting sectors state from lotus: %w", err)
	}

	// Convert to a map of <sector id> => <seal state>
	sectorStates := make(map[abi.SectorID]db.SealState)
	allSectorStates := make(map[abi.SectorID]db.SealState)
	for _, loc := range storageList {
		for _, sectorDecl := range loc {
			// Explicity set the sector state if its Sealed or Unsealed
			switch {
			case sectorDecl.SectorFileType.Has(storiface.FTUnsealed):
				sectorStates[sectorDecl.SectorID] = db.SealStateUnsealed
			case sectorDecl.SectorFileType.Has(storiface.FTSealed):
				if state, ok := sectorStates[sectorDecl.SectorID]; !ok || state != db.SealStateUnsealed {
					sectorStates[sectorDecl.SectorID] = db.SealStateSealed
				}
			}

			// If the state hasnt been set it should be in the cache, mark it so we dont remove
			// This may get overriden by the sealed status if it comes after in the list, which is fine
			if _, ok := sectorStates[sectorDecl.SectorID]; !ok {
				sectorStates[sectorDecl.SectorID] = db.SealStateCache
			}
			allSectorStates[sectorDecl.SectorID] = sectorStates[sectorDecl.SectorID]
		}
	}

	// Get the previously known state of all sectors in the database
	previousSectorStates, err := m.sdb.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting sectors state from database: %w", err)
	}

	// Check which sectors have changed state since the last time we checked
	sectorUpdates := make(map[abi.SectorID]db.SealState)
	for _, pss := range previousSectorStates {
		sealState, ok := sectorStates[pss.SectorID]
		if ok {
			// Check if the state has changed, ignore if the new state is cache
			if pss.SealState != sealState && sealState != db.SealStateCache {
				sectorUpdates[pss.SectorID] = sealState
			}
			// Delete the sector from the map - at the end the remaining
			// sectors in the map are ones we didn't know about before
			delete(sectorStates, pss.SectorID)
		} else {
			// The sector is no longer in the list, so it must have been removed
			sectorUpdates[pss.SectorID] = db.SealStateRemoved
		}
	}

	// The remaining sectors in the map are ones we didn't know about before
	for sectorID, sealState := range sectorStates {
		sectorUpdates[sectorID] = sealState
	}

	head, err := m.fullnodeApi.ChainHead(ctx)
	if err != nil {
		return nil, err
	}

	activeSet, err := m.fullnodeApi.StateMinerActiveSectors(ctx, maddr, head.Key())
	if err != nil {
		return nil, err
	}

	allSet, err := m.fullnodeApi.StateMinerSectors(ctx, maddr, nil, head.Key())
	if err != nil {
		return nil, err
	}

	mid, err := address.IDFromAddress(maddr)
	if err != nil {
		return nil, err
	}

	activeSectors := make(map[abi.SectorID]struct{}, len(activeSet))
	for _, info := range activeSet {
		sectorID := abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: info.SectorNumber,
		}

		activeSectors[sectorID] = struct{}{}
	}

	sectorWithDeals := make(map[abi.SectorID]struct{})
	zero := big.Zero()
	for _, info := range allSet {
		sectorID := abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: info.SectorNumber,
		}

		if info.DealWeight.GreaterThan(zero) {
			sectorWithDeals[sectorID] = struct{}{}
		}
	}

	for k := range allSectorStates {
		if _, ok := activeSectors[k]; !ok {
			log.Debugw("sector present in all sector states, but not active", "number", k)
		}
	}

	return &SectorStateUpdates{maddr, sectorUpdates, activeSectors, sectorWithDeals, allSectorStates, time.Now()}, nil
}
