package sectorstatemgr

//go:generate go run github.com/golang/mock/mockgen -destination=mock/sectorstatemgr.go -package=mock . StorageAPI

import (
	"context"
	"fmt"
	"sort"
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

type SectorStateUpdates struct {
	Updates          map[abi.SectorID]db.SealState
	SectorsTable     []Sector
	CommittedSectors map[abi.SectorID]struct{}
	ActiveSectors    map[abi.SectorID]struct{}
	SectorStates     map[abi.SectorID]db.SealState
	UpdatedAt        time.Time
}

type Sector struct {
	SectorNumber abi.SectorNumber
	//State         api.SectorState
	Unsealed bool
	//Deals         []abi.DealID
	Active bool
	//DealWeight    big.Int
	//VerifiedPower big.Int
	//Expiration    abi.ChainEpoch
	OnChain bool
}

type StorageAPI interface {
	StorageRedeclareLocal(context.Context, *storiface.ID, bool) error
	StorageList(context.Context) (map[storiface.ID][]storiface.Decl, error)
	SectorsList(context.Context) ([]abi.SectorNumber, error)
	SectorsStatus(context.Context, abi.SectorNumber, bool) (api.SectorInfo, error)
}

type SectorStateMgr struct {
	sync.Mutex

	cfg         config.StorageConfig
	fullnodeApi api.FullNode
	minerApi    StorageAPI
	Maddr       address.Address

	PubSub *PubSub

	sdb *db.SectorStateDB
}

func NewSectorStateMgr(cfg *config.Boost) func(lc fx.Lifecycle, sdb *db.SectorStateDB, minerApi lotus_modules.MinerStorageService, fullnodeApi api.FullNode, maddr lotus_dtypes.MinerAddress) *SectorStateMgr {
	return func(lc fx.Lifecycle, sdb *db.SectorStateDB, minerApi lotus_modules.MinerStorageService, fullnodeApi api.FullNode, maddr lotus_dtypes.MinerAddress) *SectorStateMgr {
		mgr := &SectorStateMgr{
			cfg:         cfg.Storage,
			minerApi:    minerApi,
			fullnodeApi: fullnodeApi,
			Maddr:       address.Address(maddr),

			PubSub: NewPubSub(),

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
				mgr.PubSub.Close()
				return nil
			},
		})

		return mgr
	}
}

func (m *SectorStateMgr) Run(ctx context.Context) {
	duration := time.Duration(m.cfg.StorageListRefreshDuration)
	log.Infof("starting sector state manager running on interval %s", duration.String())

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

	ssu, err := m.refreshState(ctx)
	if err != nil {
		return err
	}

	for sectorID, sectorSealState := range ssu.Updates {
		// Update the sector seal state in the database
		err = m.sdb.Update(ctx, sectorID, sectorSealState)
		if err != nil {
			return fmt.Errorf("updating sectors unseal state in database for miner %d / sector %d: %w", sectorID.Miner, sectorID.Number, err)
		}
	}

	m.PubSub.Publish(ssu)

	return nil
}

func (m *SectorStateMgr) refreshState(ctx context.Context) (*SectorStateUpdates, error) {
	defer func(start time.Time) { log.Debugw("refreshState", "took", time.Since(start)) }(time.Now())

	// Tell lotus to update it's storage list and remove any removed sectors
	if m.cfg.RedeclareOnStorageListRefresh {
		log.Info("redeclaring storage")
		err := m.minerApi.StorageRedeclareLocal(ctx, nil, true)
		if err != nil {
			log.Errorw("redeclaring local storage on lotus miner", "err", err)
		}
	}

	// Get the current unsealed state of all sectors from lotus
	storageList, err := m.minerApi.StorageList(ctx)
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

	activeSet, err := m.fullnodeApi.StateMinerActiveSectors(ctx, m.Maddr, head.Key())
	if err != nil {
		return nil, err
	}

	mid, err := address.IDFromAddress(m.Maddr)
	if err != nil {
		return nil, err
	}
	activeSectors := make(map[abi.SectorID]struct{}, len(activeSet))
	for _, info := range activeSet {
		sid := abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: info.SectorNumber,
		}

		activeSectors[sid] = struct{}{}
	}

	committedSectorsSet, err := m.fullnodeApi.StateMinerSectors(ctx, m.Maddr, nil, head.Key())
	if err != nil {
		return nil, err
	}
	committedSectors := make(map[abi.SectorID]struct{}, len(committedSectorsSet))
	for _, info := range committedSectorsSet {
		sid := abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: info.SectorNumber,
		}

		committedSectors[sid] = struct{}{}
	}

	sectorNumbers, err := m.minerApi.SectorsList(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(sectorNumbers, func(i, j int) bool {
		return sectorNumbers[i] < sectorNumbers[j]
	})

	sectorsTable := make([]Sector, 0, len(sectorNumbers))
	for _, sectorNumber := range sectorNumbers {
		sid := abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: sectorNumber,
		}

		//st, err := m.minerApi.SectorsStatus(ctx, sectorNumber, true)
		//if err != nil {
		//return nil, err
		//}

		s := Sector{
			SectorNumber: sectorNumber,
			//State:        st.State,
			//Deals:        st.Deals,
			//Expiration:   st.Expiration,
		}

		_, s.Active = activeSectors[sid]
		_, s.OnChain = committedSectors[sid]

		// TODO: get Unsealed state from unsealed state manager once this PR has landed:
		// https://github.com/filecoin-project/boost/pull/1463
		s.Unsealed = true

		//
		// Copied from code for lotus-miner sectors list command
		//
		//{
		//const verifiedPowerGainMul = 9
		//dw := big.NewInt(0)
		//vp := big.NewInt(0)
		//estimate := (st.Expiration-st.Activation <= 0) || sealing.IsUpgradeState(sealing.SectorState(st.State))
		//if !estimate {
		//rdw := big.Add(st.DealWeight, st.VerifiedDealWeight)
		//dw = big.Div(rdw, big.NewInt(int64(st.Expiration-st.Activation)))
		//vp = big.Div(big.Mul(st.VerifiedDealWeight, big.NewInt(verifiedPowerGainMul)), big.NewInt(int64(st.Expiration-st.Activation)))
		//} else {
		//for _, piece := range st.Pieces {
		//if piece.DealInfo != nil {
		//dw = big.Add(dw, big.NewInt(int64(piece.Piece.Size)))
		//if piece.DealInfo.DealProposal != nil && piece.DealInfo.DealProposal.VerifiedDeal {
		//vp = big.Add(vp, big.Mul(big.NewInt(int64(piece.Piece.Size)), big.NewInt(verifiedPowerGainMul)))
		//}
		//}
		//}
		//}

		//s.DealWeight = dw
		//s.VerifiedPower = vp
		//}

		sectorsTable = append(sectorsTable, s)
	}

	return &SectorStateUpdates{
		Updates:          sectorUpdates,
		SectorsTable:     sectorsTable,
		CommittedSectors: committedSectors,
		ActiveSectors:    activeSectors,
		SectorStates:     allSectorStates,
		UpdatedAt:        time.Now(),
	}, nil
}
