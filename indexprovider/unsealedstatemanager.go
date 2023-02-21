package indexprovider

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	logging "github.com/ipfs/go-log/v2"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/metadata"
	"time"
)

//go:generate go run github.com/golang/mock/mockgen -destination=./mock/mock.go -package=mock github.com/filecoin-project/go-fil-markets/storagemarket StorageProvider

var usmlog = logging.Logger("unsmgr")

type ApiStorageMiner interface {
	StorageList(ctx context.Context) (map[storiface.ID][]storiface.Decl, error)
}

type UnsealedStateManager struct {
	idxprov    *Wrapper
	legacyProv storagemarket.StorageProvider
	dealsDB    *db.DealsDB
	sdb        *db.SectorStateDB
	api        ApiStorageMiner
}

func NewUnsealedStateManager(idxprov *Wrapper, legacyProv storagemarket.StorageProvider, dealsDB *db.DealsDB, sdb *db.SectorStateDB, api ApiStorageMiner) *UnsealedStateManager {
	return &UnsealedStateManager{
		idxprov:    idxprov,
		legacyProv: legacyProv,
		dealsDB:    dealsDB,
		sdb:        sdb,
		api:        api,
	}
}

func (m *UnsealedStateManager) Run(ctx context.Context) {
	usmlog.Info("starting unsealed state manager")
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	// Check immediately
	err := m.checkForUpdates(ctx)
	if err != nil {
		usmlog.Errorf("error checking for unsealed state updates: %s", err)
	}

	// Check every tick
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
	usmlog.Info("checking for sector state updates")
	stateUpdates, err := m.getStateUpdates(ctx)
	if err != nil {
		return err
	}

	legacyDeals, err := m.legacyDealsBySectorID(stateUpdates)
	if err != nil {
		return fmt.Errorf("getting legacy deals from datastore: %w", err)
	}

	// For each sector
	for sectorID, sectorSealState := range stateUpdates {
		// Get the deals in the sector
		deals, err := m.dealsBySectorID(ctx, legacyDeals, sectorID)
		if err != nil {
			return fmt.Errorf("getting deals for miner %d / sector %d: %w", sectorID.Miner, sectorID.Number, err)
		}

		// For each deal in the sector
		for _, deal := range deals {
			if !deal.AnnounceToIPNI {
				continue
			}

			propnd, err := cborutil.AsIpld(&deal.DealProposal)
			if err != nil {
				return fmt.Errorf("failed to compute signed deal proposal ipld node: %w", err)
			}
			propCid := propnd.Cid()

			if sectorSealState == db.SealStateRemoved {
				// Announce deals that are no longer unsealed to indexer
				announceCid, err := m.idxprov.AnnounceBoostDealRemoved(ctx, propCid)
				if err != nil {
					// Check if the error is because the deal wasn't previously announced
					if !errors.Is(err, provider.ErrContextIDNotFound) {
						// There was some other error, write it to the log
						usmlog.Errorw("announcing deal removed to index provider",
							"deal id", deal.DealID, "error", err)
						continue
					}
				} else {
					usmlog.Infow("announced to index provider that deal has been removed",
						"deal id", deal.DealID, "sector id", deal.SectorID.Number, "announce cid", announceCid.String())
				}
			} else {
				// Announce deals that have changed seal state to indexer
				md := metadata.GraphsyncFilecoinV1{
					PieceCID:      deal.DealProposal.Proposal.PieceCID,
					FastRetrieval: sectorSealState == db.SealStateUnsealed,
					VerifiedDeal:  deal.DealProposal.Proposal.VerifiedDeal,
				}
				announceCid, err := m.idxprov.announceBoostDealMetadata(ctx, md, propCid)
				if err != nil {
					// Check if the error is because the deal was already advertised
					if !errors.Is(err, provider.ErrAlreadyAdvertised) {
						// There was some other error, write it to the log
						usmlog.Errorf("announcing deal %s to index provider: %w", deal.DealID, err)
						continue
					}
				} else {
					usmlog.Infow("announced deal seal state to index provider",
						"deal id", deal.DealID, "sector id", deal.SectorID.Number,
						"seal state", sectorSealState, "announce cid", announceCid.String())
				}
			}
		}

		// Update the sector seal state in the database
		err = m.sdb.Update(ctx, sectorID, sectorSealState)
		if err != nil {
			return fmt.Errorf("updating sectors unseal state in database for miner %d / sector %d: %w", sectorID.Miner, sectorID.Number, err)
		}
	}

	return nil
}

func (m *UnsealedStateManager) getStateUpdates(ctx context.Context) (map[abi.SectorID]db.SealState, error) {
	// Get the current unsealed state of all sectors from lotus
	storageList, err := m.api.StorageList(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting sectors state from lotus: %w", err)
	}

	// Convert to a map of <sector id> => <seal state>
	sectorStates := make(map[abi.SectorID]db.SealState, len(storageList))
	for _, storageStates := range storageList {
		for _, storageState := range storageStates {
			var sealState db.SealState
			switch {
			case storageState.SectorFileType.Has(storiface.FTUnsealed):
				sealState = db.SealStateUnsealed
			case storageState.SectorFileType.Has(storiface.FTSealed):
				sealState = db.SealStateSealed
			}
			sectorStates[storageState.SectorID] = sealState
		}
	}

	// Get the previously known state of all sectors in the database
	previousSectorStates, err := m.sdb.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting sectors state from database: %w", err)
	}

	// Check which sectors have changed state since the last time we checked
	sealStateUpdates := make(map[abi.SectorID]db.SealState)
	for _, previousSectorState := range previousSectorStates {
		sealState, ok := sectorStates[previousSectorState.SectorID]
		if ok {
			// Check if the state has changed
			if previousSectorState.SealState != sealState {
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

	return sealStateUpdates, nil
}

type basicDealInfo struct {
	AnnounceToIPNI bool
	DealID         string
	SectorID       abi.SectorID
	DealProposal   storagemarket.ClientDealProposal
}

// Get deals by sector ID, whether they're legacy or boost deals
func (m *UnsealedStateManager) dealsBySectorID(ctx context.Context, legacyDeals map[abi.SectorID][]storagemarket.MinerDeal, sectorID abi.SectorID) ([]basicDealInfo, error) {
	// First query the boost database
	deals, err := m.dealsDB.BySectorID(ctx, sectorID)
	if err != nil {
		return nil, fmt.Errorf("getting deals from boost database: %w", err)
	}

	basicDeals := make([]basicDealInfo, 0, len(deals))
	for _, dl := range deals {
		basicDeals = append(basicDeals, basicDealInfo{
			AnnounceToIPNI: dl.AnnounceToIPNI,
			DealID:         dl.DealUuid.String(),
			SectorID:       sectorID,
			DealProposal:   dl.ClientDealProposal,
		})
	}

	// Then check the legacy deals
	legDeals, ok := legacyDeals[sectorID]
	if ok {
		for _, dl := range legDeals {
			basicDeals = append(basicDeals, basicDealInfo{
				AnnounceToIPNI: true,
				DealID:         dl.ProposalCid.String(),
				SectorID:       sectorID,
				DealProposal:   dl.ClientDealProposal,
			})
		}
	}

	return basicDeals, nil
}

// Iterate over all legacy deals and make a map of sector ID -> legacy deal.
// To save memory, only include legacy deals with a sector ID that we know
// we're going to query, ie the set of sector IDs in the stateUpdates map.
func (m *UnsealedStateManager) legacyDealsBySectorID(stateUpdates map[abi.SectorID]db.SealState) (map[abi.SectorID][]storagemarket.MinerDeal, error) {
	legacyDeals, err := m.legacyProv.ListLocalDeals()
	if err != nil {
		return nil, err
	}

	bySectorID := make(map[abi.SectorID][]storagemarket.MinerDeal, len(legacyDeals))
	for _, deal := range legacyDeals {
		minerID, err := address.IDFromAddress(deal.Proposal.Provider)
		if err != nil {
			// just skip the deal if we can't convert its address to an ID address
			continue
		}
		sectorID := abi.SectorID{
			Miner:  abi.ActorID(minerID),
			Number: deal.SectorNumber,
		}
		_, ok := stateUpdates[sectorID]
		if ok {
			bySectorID[sectorID] = append(bySectorID[sectorID], deal)
		}
	}

	return bySectorID, nil
}
