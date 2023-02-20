package indexprovider

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	logging "github.com/ipfs/go-log/v2"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/metadata"
	"time"
)

var usmlog = logging.Logger("unsmgr")

type ApiStorageMiner interface {
	StorageList(ctx context.Context) (map[storiface.ID][]storiface.Decl, error)
}

type UnsealedStateManager struct {
	idxprov *Wrapper
	dealsDB *db.DealsDB
	sdb     *db.SectorStateDB
	api     ApiStorageMiner
}

func NewUnsealedStateManager(idxprov *Wrapper, dealsDB *db.DealsDB, sdb *db.SectorStateDB, api ApiStorageMiner) *UnsealedStateManager {
	return &UnsealedStateManager{
		idxprov: idxprov,
		dealsDB: dealsDB,
		sdb:     sdb,
		api:     api,
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

	for sectorID, sectorSealState := range stateUpdates {
		deal, err := m.dealsDB.BySectorID(ctx, sectorID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// If boost doesn't know about this deal, just ignore it
				continue
			}
			return fmt.Errorf("getting deal for miner %d / sector %d: %w", sectorID.Miner, sectorID.Number, err)
		}

		if !deal.AnnounceToIPNI {
			continue
		}

		propCid, err := deal.SignedProposalCid()
		if err != nil {
			return fmt.Errorf("failed to get proposal cid from deal: %w", err)
		}

		if sectorSealState == db.SealStateRemoved {
			// Announce deals that are no longer unsealed to indexer
			announceCid, err := m.idxprov.AnnounceBoostDealRemoved(ctx, propCid)
			if err != nil {
				// Check if the error is because the deal wasn't previously announced
				if !errors.Is(err, provider.ErrContextIDNotFound) {
					// There was some other error, write it to the log
					usmlog.Errorw("announcing deal removed to index provider",
						"deal id", deal.DealUuid, "error", err)
					continue
				}
			} else {
				usmlog.Infow("announced to index provider that deal has been removed",
					"deal id", deal.DealUuid, "sector id", deal.SectorID, "announce cid", announceCid.String())
			}
		} else {
			// Announce deals that have changed seal state to indexer
			md := metadata.GraphsyncFilecoinV1{
				PieceCID:      deal.ClientDealProposal.Proposal.PieceCID,
				FastRetrieval: sectorSealState == db.SealStateUnsealed,
				VerifiedDeal:  deal.ClientDealProposal.Proposal.VerifiedDeal,
			}
			announceCid, err := m.idxprov.announceBoostDealMetadata(ctx, md, propCid)
			if err != nil {
				// Check if the error is because the deal was already advertised
				if !errors.Is(err, provider.ErrAlreadyAdvertised) {
					// There was some other error, write it to the log
					usmlog.Errorf("announcing deal %s to index provider: %w", deal.DealUuid, err)
					continue
				}
			} else {
				usmlog.Infow("announced deal seal state to index provider",
					"deal id", deal.DealUuid, "sector id", deal.SectorID,
					"seal state", sectorSealState, "announce cid", announceCid.String())
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
