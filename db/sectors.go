package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
)

type SealState string

const SealStateSealed SealState = "Sealed"
const SealStateUnsealed SealState = "Unsealed"
const SealStateRemoved SealState = "Removed"
const SealStateCache SealState = "Cache"

type SectorState struct {
	SectorID  abi.SectorID
	UpdatedAt time.Time
	SealState SealState
}

type SectorStateDB struct {
	db *sql.DB
}

func NewSectorStateDB(db *sql.DB) *SectorStateDB {
	return &SectorStateDB{db}
}

func (sdb *SectorStateDB) List(ctx context.Context) ([]SectorState, error) {
	qry := "SELECT MinerID, SectorID, UpdatedAt, SealState FROM SectorState"
	rows, err := sdb.db.QueryContext(ctx, qry)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	states := make([]SectorState, 0, 16)
	for rows.Next() {
		var state SectorState
		err := rows.Scan(&state.SectorID.Miner, &state.SectorID.Number, &state.UpdatedAt, &state.SealState)

		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return states, nil
}

func (sdb *SectorStateDB) Update(ctx context.Context, sectorID abi.SectorID, SealState SealState) error {
	now := time.Now()
	qry := "REPLACE INTO SectorState (MinerID, SectorID, UpdatedAt, SealState) VALUES (?, ?, ?, ?)"
	_, err := sdb.db.ExecContext(ctx, qry, sectorID.Miner, sectorID.Number, now, SealState)
	return err
}
