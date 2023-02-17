package db

import (
	"context"
	"database/sql"
	"github.com/filecoin-project/go-state-types/abi"
	"strings"
	"time"
)

type SectorState struct {
	SectorID  abi.SectorID
	UpdatedAt time.Time
	Unsealed  bool
}

type SectorStateDB struct {
	db *sql.DB
}

func NewSectorStateDB(db *sql.DB) *SectorStateDB {
	return &SectorStateDB{db}
}

func (sdb *SectorStateDB) List(ctx context.Context) ([]SectorState, error) {
	qry := "SELECT SectorID, UpdatedAt, Unsealed FROM SectorState"
	rows, err := sdb.db.QueryContext(ctx, qry)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states := make([]SectorState, 0, 16)
	for rows.Next() {
		var state SectorState
		err := rows.Scan(&state.SectorID.Number, &state.UpdatedAt, &state.Unsealed)

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

func (sdb *SectorStateDB) Get(ctx context.Context, sectorID abi.SectorID) (*SectorState, error) {
	qry := "SELECT UpdatedAt, Unsealed FROM SectorState WHERE SectorID = ?"
	row := sdb.db.QueryRowContext(ctx, qry, sectorID.Number)

	state := &SectorState{SectorID: sectorID}
	err := row.Scan(&state.UpdatedAt, &state.Unsealed)
	if err != nil {
		return nil, err
	}

	return state, nil
}

func (sdb *SectorStateDB) Update(ctx context.Context, updates map[abi.SectorID]bool) error {
	if len(updates) == 0 {
		return nil
	}

	now := time.Now()
	qry := "REPLACE INTO SectorState (SectorID, UpdatedAt, Unsealed) "

	var vals []string
	var args []interface{}
	for sectorID, isUnsealed := range updates {
		vals = append(vals, "(?,?,?)")
		args = append(args, sectorID.Number, now, isUnsealed)
	}
	qry += "VALUES " + strings.Join(vals, ",")
	_, err := sdb.db.ExecContext(ctx, qry, args...)
	return err
}
