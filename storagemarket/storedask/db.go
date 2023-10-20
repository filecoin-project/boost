package storedask

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"path"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
	"go.uber.org/fx"
)

const AskDBName = "ask.db"

//go:embed create_ask_db.sql
var createAskDBSQL string

func createAskTable(ctx context.Context, askDB *sql.DB) error {
	if _, err := askDB.ExecContext(ctx, createAskDBSQL); err != nil {
		return fmt.Errorf("failed to create tables in ask DB: %w", err)
	}
	return nil
}

type StorageAskDB struct {
	db *sql.DB
}

func NewStorageAskDB(r lotus_repo.LockedRepo) (*StorageAskDB, error) {
	dbPath := path.Join(r.Path(), AskDBName+"?cache=shared")
	d, err := db.SqlDB(dbPath)
	if err != nil {
		return nil, err
	}
	return &StorageAskDB{db: d}, nil
}

func CreateAskTables(lc fx.Lifecycle, db *StorageAskDB) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return createAskTable(ctx, db.db)
		},
	})
}

func (s *StorageAskDB) Update(ctx context.Context, ask legacytypes.StorageAsk) error {
	var minerString string
	qry := "SELECT Miner FROM StorageAsk WHERE Miner=?;"
	row := s.db.QueryRowContext(ctx, qry, ask.Miner.String())
	err := row.Scan(&minerString)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return s.set(ctx, ask)
	case err != nil:
		return err
	default:
		return s.update(ctx, ask)
	}
}

func (s *StorageAskDB) set(ctx context.Context, ask legacytypes.StorageAsk) error {
	qry := "INSERT INTO StorageAsk (Price, VerifiedPrice, MinPieceSize, MaxPieceSize, Miner, TS, Expiry, SeqNo) "
	qry += "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
	values := []interface{}{ask.Price.Int64(), ask.VerifiedPrice.Int64(), ask.MinPieceSize, ask.MaxPieceSize, ask.Miner.String(), ask.Timestamp, ask.Expiry, ask.SeqNo}
	_, err := s.db.ExecContext(ctx, qry, values...)
	return err
}

func (s *StorageAskDB) update(ctx context.Context, ask legacytypes.StorageAsk) error {
	qry := "UPDATE StorageAsk (Price, VerifiedPrice, MinPieceSize, MaxPieceSize, TS, Expiry, SeqNo) "
	qry += "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
	qry += "WHERE Miner=?"
	values := []interface{}{ask.Price.Int64(), ask.VerifiedPrice.Int64(), ask.MinPieceSize, ask.MaxPieceSize, ask.Timestamp, ask.Expiry, ask.SeqNo, ask.Miner.String()}
	_, err := s.db.ExecContext(ctx, qry, values...)
	return err
}

func (s *StorageAskDB) Get(ctx context.Context, miner address.Address) (legacytypes.StorageAsk, error) {
	var price, verifiedPrice, timestamp, expiry int64
	var minPieceSize, maxPieceSize, seqNo uint64
	var minerS string
	qry := "SELECT Price, VerifiedPrice, MinPieceSize, MaxPieceSize, Miner, TS, Expiry, SeqNo FROM StorageAsk WHERE Miner=?;"
	row := s.db.QueryRowContext(ctx, qry, miner.String())
	err := row.Scan(&price, &verifiedPrice, &minPieceSize, &maxPieceSize, &minerS, &timestamp, &expiry, &seqNo)
	if err != nil {
		return legacytypes.StorageAsk{}, err
	}

	m, err := address.NewFromString(minerS)
	if err != nil {
		return legacytypes.StorageAsk{}, fmt.Errorf("converting stored ask address")
	}

	if m != miner {
		return legacytypes.StorageAsk{}, fmt.Errorf("stored miner address does match the supplied address")
	}

	return legacytypes.StorageAsk{
		Price:         abi.NewTokenAmount(price),
		VerifiedPrice: abi.NewTokenAmount(verifiedPrice),
		Timestamp:     abi.ChainEpoch(timestamp),
		Expiry:        abi.ChainEpoch(expiry),
		Miner:         miner,
		MinPieceSize:  abi.PaddedPieceSize(minPieceSize),
		MaxPieceSize:  abi.PaddedPieceSize(maxPieceSize),
		SeqNo:         seqNo,
	}, nil
}
