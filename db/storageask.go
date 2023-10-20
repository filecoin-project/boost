package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
)

type StorageAskDB struct {
	db *sql.DB
}

func NewStorageAskDB(db *sql.DB) *StorageAskDB {
	return &StorageAskDB{db: db}
}

func (s *StorageAskDB) Update(ctx context.Context, ask legacytypes.StorageAsk) error {
	var minerString string
	qry := "SELECT Miner FROM StorageAsk WHERE Miner=?;"
	row := s.db.QueryRowContext(ctx, qry, ask.Miner.String())
	err := row.Scan(&minerString)
	switch {
	case err == sql.ErrNoRows:
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
	values := []interface{}{ask.Price, ask.VerifiedPrice, ask.MinPieceSize, ask.MaxPieceSize, ask.Miner.String(), ask.Timestamp, ask.Expiry, ask.SeqNo}
	_, err := s.db.ExecContext(ctx, qry, values...)
	return err
}

func (s *StorageAskDB) update(ctx context.Context, ask legacytypes.StorageAsk) error {
	qry := "UPDATE StorageAsk (Price, VerifiedPrice, MinPieceSize, MaxPieceSize, TS, Expiry, SeqNo) "
	qry += "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
	qry += "WHERE Miner=?"
	values := []interface{}{ask.Price, ask.VerifiedPrice, ask.MinPieceSize, ask.MaxPieceSize, ask.Timestamp, ask.Expiry, ask.SeqNo, ask.Miner.String()}
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
