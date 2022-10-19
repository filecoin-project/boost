package client

import (
	"context"
	"time"

	"github.com/filecoin-project/go-jsonrpc"

	"github.com/filecoin-project/boost/cmd/boostd-data/model"
	"github.com/ipfs/go-cid"
	logger "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
)

var log = logger.Logger("boostd-data-client")

type Client struct {
	AddDealForPiece           func(cid.Cid, model.DealInfo) error
	AddIndex                  func(cid.Cid, []model.Record) error
	GetIndex                  func(cid.Cid) ([]model.Record, error)
	GetOffset                 func(cid.Cid, mh.Multihash) (uint64, error)
	GetPieceDeals             func(cid.Cid) ([]model.DealInfo, error)
	GetRecords                func(cid.Cid) ([]model.Record, error)
	IndexedAt                 func(cid.Cid) (time.Time, error)
	PiecesContainingMultihash func(mh.Multihash) ([]cid.Cid, error)
}

type Store struct {
	client Client
}

func NewStore(addr string) (*Store, error) {
	var client Client

	closer, err := jsonrpc.NewClient(context.Background(), addr, "boostddata", &client, nil)
	if err != nil {
		return nil, err
	}
	defer closer()

	return &Store{
		client: client,
	}, nil
}

func (s *Store) GetIndex(pieceCid cid.Cid) (index.Index, error) {
	resp, err := s.client.GetIndex(pieceCid)
	if err != nil {
		return nil, err
	}

	//TODO: figure out how to remove this conversion
	var records []index.Record
	for _, r := range resp {
		records = append(records, index.Record{
			r.Cid,
			r.Offset,
		})
	}

	mis := make(index.MultihashIndexSorted)
	err = mis.Load(records)
	if err != nil {
		return nil, err
	}

	return &mis, nil
}

func (s *Store) GetRecords(pieceCid cid.Cid) ([]model.Record, error) {
	resp, err := s.client.GetIndex(pieceCid)
	if err != nil {
		return nil, err
	}

	log.Warnw("get-index", "piece-cid", pieceCid, "records", len(resp))

	return resp, nil
}

func (s *Store) GetPieceDeals(pieceCid cid.Cid) ([]model.DealInfo, error) {
	return s.client.GetPieceDeals(pieceCid)
}

func (s *Store) PiecesContaining(m mh.Multihash) ([]cid.Cid, error) {
	return s.client.PiecesContainingMultihash(m)
}

func (s *Store) AddDealForPiece(pieceCid cid.Cid, dealInfo model.DealInfo) error {
	return s.client.AddDealForPiece(pieceCid, dealInfo)
}

func (s *Store) AddIndex(pieceCid cid.Cid, records []model.Record) error {
	log.Warnw("add-index", "piece-cid", pieceCid, "records", len(records))

	return s.client.AddIndex(pieceCid, records)
}

func (s *Store) IsIndexed(pieceCid cid.Cid) (bool, error) {
	t, err := s.client.IndexedAt(pieceCid)
	if err != nil {
		return false, err
	}
	return !t.IsZero(), nil
}

func (s *Store) IndexedAt(pieceCid cid.Cid) (time.Time, error) {
	return s.client.IndexedAt(pieceCid)
}

func (s *Store) GetOffset(pieceCid cid.Cid, hash mh.Multihash) (uint64, error) {
	return s.client.GetOffset(pieceCid, hash)
}
