package client

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/go-jsonrpc"
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
	GetOffsetSize             func(cid.Cid, mh.Multihash) (*model.OffsetSize, error)
	GetPieceDeals             func(cid.Cid) ([]model.DealInfo, error)
	GetRecords                func(cid.Cid) ([]model.Record, error)
	IndexedAt                 func(cid.Cid) (time.Time, error)
	PiecesContainingMultihash func(mh.Multihash) ([]cid.Cid, error)
}

type Store struct {
	client Client
	closer jsonrpc.ClientCloser
}

func NewStore() *Store {
	return &Store{}
}

func (s *Store) Dial(ctx context.Context, addr string) error {
	var client Client

	closer, err := jsonrpc.NewClient(ctx, addr, "boostddata", &client, nil)
	if err != nil {
		return fmt.Errorf("dialing boostd-data server: %w", err)
	}

	s.client = client
	s.closer = closer
	return nil
}

func (s *Store) Close(_ context.Context) {
	s.closer()
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) (index.Index, error) {
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

func (s *Store) GetRecords(ctx context.Context, pieceCid cid.Cid) ([]model.Record, error) {
	resp, err := s.client.GetIndex(pieceCid)
	if err != nil {
		return nil, err
	}

	log.Debugw("get-index", "piece-cid", pieceCid, "records", len(resp))

	return resp, nil
}

func (s *Store) GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error) {
	return s.client.GetPieceDeals(pieceCid)
}

func (s *Store) PiecesContaining(ctx context.Context, m mh.Multihash) ([]cid.Cid, error) {
	return s.client.PiecesContainingMultihash(m)
}

func (s *Store) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	return s.client.AddDealForPiece(pieceCid, dealInfo)
}

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record) error {
	log.Debugw("add-index", "piece-cid", pieceCid, "records", len(records))

	return s.client.AddIndex(pieceCid, records)
}

func (s *Store) IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	t, err := s.client.IndexedAt(pieceCid)
	if err != nil {
		return false, err
	}
	return !t.IsZero(), nil
}

func (s *Store) IndexedAt(ctx context.Context, pieceCid cid.Cid) (time.Time, error) {
	return s.client.IndexedAt(pieceCid)
}

func (s *Store) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	return s.client.GetOffsetSize(pieceCid, hash)
}
