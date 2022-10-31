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

type Store struct {
	client struct {
		AddDealForPiece           func(context.Context, cid.Cid, model.DealInfo) error
		AddIndex                  func(context.Context, cid.Cid, []model.Record) error
		GetIndex                  func(context.Context, cid.Cid) ([]model.Record, error)
		GetOffsetSize             func(context.Context, cid.Cid, mh.Multihash) (*model.OffsetSize, error)
		GetPieceDeals             func(context.Context, cid.Cid) ([]model.DealInfo, error)
		MarkIndexErrored          func(context.Context, cid.Cid, error) error
		IndexedAt                 func(context.Context, cid.Cid) (time.Time, error)
		PiecesContainingMultihash func(context.Context, mh.Multihash) ([]cid.Cid, error)
	}
	closer jsonrpc.ClientCloser
}

func NewStore() *Store {
	return &Store{}
}

func (s *Store) Dial(ctx context.Context, addr string) error {
	var err error
	s.closer, err = jsonrpc.NewClient(ctx, addr, "boostddata", &s.client, nil)
	if err != nil {
		return fmt.Errorf("dialing boostd-data server: %w", err)
	}
	return nil
}

func (s *Store) Close(_ context.Context) {
	if s != nil && s.closer != nil {
		s.closer()
	}
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) (index.Index, error) {
	resp, err := s.client.GetIndex(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	var records []index.Record
	for _, r := range resp {
		records = append(records, index.Record{
			Cid:    r.Cid,
			Offset: r.Offset,
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
	resp, err := s.client.GetIndex(ctx, pieceCid)
	if err != nil {
		return nil, err
	}

	log.Debugw("get-records", "piece-cid", pieceCid, "records", len(resp))

	return resp, nil
}

func (s *Store) GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error) {
	return s.client.GetPieceDeals(ctx, pieceCid)
}

func (s *Store) PiecesContaining(ctx context.Context, m mh.Multihash) ([]cid.Cid, error) {
	return s.client.PiecesContainingMultihash(ctx, m)
}

func (s *Store) MarkIndexErrored(ctx context.Context, pieceCid cid.Cid, err error) error {
	return s.client.MarkIndexErrored(ctx, pieceCid, err)
}

func (s *Store) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	return s.client.AddDealForPiece(ctx, pieceCid, dealInfo)
}

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record) error {
	log.Debugw("add-index", "piece-cid", pieceCid, "records", len(records))

	return s.client.AddIndex(ctx, pieceCid, records)
}

func (s *Store) IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	t, err := s.client.IndexedAt(ctx, pieceCid)
	if err != nil {
		return false, err
	}
	return !t.IsZero(), nil
}

func (s *Store) IndexedAt(ctx context.Context, pieceCid cid.Cid) (time.Time, error) {
	return s.client.IndexedAt(ctx, pieceCid)
}

func (s *Store) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	return s.client.GetOffsetSize(ctx, pieceCid, hash)
}
