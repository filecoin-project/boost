package client

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/ipfs/go-cid"
	logger "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
)

var log = logger.Logger("boostd-data-client")

type Store struct {
	client *rpc.Client
}

func NewStore(addr string) (*Store, error) {
	client, err := rpc.Dial(addr)
	if err != nil {
		return nil, err
	}

	return &Store{
		client: client,
	}, nil
}

func (s *Store) GetIndex(ctx context.Context, pieceCid cid.Cid) (index.Index, error) {
	var resp []model.Record
	err := s.client.CallContext(ctx, &resp, "boostddata_getIndex", pieceCid)
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
	var resp []model.Record
	err := s.client.CallContext(ctx, &resp, "boostddata_getIndex", pieceCid)
	if err != nil {
		return nil, err
	}

	log.Debugw("get-index", "piece-cid", pieceCid, "records", len(resp))

	return resp, nil
}

func (s *Store) GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error) {
	var resp []model.DealInfo
	err := s.client.CallContext(ctx, &resp, "boostddata_getPieceDeals", pieceCid)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Store) PiecesContaining(ctx context.Context, m mh.Multihash) ([]cid.Cid, error) {
	var resp []cid.Cid
	err := s.client.CallContext(ctx, &resp, "boostddata_piecesContainingMultihash", m)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Store) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	return s.client.CallContext(ctx, nil, "boostddata_addDealForPiece", pieceCid, dealInfo)
}

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record) error {
	log.Debugw("add-index", "piece-cid", pieceCid, "records", len(records))

	return s.client.CallContext(ctx, nil, "boostddata_addIndex", pieceCid, records)
}

func (s *Store) IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	var t time.Time

	err := s.client.CallContext(ctx, &t, "boostddata_indexedAt", pieceCid)
	if err != nil {
		return false, err
	}
	return !t.IsZero(), nil
}

func (s *Store) IndexedAt(ctx context.Context, pieceCid cid.Cid) (time.Time, error) {
	var ts time.Time
	err := s.client.CallContext(ctx, &ts, "boostddata_indexedAt", pieceCid)
	if err != nil {
		return time.Time{}, err
	}
	return ts, nil
}

func (s *Store) GetOffset(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (uint64, error) {
	var resp uint64
	err := s.client.CallContext(ctx, &resp, "boostddata_getOffset", pieceCid, hash)
	if err != nil {
		return 0, err
	}

	return resp, nil
}
