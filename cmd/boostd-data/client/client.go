package client

import (
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/filecoin-project/boost/cmd/boostd-data/model"
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

func (s *Store) GetIndex(pieceCid cid.Cid) (index.Index, error) {
	var resp []model.Record
	err := s.client.Call(&resp, "boostddata_getIndex", pieceCid)
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
	var resp []model.Record
	err := s.client.Call(&resp, "boostddata_getIndex", pieceCid)
	if err != nil {
		return nil, err
	}

	log.Warnw("get-index", "piece-cid", pieceCid, "records", len(resp))

	return resp, nil
}

func (s *Store) GetPieceDeals(pieceCid cid.Cid) ([]model.DealInfo, error) {
	var resp []model.DealInfo
	err := s.client.Call(&resp, "boostddata_getPieceDeals", pieceCid)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Store) PiecesContaining(m mh.Multihash) ([]cid.Cid, error) {
	var resp []cid.Cid
	err := s.client.Call(&resp, "boostddata_piecesContainingMultihash", m)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Store) AddDealForPiece(pieceCid cid.Cid, dealInfo model.DealInfo) error {
	return s.client.Call(nil, "boostddata_addDealForPiece", pieceCid, dealInfo)
}

func (s *Store) AddIndex(pieceCid cid.Cid, records []model.Record) error {
	log.Warnw("add-index", "piece-cid", pieceCid, "records", len(records))

	return s.client.Call(nil, "boostddata_addIndex", pieceCid, records)
}

func (s *Store) IsIndexed(pieceCid cid.Cid) (bool, error) {
	var t time.Time

	err := s.client.Call(&t, "boostddata_indexedAt", pieceCid)
	if err != nil {
		return false, err
	}
	return !t.IsZero(), nil
}

func (s *Store) IndexedAt(pieceCid cid.Cid) (time.Time, error) {
	var ts time.Time
	err := s.client.Call(&ts, "boostddata_indexedAt", pieceCid)
	if err != nil {
		return time.Time{}, err
	}
	return ts, nil
}

func (s *Store) GetOffset(pieceCid cid.Cid, hash mh.Multihash) (uint64, error) {
	var resp uint64
	err := s.client.Call(&resp, "boostddata_getOffset", pieceCid, hash)
	if err != nil {
		return 0, err
	}

	return resp, nil
}
