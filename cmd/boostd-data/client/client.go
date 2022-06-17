package client

import (
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/filecoin-project/boost/cmd/boostd-data/model"
	"github.com/ipfs/go-cid"
	logger "github.com/ipfs/go-log/v2"
	carindex "github.com/ipld/go-car/v2/index"
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

func (s *Store) GetIndex(pieceCid cid.Cid) ([]carindex.Record, error) {
	var resp []carindex.Record
	err := s.client.Call(&resp, "boostddata_getIndex", pieceCid)
	if err != nil {
		return nil, err
	}

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
	return s.client.Call(nil, "boostddata_addIndex", pieceCid, records)
}

func (s *Store) IsIndexed(pieceCid cid.Cid) (bool, error) {
	var resp bool
	err := s.client.Call(&resp, "boostddata_isIndexed", pieceCid)
	if err != nil {
		return false, err
	}
	return resp, nil
}

func (s *Store) GetOffset(pieceCid cid.Cid, hash mh.Multihash) (uint64, error) {
	var resp uint64
	err := s.client.Call(&resp, "boostddata_getOffset", pieceCid, hash)
	if err != nil {
		return 0, err
	}

	return resp, nil
}
