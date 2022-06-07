package client

import (
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/filecoin-project/boost/cmd/boostd-data/model"
	"github.com/ipfs/go-cid"
	logger "github.com/ipfs/go-log/v2"
	carindex "github.com/ipld/go-car/v2/index"
	mh "github.com/multiformats/go-multihash"
)

var log = logger.Logger("rpcclient")

type PieceMeta struct {
	client *rpc.Client
}

func NewPieceMeta() (*PieceMeta, error) {
	client, err := rpc.Dial("http://localhost:8089")
	if err != nil {
		return nil, err
	}

	return &PieceMeta{
		client: client,
	}, nil
}

func (s *PieceMeta) GetIterableIndex(pieceCid cid.Cid) ([]carindex.Record, error) {
	var resp []carindex.Record
	err := s.client.Call(&resp, "boostddata_getIterableIndex", pieceCid)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *PieceMeta) GetPieceDeals(pieceCid cid.Cid) ([]model.DealInfo, error) {
	var resp []model.DealInfo
	err := s.client.Call(&resp, "boostddata_getPieceDeals", pieceCid)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *PieceMeta) PiecesContainingMultihash(m mh.Multihash) ([]cid.Cid, error) {
	var resp []cid.Cid
	err := s.client.Call(&resp, "boostddata_piecesContainingMultihash", m)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *PieceMeta) AddDealForPiece(pieceCid cid.Cid, dealInfo model.DealInfo) error {
	return s.client.Call(nil, "boostddata_addDealForPiece", pieceCid, dealInfo)
}

func (s *PieceMeta) GetOffset(pieceCid cid.Cid, hash mh.Multihash) (uint64, error) {
	var resp uint64
	err := s.client.Call(&resp, "boostddata_getOffset", pieceCid, hash)
	if err != nil {
		return 0, err
	}

	return resp, nil
}
