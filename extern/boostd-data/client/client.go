package client

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/svc/types"
	"github.com/filecoin-project/go-address"
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
		AddIndex                  func(context.Context, cid.Cid, []model.Record, bool) <-chan types.AddIndexProgress
		IsIndexed                 func(ctx context.Context, pieceCid cid.Cid) (bool, error)
		IsCompleteIndex           func(ctx context.Context, pieceCid cid.Cid) (bool, error)
		GetIndex                  func(context.Context, cid.Cid) (<-chan types.IndexRecord, error)
		GetOffsetSize             func(context.Context, cid.Cid, mh.Multihash) (*model.OffsetSize, error)
		ListPieces                func(ctx context.Context) ([]cid.Cid, error)
		PiecesCount               func(ctx context.Context, maddr address.Address) (int, error)
		ScanProgress              func(ctx context.Context, maddr address.Address) (*types.ScanProgress, error)
		GetPieceMetadata          func(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error)
		GetPieceDeals             func(context.Context, cid.Cid) ([]model.DealInfo, error)
		IndexedAt                 func(context.Context, cid.Cid) (time.Time, error)
		PiecesContainingMultihash func(context.Context, mh.Multihash) ([]cid.Cid, error)
		RemoveDealForPiece        func(context.Context, cid.Cid, string) error
		RemovePieceMetadata       func(context.Context, cid.Cid) error
		RemoveIndexes             func(context.Context, cid.Cid) error
		NextPiecesToCheck         func(ctx context.Context, maddr address.Address) ([]cid.Cid, error)
		FlagPiece                 func(ctx context.Context, pieceCid cid.Cid, hasUnsealedDeal bool, maddr address.Address) error
		UnflagPiece               func(ctx context.Context, pieceCid cid.Cid, maddr address.Address) error
		FlaggedPiecesList         func(ctx context.Context, filter *types.FlaggedPiecesListFilter, cursor *time.Time, offset int, limit int) ([]model.FlaggedPiece, error)
		FlaggedPiecesCount        func(ctx context.Context, filter *types.FlaggedPiecesListFilter) (int, error)
	}
	closer   jsonrpc.ClientCloser
	dialOpts []jsonrpc.Option
}

func NewStore(dialOpts ...jsonrpc.Option) *Store {
	return &Store{dialOpts: dialOpts}
}

func (s *Store) Dial(ctx context.Context, addr string) error {
	var err error
	s.closer, err = jsonrpc.NewMergeClient(ctx, addr, "boostddata", []interface{}{&s.client}, nil, s.dialOpts...)
	if err != nil {
		return fmt.Errorf("dialing local index directory server: %w", err)
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
	for r := range resp {
		if r.Error != nil {
			return nil, r.Error
		}
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

	var records []model.Record
	for r := range resp {
		if r.Error != nil {
			return nil, r.Error
		}
		records = append(records, r.Record)
	}

	return records, nil
}

func (s *Store) GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error) {
	return s.client.GetPieceMetadata(ctx, pieceCid)
}

func (s *Store) GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error) {
	return s.client.GetPieceDeals(ctx, pieceCid)
}

func (s *Store) PiecesContainingMultihash(ctx context.Context, m mh.Multihash) ([]cid.Cid, error) {
	return s.client.PiecesContainingMultihash(ctx, m)
}

func (s *Store) AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error {
	return s.client.AddDealForPiece(ctx, pieceCid, dealInfo)
}

func (s *Store) AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record, isCompleteIndex bool) error {
	log.Debugw("add-index", "piece-cid", pieceCid, "records", len(records))

	respch := s.client.AddIndex(ctx, pieceCid, records, isCompleteIndex)
	for resp := range respch {
		if resp.Err != "" {
			return fmt.Errorf("add index with piece cid %s: %s", pieceCid, resp.Err)
		}
		//fmt.Printf("%s: Percent complete: %f%%\n", time.Now(), resp.Progress*100)
	}
	return nil
}

func (s *Store) IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	return s.client.IsIndexed(ctx, pieceCid)
}

func (s *Store) IsCompleteIndex(ctx context.Context, pieceCid cid.Cid) (bool, error) {
	return s.client.IsCompleteIndex(ctx, pieceCid)
}

func (s *Store) IndexedAt(ctx context.Context, pieceCid cid.Cid) (time.Time, error) {
	return s.client.IndexedAt(ctx, pieceCid)
}

func (s *Store) GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash mh.Multihash) (*model.OffsetSize, error) {
	return s.client.GetOffsetSize(ctx, pieceCid, hash)
}

func (s *Store) RemoveDealForPiece(ctx context.Context, pieceCid cid.Cid, dealId string) error {
	return s.client.RemoveDealForPiece(ctx, pieceCid, dealId)
}

func (s *Store) RemovePieceMetadata(ctx context.Context, pieceCid cid.Cid) error {
	return s.client.RemovePieceMetadata(ctx, pieceCid)
}

func (s *Store) RemoveIndexes(ctx context.Context, pieceCid cid.Cid) error {
	return s.client.RemoveIndexes(ctx, pieceCid)
}

func (s *Store) ListPieces(ctx context.Context) ([]cid.Cid, error) {
	return s.client.ListPieces(ctx)
}

func (s *Store) PiecesCount(ctx context.Context, maddr address.Address) (int, error) {
	return s.client.PiecesCount(ctx, maddr)
}

func (s *Store) ScanProgress(ctx context.Context, maddr address.Address) (*types.ScanProgress, error) {
	return s.client.ScanProgress(ctx, maddr)
}

func (s *Store) NextPiecesToCheck(ctx context.Context, maddr address.Address) ([]cid.Cid, error) {
	return s.client.NextPiecesToCheck(ctx, maddr)
}

func (s *Store) FlagPiece(ctx context.Context, pieceCid cid.Cid, hasUnsealedDeal bool, maddr address.Address) error {
	return s.client.FlagPiece(ctx, pieceCid, hasUnsealedDeal, maddr)
}

func (s *Store) UnflagPiece(ctx context.Context, pieceCid cid.Cid, maddr address.Address) error {
	return s.client.UnflagPiece(ctx, pieceCid, maddr)
}

func (s *Store) FlaggedPiecesList(ctx context.Context, filter *types.FlaggedPiecesListFilter, cursor *time.Time, offset int, limit int) ([]model.FlaggedPiece, error) {
	return s.client.FlaggedPiecesList(ctx, filter, cursor, offset, limit)
}

func (s *Store) FlaggedPiecesCount(ctx context.Context, filter *types.FlaggedPiecesListFilter) (int, error) {
	return s.client.FlaggedPiecesCount(ctx, filter)
}
