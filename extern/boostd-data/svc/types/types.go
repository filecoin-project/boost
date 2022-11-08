package types

import (
	"context"
	"time"

	"github.com/filecoin-project/boostd-data/model"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

type Service interface {
	AddDealForPiece(context.Context, cid.Cid, model.DealInfo) error
	AddIndex(context.Context, cid.Cid, []model.Record) error
	GetIndex(context.Context, cid.Cid) ([]model.Record, error)
	IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error)
	GetOffsetSize(context.Context, cid.Cid, mh.Multihash) (*model.OffsetSize, error)
	GetPieceDeals(context.Context, cid.Cid) ([]model.DealInfo, error)
	IndexedAt(context.Context, cid.Cid) (time.Time, error)
	PiecesContainingMultihash(context.Context, mh.Multihash) ([]cid.Cid, error)
}

type ServiceImpl interface {
	Service
	Start(ctx context.Context) error
}
