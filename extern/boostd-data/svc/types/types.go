package types

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/filecoin-project/boostd-data/model"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

var ErrNotFound = errors.New("not found")

// IsNotFound just does a string match against the error message
// to check if it matches the ErrNotFound error message.
// We have to do string matching so that it can be used on errors that
// cross the RPC boundary (we can't use errors.Is)
func IsNotFound(err error) bool {
	return strings.Contains(err.Error(), ErrNotFound.Error())
}

type Service interface {
	AddDealForPiece(context.Context, cid.Cid, model.DealInfo) error
	AddIndex(context.Context, cid.Cid, []model.Record) error
	GetIndex(context.Context, cid.Cid) ([]model.Record, error)
	IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error)
	GetOffsetSize(context.Context, cid.Cid, mh.Multihash) (*model.OffsetSize, error)
	GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error)
	GetPieceDeals(context.Context, cid.Cid) ([]model.DealInfo, error)
	IndexedAt(context.Context, cid.Cid) (time.Time, error)
	PiecesContainingMultihash(context.Context, mh.Multihash) ([]cid.Cid, error)
}

type ServiceImpl interface {
	Service
	Start(ctx context.Context) error
}
