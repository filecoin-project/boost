package types

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

var ErrNotFound = errors.New("not found")

// IsNotFound just does a string match against the error message
// to check if it matches the ErrNotFound error message.
// We have to do string matching so that it can be used on errors that
// cross the RPC boundary (we can't use errors.Is)
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), ErrNotFound.Error())
}

type IndexRecord struct {
	model.Record
	Error error `json:"e,omitempty"`
}

type AddIndexProgress struct {
	Progress float64 `json:"p"`
	Err      string  `json:"e,omitempty"`
}

type ScanProgress struct {
	Progress float64   `json:"p"`
	LastScan time.Time `json:"l"`
}

type FlaggedPiecesListFilter struct {
	MinerAddr       address.Address
	HasUnsealedCopy bool
}

type Service interface {
	AddDealForPiece(context.Context, cid.Cid, model.DealInfo) error
	AddIndex(context.Context, cid.Cid, []model.Record, bool) <-chan AddIndexProgress
	GetIndex(context.Context, cid.Cid) (<-chan IndexRecord, error)
	IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error)
	IsCompleteIndex(ctx context.Context, pieceCid cid.Cid) (bool, error)
	GetOffsetSize(context.Context, cid.Cid, mh.Multihash) (*model.OffsetSize, error)
	ListPieces(ctx context.Context) ([]cid.Cid, error)
	GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error)
	GetPieceDeals(context.Context, cid.Cid) ([]model.DealInfo, error)
	IndexedAt(context.Context, cid.Cid) (time.Time, error)
	PiecesContainingMultihash(context.Context, mh.Multihash) ([]cid.Cid, error)
	RemoveDealForPiece(context.Context, cid.Cid, string) error
	RemovePieceMetadata(context.Context, cid.Cid) error
	RemoveIndexes(context.Context, cid.Cid) error
	NextPiecesToCheck(ctx context.Context, maddr address.Address) ([]cid.Cid, error)
	FlagPiece(ctx context.Context, pieceCid cid.Cid, hasUnsealedCopy bool, maddr address.Address) error
	UnflagPiece(ctx context.Context, pieceCid cid.Cid, maddr address.Address) error
	FlaggedPiecesList(ctx context.Context, filter *FlaggedPiecesListFilter, cursor *time.Time, offset int, limit int) ([]model.FlaggedPiece, error)
	FlaggedPiecesCount(ctx context.Context, filter *FlaggedPiecesListFilter) (int, error)
}

type ServiceImpl interface {
	Service
	Start(ctx context.Context) error
}
