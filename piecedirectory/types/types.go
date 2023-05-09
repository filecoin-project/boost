package types

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
)

//go:generate go run github.com/golang/mock/mockgen -destination=mocks/piecedirectory.go -package=mock_piecedirectory . SectionReader,PieceReader,Store

type SectionReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

var ErrSealed = errors.New("sector is not unsealed")

type PieceReader interface {
	// GetReader returns a reader over a piece. If there is no unsealed copy, returns ErrSealed.
	GetReader(ctx context.Context, id abi.SectorNumber, offset abi.PaddedPieceSize, length abi.PaddedPieceSize) (SectionReader, error)
}

type Store interface {
	AddDealForPiece(ctx context.Context, pieceCid cid.Cid, dealInfo model.DealInfo) error
	AddIndex(ctx context.Context, pieceCid cid.Cid, records []model.Record, isCompleteIndex bool) error
	IsIndexed(ctx context.Context, pieceCid cid.Cid) (bool, error)
	IsCompleteIndex(ctx context.Context, pieceCid cid.Cid) (bool, error)
	GetIndex(ctx context.Context, pieceCid cid.Cid) (index.Index, error)
	GetOffsetSize(ctx context.Context, pieceCid cid.Cid, hash multihash.Multihash) (*model.OffsetSize, error)
	GetPieceMetadata(ctx context.Context, pieceCid cid.Cid) (model.Metadata, error)
	ListPieces(ctx context.Context) ([]cid.Cid, error)
	GetPieceDeals(ctx context.Context, pieceCid cid.Cid) ([]model.DealInfo, error)
	PiecesContainingMultihash(ctx context.Context, m multihash.Multihash) ([]cid.Cid, error)
	RemoveDealForPiece(context.Context, cid.Cid, string) error
	RemovePieceMetadata(context.Context, cid.Cid) error
	RemoveIndexes(context.Context, cid.Cid) error
	NextPiecesToCheck(ctx context.Context) ([]cid.Cid, error)
	FlagPiece(ctx context.Context, pieceCid cid.Cid) error
	UnflagPiece(ctx context.Context, pieceCid cid.Cid) error
	FlaggedPiecesList(ctx context.Context, cursor *time.Time, offset int, limit int) ([]model.FlaggedPiece, error)
	FlaggedPiecesCount(ctx context.Context) (int, error)
}

// PieceDirMetadata has the db metadata info and a flag to indicate if this
// process is currently indexing the piece
type PieceDirMetadata struct {
	model.Metadata
	Indexing bool
}
