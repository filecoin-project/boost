package types

import (
	"context"
	"errors"
	"io"

	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
)

//go:generate go run github.com/golang/mock/mockgen -destination=mocks/piecedirectory.go -package=mock_piecedirectory . PieceReader

type SectionReader interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

var ErrSealed = errors.New("sector is not unsealed")

type PieceReader interface {
	// GetReader returns a reader over a piece. If there is no unsealed copy, returns ErrSealed.
	GetReader(ctx context.Context, minerAddr address.Address, id abi.SectorNumber, offset abi.PaddedPieceSize, length abi.PaddedPieceSize) (SectionReader, error)
}

// PieceDirMetadata has the db metadata info and a flag to indicate if this
// process is currently indexing the piece
type PieceDirMetadata struct {
	model.Metadata
	Indexing bool
}
