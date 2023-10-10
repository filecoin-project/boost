package legacy

import (
	"context"
	"io"

	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/go-state-types/abi"
)

type MountReader interface {
	io.ReadSeekCloser
	io.ReaderAt
}

type SectorAccessor interface {
	retrievalmarket.SectorAccessor

	UnsealSectorAt(ctx context.Context, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (MountReader, error)
}
