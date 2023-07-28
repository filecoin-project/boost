package server

import (
	"context"
	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
)

type AskGetter interface {
	GetAsk() *retrievalmarket.Ask
}

type SectorAccessor interface {
	IsUnsealed(ctx context.Context, minerAddr address.Address, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error)
}
