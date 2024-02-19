package server

import (
	"context"

	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
)

type SectorAccessor interface {
	IsUnsealed(ctx context.Context, minerAddr address.Address, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error)
}

// DealDecider is a function that makes a decision about whether to accept a deal
type DealDecider func(ctx context.Context, state legacyretrievaltypes.ProviderDealState) (bool, string, error)
