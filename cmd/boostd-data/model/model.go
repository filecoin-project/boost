package model

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
)

type DealInfo struct {
	DealUuid    uuid.UUID
	SectorID    abi.SectorNumber
	PieceOffset abi.PaddedPieceSize
	PieceLength abi.PaddedPieceSize
	// The size of the CAR file without zero-padding.
	// This value may be zero if the size is unknown.
	CarLength uint64

	// If we don't have CarLength, we have to iterate over all offsets, get the largest offset and sum it with length.
}

type Metadata struct {
	Cursor    uint64 `json:"cursor"`
	IsIndexed bool   `json:"is_indexed"`
}
