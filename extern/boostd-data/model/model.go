package model

import (
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

// DealInfo is information about a single deal for a given piece
//                      PieceOffset
//                      v
// Sector        [..........................]
// Piece          ......[            ]......
// CAR            ......[      ]............
type DealInfo struct {
	DealUuid    uuid.UUID           `json:"deal_uuid"`
	ChainDealID abi.DealID          `json:"chain_deal_id"`
	SectorID    abi.SectorNumber    `json:"sector_id"`
	PieceOffset abi.PaddedPieceSize `json:"piece_offset"`
	PieceLength abi.PaddedPieceSize `json:"piece_length"`
	// The size of the CAR file without zero-padding.
	// This value may be zero if the size is unknown.
	CarLength uint64 `json:"car_length"`

	// If we don't have CarLength, we have to iterate over all offsets, get the largest offset and sum it with length.
}

// Metadata for PieceCid
type Metadata struct {
	Cursor    uint64     `json:"cursor"`
	IndexedAt time.Time  `json:"indexed_at"`
	Deals     []DealInfo `json:"deals"`
	Error     string     `json:"error"`
	ErrorType string     `json:"error_type"`
}

// Record is the information stored in the index for each block in a piece
type Record struct {
	Cid cid.Cid
	OffsetSize
}

type OffsetSize struct {
	// Offset is the offset into the CAR file of the section, where a section
	// is <section size><cid><block data>
	Offset uint64
	// Size is the size of the block data (not the whole section)
	Size uint64
}
