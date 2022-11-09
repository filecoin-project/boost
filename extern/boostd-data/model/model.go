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
	DealUuid    uuid.UUID           `json:"u"`
	ChainDealID abi.DealID          `json:"i"`
	SectorID    abi.SectorNumber    `json:"s"`
	PieceOffset abi.PaddedPieceSize `json:"o"`
	PieceLength abi.PaddedPieceSize `json:"l"`
	// The size of the CAR file without zero-padding.
	// This value may be zero if the size is unknown.
	CarLength uint64 `json:"c"`

	// If we don't have CarLength, we have to iterate over all offsets, get
	// the largest offset and sum it with length.
}

// Metadata for PieceCid
type Metadata struct {
	IndexedAt time.Time  `json:"i"`
	Deals     []DealInfo `json:"d"`
	Error     string     `json:"e"`
	ErrorType string     `json:"t"`
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
