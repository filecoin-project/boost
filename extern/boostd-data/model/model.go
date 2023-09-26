package model

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
)

// DealInfo is information about a single deal for a given piece
// .                    PieceOffset
// .                    v
// Sector        [..........................]
// Piece          ......[            ]......
// CAR            ......[      ]............
type DealInfo struct {
	DealUuid    string              `json:"u"`
	IsLegacy    bool                `json:"y"`
	ChainDealID abi.DealID          `json:"i"`
	MinerAddr   address.Address     `json:"m"`
	SectorID    abi.SectorNumber    `json:"s"`
	PieceOffset abi.PaddedPieceSize `json:"o"`
	PieceLength abi.PaddedPieceSize `json:"l"`
	// The size of the CAR file without zero-padding.
	// This value may be zero if the size is unknown.
	// If we don't have CarLength, we have to iterate
	// over all offsets, get the largest offset and
	// sum it with length.
	CarLength uint64 `json:"c"`

	IsDirectDeal bool `json:"d"`
}

// Metadata for PieceCid
type Metadata struct {
	Version   string    `json:"v"`
	IndexedAt time.Time `json:"i"`
	// CompleteIndex indicates whether the index has all information or is
	// missing block size information. Note that indexes imported from the
	// dagstore do not have block size information (they only have block
	// offsets).
	CompleteIndex bool       `json:"c"`
	Deals         []DealInfo `json:"d"`
}

// Record is the information stored in the index for each block in a piece
type Record struct {
	Cid cid.Cid `json:"c"`
	OffsetSize
}

type OffsetSize struct {
	// Offset is the offset into the CAR file of the section, where a section
	// is <section size><cid><block data>
	Offset uint64 `json:"o"`
	// Size is the size of the block data (not the whole section)
	Size uint64 `json:"s"`
}

func (ofsz *OffsetSize) MarshallBase64() string {
	buf := make([]byte, 2*binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, ofsz.Offset)
	n += binary.PutUvarint(buf[n:], ofsz.Size)
	return base64.RawStdEncoding.EncodeToString(buf[:n])
}

func (ofsz *OffsetSize) UnmarshallBase64(str string) error {
	buf, err := base64.RawStdEncoding.DecodeString(str)
	if err != nil {
		return fmt.Errorf("decoding offset/size from base64 string: %w", err)
	}

	offset, n := binary.Uvarint(buf)
	size, _ := binary.Uvarint(buf[n:])

	ofsz.Offset = offset
	ofsz.Size = size

	return nil
}

// FlaggedPiece is a piece that has been flagged for the user's attention
// (eg because the index is missing)
type FlaggedPiece struct {
	MinerAddr       address.Address
	PieceCid        cid.Cid
	CreatedAt       time.Time
	UpdatedAt       time.Time
	HasUnsealedCopy bool
}
