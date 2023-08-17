package types

import (
	"fmt"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	cid "github.com/ipfs/go-cid"
)

// DirectDataEntry is the local state tracked for direct data onboard.
type DirectDataEntry struct {
	// ID is an unique uuid
	ID uuid.UUID
	// CreatedAt is the time at which the deal was stored
	CreatedAt time.Time

	PieceCID  cid.Cid
	PieceSize abi.PaddedPieceSize
	Client    address.Address
	Provider  address.Address

	// CleanupData indicates whether to remove the data for a deal after the deal has been added to a sector.
	// This is always true for online deals, and can be set as a flag for offline deals.
	CleanupData bool

	// InboundCARPath is the file-path where the storage provider will persist the CAR file sent by the client.
	InboundFilePath string

	// sector packing info
	SectorID abi.SectorNumber
	Offset   abi.PaddedPieceSize
	Length   abi.PaddedPieceSize

	StartEpoch abi.ChainEpoch
	EndEpoch   abi.ChainEpoch

	// set if there's an error
	Err string
	// if there was an error, indicates whether and how to retry (auto / manual)
	Retry DealRetryType

	// Keep unsealed copy of the data
	FastRetrieval bool

	//Announce deal to the IPNI(Index Provider)
	AnnounceToIPNI bool
}

func (d *DirectDataEntry) String() string {
	return fmt.Sprintf("%+v", *d)
}
