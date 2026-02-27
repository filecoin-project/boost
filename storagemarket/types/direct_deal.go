package types

import (
	"fmt"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v13/miner"
	verifregtypes "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

// DirectDeal is the local state tracked for direct data onboard.
type DirectDeal struct {
	// ID is an unique uuid
	ID uuid.UUID
	// CreatedAt is the time at which the deal was stored
	CreatedAt time.Time

	PieceCID  cid.Cid
	PieceSize abi.PaddedPieceSize
	Client    address.Address
	Provider  address.Address

	AllocationID verifregtypes.AllocationId

	// CleanupData indicates whether to remove the data for a deal after the deal has been added to a sector.
	// This is always true for online deals, and can be set as a flag for offline deals.
	CleanupData bool

	// InboundCARPath is the file-path where the storage provider will persist the CAR file sent by the client.
	InboundFilePath string
	InboundFileSize int64

	// sector packing info
	SectorID abi.SectorNumber
	Offset   abi.PaddedPieceSize
	Length   abi.PaddedPieceSize

	// deal checkpoint in DB.
	Checkpoint dealcheckpoints.Checkpoint
	// CheckpointAt is the time at which the deal entered the last state
	CheckpointAt time.Time

	StartEpoch abi.ChainEpoch
	EndEpoch   abi.ChainEpoch

	// set if there's an error
	Err string
	// if there was an error, indicates whether and how to retry (auto / manual)
	Retry DealRetryType

	// Keep unsealed copy of the data
	KeepUnsealedCopy bool

	//Announce deal to the IPNI(Index Provider)
	AnnounceToIPNI bool

	// Notify actors with payload info upon prove commit
	Notifications []miner.DataActivationNotification
}

func (d *DirectDeal) String() string {
	return fmt.Sprintf("%+v", *d)
}
