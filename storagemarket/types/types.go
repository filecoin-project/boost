package types

import (
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
)

// StorageAsk defines the parameters by which a miner will choose to accept or
// reject a deal. Note: making a storage deal proposal which matches the miner's
// ask is a precondition, but not sufficient to ensure the deal is accepted (the
// storage provider may run its own decision logic).
type StorageAsk struct {
	// Price per GiB / Epoch
	Price         abi.TokenAmount
	VerifiedPrice abi.TokenAmount

	MinPieceSize abi.PaddedPieceSize
	MaxPieceSize abi.PaddedPieceSize
	Miner        address.Address
}

// ClientDealParams are the deal params sent by the client
type ClientDealParams struct {
	DealUuid           uuid.UUID
	MinerPeerID        peer.ID
	ClientPeerID       peer.ID
	ClientDealProposal market.ClientDealProposal

	DealDataRoot cid.Cid
	TransferURL  string
}

// ProviderDealRejectionInfo is the information sent by the Storage Provider to the Client when it rejects a valid deal.
type ProviderDealRejectionInfo struct {
	Reason  string
	Backoff time.Duration
}

type ProviderDealEvent struct {
	DealUuid uuid.UUID
	// ...
	//
}

type DataTransferEvent struct {
	DealUuid uuid.UUID
	// ...
	//
	// TransferEvent (Started, Progress, Finished) ?
	PercentComplete int
}
