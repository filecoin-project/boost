package types

import (
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

// ProviderDealState is the local state tracked for a deal by a StorageProvider
type ProviderDealState struct {
	ClientDealProposal market.ClientDealProposal

	DealUuid   uuid.UUID
	Miner      peer.ID
	Client     peer.ID
	DealStatus Status

	DealDataRoot cid.Cid
	PieceCid     cid.Cid
	PieceSize    abi.UnpaddedPieceSize
}
