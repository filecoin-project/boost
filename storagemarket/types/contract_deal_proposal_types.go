package types

import (
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/ipfs/go-cid"
)

type ContractDealProposal struct {
	PieceCID     cid.Cid
	PieceSize    abi.PaddedPieceSize
	VerifiedDeal bool
	Client       address.Address
	Provider     address.Address

	Label market.DealLabel

	StartEpoch           abi.ChainEpoch
	EndEpoch             abi.ChainEpoch
	StoragePricePerEpoch abi.TokenAmount

	ProviderCollateral abi.TokenAmount
	ClientCollateral   abi.TokenAmount

	Version string
	Params  []byte
}

type ContractParamsVersion1 struct {
	LocationRef        string
	CarSize            uint64
	SkipIpniAnnounce   bool
	RemoveUnsealedCopy bool
}
