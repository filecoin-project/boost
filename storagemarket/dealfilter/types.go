package dealfilter

import (
	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/boost/storagemarket/funds"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/storagespace"
	"github.com/filecoin-project/boost/storagemarket/types"
)

// DealFilterParams is the struct that gets passed to the Storage Deal Filter
type DealFilterParams struct {
	DealParams           types.DealParams
	SealingPipelineState sealingpipeline.Status
	FundsState           funds.Status
	StorageState         storagespace.Status
	// NetworkVersion is the chain version the deal is proposed on; a filter branching on VerifiedDeal
	// needs it because FIP-0118 leaves every deal unverified from nv29.
	NetworkVersion network.Version
}
