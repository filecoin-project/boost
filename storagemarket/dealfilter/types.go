package dealfilter

import (
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
}
