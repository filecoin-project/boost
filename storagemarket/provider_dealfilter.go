package storagemarket

import (
	"fmt"
	"time"

	"github.com/filecoin-project/boost/storagemarket/dealfilter"
	"github.com/filecoin-project/boost/storagemarket/funds"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/storagespace"
	"github.com/filecoin-project/boost/storagemarket/types"
)

func (p *Provider) getDealFilterParams(deal *types.ProviderDealState) (*dealfilter.DealFilterParams, *acceptError) {

	params := types.DealParams{
		DealUUID:           deal.DealUuid,
		ClientDealProposal: deal.ClientDealProposal,
		DealDataRoot:       deal.DealDataRoot,
		Transfer:           deal.Transfer,
		IsOffline:          deal.IsOffline,
		RemoveUnsealedCopy: !deal.FastRetrieval,
		SkipIPNIAnnounce:   !deal.AnnounceToIPNI,
	}

	// Clear transfer params in case it contains sensitive information
	// (eg Authorization header)
	params.Transfer.Params = []byte{}

	// If no external deal filter is set then return empty value for SealingPipelineState,
	// FundsState and StorageState to shorten the execution. This also avoids the expensive
	// p.sealingPipelineStatus() call
	if p.config.StorageFilter == "" {
		return &dealfilter.DealFilterParams{
			DealParams:           params,
			SealingPipelineState: sealingpipeline.Status{},
			FundsState:           funds.Status{},
			StorageState:         storagespace.Status{},
		}, nil
	}

	// Get the status of funds in the collateral and publish message wallets
	fundsStatus, err := funds.GetStatus(p.ctx, p.fundManager)
	if err != nil {
		return nil, &acceptError{
			error:         fmt.Errorf("storage deal filter: failed to fetch funds status: %w", err),
			reason:        "server error: storage deal filter: getting funds status",
			isSevereError: true,
		}
	}

	// Get the status of storage space
	storageStatus, err := storagespace.GetStatus(p.ctx, p.storageManager, p.dealsDB)
	if err != nil {
		return nil, &acceptError{
			error:         fmt.Errorf("storage deal filter: failed to fetch storage status: %w", err),
			reason:        "server error: storage deal filter: getting storage status",
			isSevereError: true,
		}
	}

	// Check cached sealing pipeline status and error
	sealingStatus, err := p.sealingPipelineStatus()
	if err != nil {
		return nil, &acceptError{
			error:         fmt.Errorf("storage deal filter: failed to fetch sealing pipeline status: %w", err),
			reason:        "server error: storage deal filter: getting sealing status",
			isSevereError: true,
		}
	}

	return &dealfilter.DealFilterParams{
		DealParams:           params,
		SealingPipelineState: sealingStatus,
		FundsState:           *fundsStatus,
		StorageState:         *storageStatus,
	}, nil
}

// sealingPipelineStatus updates the SealingPipelineCache to reduce constant sealingpipeline.GetStatus calls
// to the lotus-miner. This is to speed up the deal filter processing
func (p *Provider) sealingPipelineStatus() (sealingpipeline.Status, error) {

	if time.Now().After(p.spsCache.CacheTime.Add(p.config.SealingPipelineCacheTimeout)) || p.spsCache.CacheError != nil {
		sealingStatus, err := sealingpipeline.GetStatus(p.ctx, p.sps)
		if err != nil {
			p.spsCache.CacheError = err
			p.spsCache.CacheTime = time.Now()
		} else {
			p.spsCache.Status = *sealingStatus
			p.spsCache.CacheTime = time.Now()
			p.spsCache.CacheError = nil
		}
		return p.spsCache.Status, p.spsCache.CacheError
	}

	return p.spsCache.Status, p.spsCache.CacheError
}
