package storagemarket

import (
	"fmt"

	"github.com/filecoin-project/boost/storagemarket/dealfilter"
	"github.com/filecoin-project/boost/storagemarket/funds"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/storagespace"
	"github.com/filecoin-project/boost/storagemarket/types"
)

func (p *Provider) getDealFilterParams(deal *types.ProviderDealState) (*dealfilter.DealFilterParams, *acceptError) {
	// Get current sealing pipeline status
	sealingStatus, err := sealingpipeline.GetStatus(p.ctx, p.sps)
	if err != nil {
		return nil, &acceptError{
			error:         fmt.Errorf("storage deal filter: failed to fetch sealing pipeline status: %w", err),
			reason:        "server error: storage deal filter: getting sealing status",
			isSevereError: true,
		}
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

	params := types.DealParams{
		DealUUID:           deal.DealUuid,
		ClientDealProposal: deal.ClientDealProposal,
		DealDataRoot:       deal.DealDataRoot,
		Transfer:           deal.Transfer,
	}

	// Clear transfer params in case it contains sensitive information
	// (eg Authorization header)
	params.Transfer.Params = []byte{}

	return &dealfilter.DealFilterParams{
		DealParams:           params,
		SealingPipelineState: *sealingStatus,
		FundsState:           *fundsStatus,
		StorageState:         *storageStatus,
	}, nil
}
