package storagemarket

import (
	"context"
	"fmt"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	"github.com/filecoin-project/boost/storagemarket/types"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	ltypes "github.com/filecoin-project/lotus/chain/types"
)

// DirectDealOnChain is Boost's whole answer to "did this data land".
type DirectDealOnChain int

const (
	// DirectDealSealing: the sealer has not finished with the sector.
	DirectDealSealing DirectDealOnChain = iota
	// DirectDealOnChainDone: the sector is on chain and commits the deal's piece.
	DirectDealOnChainDone
	// DirectDealSectorGone: the sector reached a sealing state its data does not survive.
	DirectDealSectorGone
	// DirectDealNotVisible: this node's chain does not show the finished sector; not a verdict.
	DirectDealNotVisible
	// DirectDealNoClaim: a pre-nv29 sector on chain carries no claim for the allocation.
	DirectDealNoClaim
	// DirectDealClaimElsewhere: the allocation's claim names a different sector.
	DirectDealClaimElsewhere
)

func (s DirectDealOnChain) String() string {
	switch s {
	case DirectDealSealing:
		return "sealing"
	case DirectDealOnChainDone:
		return "on chain"
	case DirectDealSectorGone:
		return "sector gone"
	case DirectDealNotVisible:
		return "not on this node's chain yet"
	case DirectDealNoClaim:
		return "no claim"
	case DirectDealClaimElsewhere:
		return "claim on another sector"
	}
	return "unknown"
}

// DirectDealStatus reports where a direct deal stands, from the sealer's record for the
// deal's sector and this node's chain.
//
// A sector onboarded from nv29 on needs no claim read -- FIP-0118 wrote none -- so its own
// FULL_QA_POWER bit settles it; a pre-nv29 sector is dated by its claim, read from the same
// chain this node can see, so a missing one is really missing. The error means only that
// the chain could not be read. si is the caller's SectorsStatus(sid, false), whose
// showOnChainInfo of false leaves every chain field zeroed.
func DirectDealStatus(ctx context.Context, full v1api.FullNode, miner address.Address,
	deal *types.DirectDeal, si lapi.SectorInfo) (DirectDealOnChain, error) {

	if !IsFinalSealingState(si.State) {
		return DirectDealSealing, nil
	}
	if IsFailedSealingState(si.State) {
		return DirectDealSectorGone, nil
	}

	// The read si cannot replace: the sector's flags, and the nil only this call surfaces.
	sc, err := full.StateSectorGetInfo(ctx, miner, deal.SectorID, ltypes.EmptyTSK)
	if err != nil {
		return 0, fmt.Errorf("getting sector %d from chain: %w", deal.SectorID, err)
	}
	if sc == nil {
		return DirectDealNotVisible, nil
	}

	if SectorOnboarded(sc) {
		return DirectDealOnChainDone, nil
	}

	isClaimed, found, err := confirmClaim(ctx, full, miner, deal.AllocationID, deal.SectorID)
	if err != nil {
		return 0, err
	}
	switch {
	case found && isClaimed:
		return DirectDealOnChainDone, nil
	case found:
		return DirectDealClaimElsewhere, nil
	default:
		return DirectDealNoClaim, nil
	}
}

// confirmClaim reports whether the chain's claim for the allocation names this sector.
func confirmClaim(ctx context.Context, full v1api.FullNode, miner address.Address,
	allocId verifreg9types.AllocationId, sectorNum abi.SectorNumber) (isClaimed, found bool, err error) {

	claim, err := full.StateGetClaim(ctx, miner, verifreg9types.ClaimId(allocId), ltypes.EmptyTSK)
	if err != nil {
		return false, false, fmt.Errorf("getting claim details for allocationID %d: %w", allocId, err)
	}
	if claim == nil {
		return false, false, nil
	}
	if claim.Sector != sectorNum {
		return false, true, nil
	}
	return true, true, nil
}
