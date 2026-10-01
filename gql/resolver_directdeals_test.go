package gql

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/types"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
)

// provingPipeline reports a sealed sector, as the sealer's own record has it.
type provingPipeline struct {
	sealingpipeline.API
	state lapi.SectorState
}

func (p *provingPipeline) SectorsStatus(context.Context, abi.SectorNumber, bool) (lapi.SectorInfo, error) {
	return lapi.SectorInfo{State: p.state}, nil
}

// claimNode answers the chain reads sealingState makes through the provider's own call.
type claimNode struct {
	v1api.FullNode

	sector    *miner.SectorOnChainInfo
	sectorErr error
	claim     *verifreg9types.Claim
	claimErr  error
}

func (c *claimNode) StateSectorGetInfo(_ context.Context, _ address.Address, sector abi.SectorNumber, _ ltypes.TipSetKey) (*miner.SectorOnChainInfo, error) {
	if c.sectorErr != nil {
		return nil, c.sectorErr
	}
	return c.sector, nil
}

func (c *claimNode) StateGetClaim(context.Context, address.Address, verifreg9types.ClaimId, ltypes.TipSetKey) (*verifreg9types.Claim, error) {
	return c.claim, c.claimErr
}

// preNv29Sector: 10x from the deal's own verified weight, flag clear, a claim.
func preNv29Sector(sector abi.SectorNumber) *miner.SectorOnChainInfo {
	return &miner.SectorOnChainInfo{
		SectorNumber:       sector,
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.NewInt(1 << 20),
	}
}

// nv29Sector: piece data in at or after the fork, so the flag is set and no claim written.
func nv29Sector(sector abi.SectorNumber) *miner.SectorOnChainInfo {
	si := preNv29Sector(sector)
	si.Flags = miner.FULL_QA_POWER
	return si
}

// emptySector is flagged by the fork but holds no piece data: a snap that never landed.
func emptySector(sector abi.SectorNumber) *miner.SectorOnChainInfo {
	si := nv29Sector(sector)
	si.VerifiedDealWeight = big.Zero()
	return si
}

func newSealingStateResolverInState(t *testing.T, node *claimNode, state lapi.SectorState) *directDealResolver {
	t.Helper()

	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)

	return &directDealResolver{
		DirectDeal: types.DirectDeal{
			ID:           uuid.New(),
			Provider:     maddr,
			SectorID:     abi.SectorNumber(2),
			AllocationID: verifreg9types.AllocationId(1),
		},
		spApi:    &provingPipeline{state: state},
		fullNode: node,
	}
}

func TestSealingStateNamesWhereTheDealIs(t *testing.T) {
	const proving = "Sealer: " + string(sealing.Proving)

	for name, tc := range map[string]struct {
		node  *claimNode
		state lapi.SectorState
		want  string
	}{
		"a sector onboarded after nv29": {
			node: &claimNode{sector: nv29Sector(abi.SectorNumber(2))},
			want: proving + "(On chain)",
		},
		"a snap that landed after nv29": {
			node: &claimNode{sector: nv29Sector(abi.SectorNumber(2))},
			want: proving + "(On chain)",
		},
		"a pre-nv29 sector with its claim": {
			node: &claimNode{
				sector: preNv29Sector(abi.SectorNumber(2)),
				claim:  &verifreg9types.Claim{Sector: abi.SectorNumber(2)},
			},
			want: proving + "(On chain)",
		},
		"a pre-nv29 sector with no claim, which is a real absence": {
			node: &claimNode{sector: preNv29Sector(abi.SectorNumber(2))},
			want: proving + "(No claim found)",
		},
		"a pre-nv29 sector whose claim names another sector": {
			node: &claimNode{
				sector: preNv29Sector(abi.SectorNumber(2)),
				claim:  &verifreg9types.Claim{Sector: abi.SectorNumber(3)},
			},
			want: proving + "(Sector mismatch)",
		},
		"a flagged sector holding no piece data": {
			node: &claimNode{sector: emptySector(abi.SectorNumber(2))},
			want: proving + "(No claim found)",
		},
		"a sector this node's chain does not show": {
			node: &claimNode{},
			want: proving + "(Not on this node's chain)",
		},
		"a chain that will not answer": {
			node: &claimNode{sectorErr: context.DeadlineExceeded},
			want: proving,
		},
		"a claim the chain will not hand over": {
			node: &claimNode{
				sector:   preNv29Sector(abi.SectorNumber(2)),
				claimErr: context.DeadlineExceeded,
			},
			want: proving,
		},
		"a sealer still working on the sector": {
			node:  &claimNode{sector: nv29Sector(abi.SectorNumber(2))},
			state: lapi.SectorState(sealing.Packing),
			want:  "Sealer: " + string(sealing.Packing),
		},
	} {
		t.Run(name, func(t *testing.T) {
			state := tc.state
			if state == "" {
				state = lapi.SectorState(sealing.Proving)
			}

			dr := newSealingStateResolverInState(t, tc.node, state)

			require.Equal(t, tc.want, dr.sealingState(context.Background()))
		})
	}
}

func TestSealingStateFailedSector(t *testing.T) {
	dr := newSealingStateResolverInState(t, &claimNode{}, lapi.SectorState(sealing.FailedUnrecoverable))

	require.Equal(t, "Sealer: "+string(sealing.FailedUnrecoverable), dr.sealingState(context.Background()))
}

// TestDirectDealResolverCarriesItsPlumbing pins that both query paths build the resolver through
// the same constructor, so it has the full node sealingState needs.
func TestDirectDealResolverCarriesItsPlumbing(t *testing.T) {
	node := &claimNode{sector: nv29Sector(abi.SectorNumber(2))}
	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)

	dr := newDirectDealResolver(&resolver{
		spApi:    &provingPipeline{state: lapi.SectorState(sealing.Proving)},
		fullNode: node,
	}, &types.DirectDeal{
		ID:           uuid.New(),
		Provider:     maddr,
		SectorID:     abi.SectorNumber(2),
		AllocationID: verifreg9types.AllocationId(1),
	})

	require.Equal(t, "Sealer: "+string(sealing.Proving)+"(On chain)", dr.sealingState(context.Background()))
}
