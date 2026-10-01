package storagemarket

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	"github.com/filecoin-project/boost/storagemarket/types"

	lapi "github.com/filecoin-project/lotus/api"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
)

// TestDirectDealStatus covers every answer DirectDealStatus gives. The nv29 cases are the
// ones the fork turns on: FIP-0118 writes no claim for a sector onboarded from there on,
// so the flag must settle the deal rather than the claim it will never have.
func TestDirectDealStatus(t *testing.T) {
	const (
		thisSector  = abi.SectorNumber(2)
		otherSector = abi.SectorNumber(3)
		allocID     = verifreg9types.AllocationId(1)
	)

	tests := map[string]struct {
		sealingState lapi.SectorState
		node         *chainNode
		want         DirectDealOnChain
		wantErr      bool
	}{
		"the sealer is still working on the sector": {
			sealingState: lapi.SectorState(sealing.Packing),
			node:         &chainNode{sector: nv29Sector(thisSector)},
			want:         DirectDealSealing,
		},
		"the sealer gave up on the sector": {
			sealingState: lapi.SectorState(sealing.FailedUnrecoverable),
			node:         &chainNode{sector: nv29Sector(thisSector)},
			want:         DirectDealSectorGone,
		},
		"a sector onboarded after nv29": {
			node: &chainNode{sector: nv29Sector(thisSector)},
			want: DirectDealOnChainDone,
		},
		"a snap that landed after nv29": {
			// A snap at or after the fork is an onboarding, so the flag is set on it too.
			node: &chainNode{sector: nv29Sector(thisSector)},
			want: DirectDealOnChainDone,
		},
		"a pre-nv29 sector with its claim": {
			node: &chainNode{
				sector: preNv29Sector(thisSector),
				claim:  &verifreg9types.Claim{Sector: thisSector},
			},
			want: DirectDealOnChainDone,
		},
		"a pre-nv29 sector with no claim": {
			node: &chainNode{sector: preNv29Sector(thisSector)},
			want: DirectDealNoClaim,
		},
		"a pre-nv29 sector whose claim names another sector": {
			node: &chainNode{
				sector: preNv29Sector(thisSector),
				claim:  &verifreg9types.Claim{Sector: otherSector},
			},
			want: DirectDealClaimElsewhere,
		},
		"a sector this node's chain does not show": {
			node: &chainNode{},
			want: DirectDealNotVisible,
		},
		"a chain that will not answer": {
			node:    &chainNode{sectorErr: context.DeadlineExceeded},
			wantErr: true,
		},
		"a claim the chain will not hand over": {
			node: &chainNode{
				sector:   preNv29Sector(thisSector),
				claimErr: context.DeadlineExceeded,
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sealingState := tc.sealingState
			if sealingState == "" {
				sealingState = lapi.SectorState(sealing.Proving)
			}

			maddr, err := address.NewIDAddress(1000)
			require.NoError(t, err)

			deal := &types.DirectDeal{
				AllocationID: allocID,
				SectorID:     thisSector,
				PieceCID:     testPieceCid(t),
			}

			status, err := DirectDealStatus(context.Background(), tc.node, maddr, deal,
				lapi.SectorInfo{State: sealingState})

			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, status)
		})
	}
}

// TestDirectDealStatusIsSealedByTheSectorAlone pins where the chain is read: never while
// sealing, once when the sealer is done, and the claim never past nv29.
func TestDirectDealStatusIsSealedByTheSectorAlone(t *testing.T) {
	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)

	deal := &types.DirectDeal{
		AllocationID: verifreg9types.AllocationId(1),
		SectorID:     abi.SectorNumber(2),
		PieceCID:     testPieceCid(t),
	}

	t.Run("still sealing: no chain read", func(t *testing.T) {
		node := &chainNode{sector: nv29Sector(abi.SectorNumber(2))}

		status, err := DirectDealStatus(context.Background(), node, maddr, deal,
			lapi.SectorInfo{State: lapi.SectorState(sealing.Packing)})

		require.NoError(t, err)
		require.Equal(t, DirectDealSealing, status)
		require.Zero(t, node.sectorReads)
		require.Zero(t, node.claimReads)
	})

	t.Run("onboarded after nv29: one chain read, no claim read", func(t *testing.T) {
		node := &chainNode{sector: nv29Sector(abi.SectorNumber(2))}

		status, err := DirectDealStatus(context.Background(), node, maddr, deal,
			lapi.SectorInfo{State: lapi.SectorState(sealing.Proving)})

		require.NoError(t, err)
		require.Equal(t, DirectDealOnChainDone, status)
		require.Equal(t, 1, node.sectorReads)
		require.Zero(t, node.claimReads,
			"FIP-0118 wrote no claim for this sector, so a read could only confirm an absence the flag already accounts for")
	})

	t.Run("before nv29: the claim is the rule, so it is read", func(t *testing.T) {
		node := &chainNode{
			sector: preNv29Sector(abi.SectorNumber(2)),
			claim:  &verifreg9types.Claim{Sector: abi.SectorNumber(2)},
		}

		status, err := DirectDealStatus(context.Background(), node, maddr, deal,
			lapi.SectorInfo{State: lapi.SectorState(sealing.Proving)})

		require.NoError(t, err)
		require.Equal(t, DirectDealOnChainDone, status)
		require.Equal(t, 1, node.sectorReads)
		require.Equal(t, 1, node.claimReads)
	})

	t.Run("not on this node's chain: one look and no claim read", func(t *testing.T) {
		node := &chainNode{}

		status, err := DirectDealStatus(context.Background(), node, maddr, deal,
			lapi.SectorInfo{State: lapi.SectorState(sealing.Proving)})

		require.NoError(t, err)
		require.Equal(t, DirectDealNotVisible, status)
		require.Equal(t, 1, node.sectorReads)
		require.Zero(t, node.claimReads,
			"nothing is known about the sector, so there is nothing to weigh a claim against")
	})
}

// TestDirectDealStatusNamesEachOutcome ties every status to a distinct word.
func TestDirectDealStatusNamesEachOutcome(t *testing.T) {
	seen := map[string]DirectDealOnChain{}
	for _, s := range []DirectDealOnChain{
		DirectDealSealing,
		DirectDealOnChainDone,
		DirectDealSectorGone,
		DirectDealNotVisible,
		DirectDealNoClaim,
		DirectDealClaimElsewhere,
	} {
		name := s.String()
		require.NotEqual(t, "unknown", name, "every status names itself")
		_, dup := seen[name]
		require.False(t, dup, "two outcomes share the name %q", name)
		seen[name] = s
	}
}
