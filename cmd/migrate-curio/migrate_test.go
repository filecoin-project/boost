package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	"github.com/filecoin-project/lotus/api/v1api"
	minertypes "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	ltypes "github.com/filecoin-project/lotus/chain/types"
)

const (
	sectorID        = abi.SectorNumber(2)
	otherSector     = abi.SectorNumber(3)
	onboardedAtNv29 = true
)

// failedWith is a deal that reached a sector, then failed fatally with err.
func failedWith(err string) *types.DirectDeal {
	d := migratableDeal()
	d.Err = err
	d.Retry = types.DealRetryFatal
	return d
}

// migratableDeal is a deal that has reached a sector, so the tests below vary
// only the state being examined.
func migratableDeal() *types.DirectDeal {
	return &types.DirectDeal{
		Checkpoint: dealcheckpoints.AddedPiece,
		SectorID:   sectorID,
	}
}

func claimOnSector(s abi.SectorNumber) *verifreg9types.Claim {
	return &verifreg9types.Claim{Sector: s}
}

// fakeChainNode answers the reads the nv29 dating and the claim lookup make.
type fakeChainNode struct {
	v1api.FullNode

	sectors []*minertypes.SectorOnChainInfo
	headErr error
	err     error

	claim       *verifreg9types.Claim
	claimErr    error
	sectorReads int
	claimReads  int
}

func (f *fakeChainNode) ChainHead(context.Context) (*ltypes.TipSet, error) {
	return &ltypes.TipSet{}, f.headErr
}

func (f *fakeChainNode) StateMinerSectors(context.Context, address.Address, *bitfield.BitField, ltypes.TipSetKey) ([]*minertypes.SectorOnChainInfo, error) {
	f.sectorReads++
	return f.sectors, f.err
}

func (f *fakeChainNode) StateGetClaim(context.Context, address.Address, verifreg9types.ClaimId, ltypes.TipSetKey) (*verifreg9types.Claim, error) {
	f.claimReads++
	return f.claim, f.claimErr
}

// onboardedAtNv29Sector holds piece data in a sector the fork dated at or after nv29.
func onboardedAtNv29Sector() *minertypes.SectorOnChainInfo {
	return &minertypes.SectorOnChainInfo{
		SectorNumber: sectorID,
		Activation:   300,
		DealWeight:   big.NewInt(1),
		Flags:        minertypes.FULL_QA_POWER,
	}
}

// TestDateDealsAgainstNv29: one read, dated by the flag; a read that cannot date stops the run.
func TestDateDealsAgainstNv29(t *testing.T) {
	ctx := context.Background()
	maddr := address.TestAddress

	t.Run("one read dates the carrying sectors and leaves the others", func(t *testing.T) {
		node := &fakeChainNode{sectors: []*minertypes.SectorOnChainInfo{
			onboardedAtNv29Sector(),
			{SectorNumber: otherSector, Activation: 100, DealWeight: big.NewInt(1)},
			{SectorNumber: 4, Activation: 300, Flags: minertypes.FULL_QA_POWER},
		}}

		onboarded, err := dateDealsAgainstNv29(ctx, node, maddr)

		require.NoError(t, err)
		require.True(t, onboarded[sectorID])
		require.False(t, onboarded[otherSector])
		require.False(t, onboarded[4], "a flagged sector with no piece spacetime holds no piece to date")
		require.Equal(t, 1, node.sectorReads, "the miner's sectors are read once, not once per deal")
	})

	t.Run("the flag dates the sector, not its epoch", func(t *testing.T) {
		// An activation past any fork is not enough: only the bit FIP-0118 sets dates a sector.
		node := &fakeChainNode{sectors: []*minertypes.SectorOnChainInfo{
			{SectorNumber: sectorID, Activation: 9_000_000, DealWeight: big.NewInt(1)},
		}}

		onboarded, err := dateDealsAgainstNv29(ctx, node, maddr)

		require.NoError(t, err)
		require.Empty(t, onboarded)
	})

	t.Run("a chain that will not answer stops the run", func(t *testing.T) {
		node := &fakeChainNode{err: errors.New("connection refused")}

		onboarded, err := dateDealsAgainstNv29(ctx, node, maddr)

		require.ErrorContains(t, err, "listing the miner's sectors")
		require.ErrorContains(t, err, "connection refused")
		require.Nil(t, onboarded)
	})

	t.Run("a chain that will not name a head stops the run too", func(t *testing.T) {
		node := &fakeChainNode{headErr: errors.New("node is syncing")}

		onboarded, err := dateDealsAgainstNv29(ctx, node, maddr)

		require.ErrorContains(t, err, "getting chain head")
		require.ErrorContains(t, err, "node is syncing")
		require.Nil(t, onboarded)
		require.Zero(t, node.sectorReads, "no sectors can be dated without a head to date them at")
	})
}

// TestClaimToRead: past nv29 the flag spares the read.
func TestClaimToRead(t *testing.T) {
	ctx := context.Background()
	maddr := address.TestAddress

	t.Run("an nv29 sector is not asked for a claim", func(t *testing.T) {
		node := &fakeChainNode{claim: claimOnSector(sectorID)}

		claim, err := claimToRead(ctx, node, maddr, migratableDeal(), onboardedAtNv29)

		require.NoError(t, err)
		require.Nil(t, claim)
		require.Zero(t, node.claimReads, "the flag answers for this deal; the chain is not asked")
	})

	t.Run("a pre-nv29 sector is", func(t *testing.T) {
		node := &fakeChainNode{claim: claimOnSector(sectorID)}

		claim, err := claimToRead(ctx, node, maddr, migratableDeal(), false)

		require.NoError(t, err)
		require.Equal(t, sectorID, claim.Sector)
		require.Equal(t, 1, node.claimReads)
	})

	t.Run("a pre-nv29 look up that fails stops the deal, not the run", func(t *testing.T) {
		node := &fakeChainNode{claimErr: errors.New("connection refused")}

		claim, err := claimToRead(ctx, node, maddr, migratableDeal(), false)

		require.ErrorContains(t, err, "error getting the claim status")
		require.ErrorContains(t, err, "connection refused")
		require.Nil(t, claim)
	})
}

// TestMigratableDirectDeal covers the gate the migration runs over every direct
// deal. The nv29 cases matter most: a claim that will never exist must not read
// as the data having left the chain, since that leaves the deal record and its
// retrieval mapping behind in a migration that has already sealed the data.
func TestMigratableDirectDeal(t *testing.T) {
	tests := map[string]struct {
		deal               *types.DirectDeal
		claim              *verifreg9types.Claim
		onboardedAfterNv29 bool
		sectorAlive        bool
		want               bool
		wantReason         string
		wantErr            string
	}{
		"a claimed deal sealed before nv29": {
			deal:        migratableDeal(),
			claim:       claimOnSector(sectorID),
			sectorAlive: true,
			want:        true,
		},
		"a deal sealed after nv29 with no claim, which is the steady state there": {
			deal:               migratableDeal(),
			onboardedAfterNv29: onboardedAtNv29,
			sectorAlive:        true,
			want:               true,
		},
		"a claim that will never exist must not strand a deal failed by an older boost": {
			// Boost used to fail such a deal once the claim lookup came back
			// empty. Skipping it here leaves the deal record and the retrieval
			// mapping out of Curio even though the data sealed.
			deal:               failedWith(storagemarket.ErrNoClaimFound.Error()),
			onboardedAfterNv29: onboardedAtNv29,
			sectorAlive:        true,
			want:               true,
		},
		"a deal onboarded before nv29 with no claim is still skipped": {
			deal:        migratableDeal(),
			sectorAlive: true,
			wantReason:  "no claim was found for a piece onboarded before nv29",
		},
		"a failed deal onboarded before nv29 is still skipped": {
			deal:        failedWith(storagemarket.ErrNoClaimFound.Error()),
			sectorAlive: true,
			wantReason:  "the deal retry is fatal",
		},
		"another fatal error is not excused by nv29": {
			// Only the missing claim is expected past nv29.
			deal:               failedWith("commp mismatch"),
			onboardedAfterNv29: onboardedAtNv29,
			sectorAlive:        true,
			wantReason:         "commp mismatch",
		},
		"a claim mismatch recorded as the deal error is not excused either": {
			deal:               failedWith("sector mismatch for claim"),
			onboardedAfterNv29: onboardedAtNv29,
			sectorAlive:        true,
			wantReason:         "sector mismatch for claim",
		},
		"a pre-nv29 claim that names another sector stops the deal": {
			deal:        migratableDeal(),
			claim:       claimOnSector(otherSector),
			sectorAlive: true,
			wantErr:     "sector mismatch for deal",
		},
		"a deal that never reached a sector": {
			deal: &types.DirectDeal{
				Checkpoint: dealcheckpoints.Accepted,
			},
			onboardedAfterNv29: onboardedAtNv29,
			sectorAlive:        true,
			wantReason:         "the checkpoint is below add piece",
		},
		"a deal whose sector is gone": {
			deal:               migratableDeal(),
			onboardedAfterNv29: onboardedAtNv29,
			wantReason:         "the deal sector is no longer alive",
		},
		"a deal whose sector is gone but whose claim is still there": {
			deal:       migratableDeal(),
			claim:      claimOnSector(sectorID),
			wantReason: "the deal sector is no longer alive",
		},
		"a dead sector is skipped for being dead, not for a claim it never had": {
			// A dead sector cannot be excused; the reason has to name the sector.
			deal:        migratableDeal(),
			sectorAlive: false,
			wantReason:  "the deal sector is no longer alive",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ok, reason, err := migratableDirectDeal(tc.deal, tc.claim, tc.onboardedAfterNv29, tc.sectorAlive)

			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.False(t, ok)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, ok)
			if tc.want {
				require.Empty(t, reason, "a migrated deal has nothing to explain")
				return
			}
			// The reason ends up in the operator's migration log, so a skip that
			// says nothing useful is worth failing on.
			require.Contains(t, reason, tc.wantReason)
		})
	}
}
