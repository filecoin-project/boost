package gql

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/types"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
)

// provingPipeline reports a sealed sector with no on-chain info, as lotus does
// unless asked for it, so reading Activation off this call yields a zero epoch.
type provingPipeline struct {
	sealingpipeline.API
	state lapi.SectorState
}

func (p *provingPipeline) SectorsStatus(context.Context, abi.SectorNumber, bool) (lapi.SectorInfo, error) {
	return lapi.SectorInfo{State: p.state}, nil
}

// claimNode answers the params, sector and claim lookups sealingState makes.
type claimNode struct {
	v1api.FullNode
	nv29Height      abi.ChainEpoch
	noUpgradeHeight bool
	paramsErr       error
	claim           *verifreg9types.Claim
	activation      abi.ChainEpoch
	sectorErr       error
	sectorMissing   bool
}

func (c *claimNode) StateGetNetworkParams(context.Context) (*lapi.NetworkParams, error) {
	if c.paramsErr != nil {
		return nil, c.paramsErr
	}
	if c.noUpgradeHeight {
		// A node built before nv29 has no such field: it decodes to a zero epoch.
		return &lapi.NetworkParams{}, nil
	}
	return &lapi.NetworkParams{
		ForkUpgradeParams: lapi.ForkUpgradeParams{UpgradeXxHeight: c.nv29Height},
	}, nil
}

func (c *claimNode) StateGetClaim(context.Context, address.Address, verifreg9types.ClaimId, ltypes.TipSetKey) (*verifreg9types.Claim, error) {
	return c.claim, nil
}

func (c *claimNode) StateSectorGetInfo(context.Context, address.Address, abi.SectorNumber, ltypes.TipSetKey) (*miner.SectorOnChainInfo, error) {
	if c.sectorErr != nil {
		return nil, c.sectorErr
	}
	if c.sectorMissing {
		return nil, nil
	}
	return &miner.SectorOnChainInfo{Activation: c.activation}, nil
}

// nv29Height is the upgrade epoch these tests date sectors against.
const nv29Height = abi.ChainEpoch(1000)

func newSealingStateResolver(t *testing.T, node *claimNode) *directDealResolver {
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
		spApi:    &provingPipeline{state: lapi.SectorState(sealing.Proving)},
		fullNode: node,
	}
}

// TestSealingStateNoClaimSealedAfterNv29 covers a deal that sealed after the
// upgrade, where having no claim is normal and must not be flagged.
func TestSealingStateNoClaimSealedAfterNv29(t *testing.T) {
	dr := newSealingStateResolver(t, &claimNode{nv29Height: nv29Height, activation: nv29Height + 1})

	state := dr.sealingState(context.Background())
	require.Equal(t, "Sealer: "+string(sealing.Proving), state)
	require.NotContains(t, state, "No claim found")
}

// TestSealingStateNoClaimSealedBeforeNv29 is what dating by the sector buys:
// a missing claim here is a real fault and must keep showing.
func TestSealingStateNoClaimSealedBeforeNv29(t *testing.T) {
	dr := newSealingStateResolver(t, &claimNode{nv29Height: nv29Height, activation: nv29Height - 1})

	require.Contains(t, dr.sealingState(context.Background()), "No claim found")
}

// TestSealingStateNoClaimActivationUnknown covers what cannot be dated: the
// report falls back to flagging the absence, the safer way to be wrong.
func TestSealingStateNoClaimActivationUnknown(t *testing.T) {
	for name, node := range map[string]*claimNode{
		"params lookup fails": {paramsErr: context.DeadlineExceeded, activation: nv29Height + 1},
		"sector lookup fails": {nv29Height: nv29Height, sectorErr: context.DeadlineExceeded},
		"sector not on chain": {nv29Height: nv29Height, sectorMissing: true},
		"upgrade unscheduled": {nv29Height: abi.ChainEpoch(999999999999999), activation: nv29Height + 1},
		"activation is zero":  {nv29Height: nv29Height},
		"activation pre-nv29": {nv29Height: nv29Height, activation: nv29Height - 1},
		// A node that cannot report the upgrade height reads as "not past it",
		// even though this sector's activation on its own would read as past.
		"upgrade height not reported": {noUpgradeHeight: true, activation: nv29Height + 1},
	} {
		t.Run(name, func(t *testing.T) {
			dr := newSealingStateResolver(t, node)

			require.Contains(t, dr.sealingState(context.Background()), "No claim found")
		})
	}
}

// TestSealingStateClaimVerified checks that a matching claim is still reported
// as verified past nv29, so the new branch has not displaced the normal case.
func TestSealingStateClaimVerified(t *testing.T) {
	claim := &verifreg9types.Claim{Sector: abi.SectorNumber(2)}
	dr := newSealingStateResolver(t, &claimNode{nv29Height: nv29Height, activation: nv29Height + 1, claim: claim})

	require.Contains(t, dr.sealingState(context.Background()), "Claim verified")
}

// TestSealingStateClaimSectorMismatch checks that a claim on another sector is
// still reported: a real inconsistency, not the expected nv29 absence.
func TestSealingStateClaimSectorMismatch(t *testing.T) {
	claim := &verifreg9types.Claim{Sector: abi.SectorNumber(3)}
	dr := newSealingStateResolver(t, &claimNode{nv29Height: nv29Height, activation: nv29Height + 1, claim: claim})

	require.Contains(t, dr.sealingState(context.Background()), "Sector mismatch")
}
