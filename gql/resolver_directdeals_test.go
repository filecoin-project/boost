package gql

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/go-state-types/network"

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

// claimNode answers the version, schedule, sector and claim lookups sealingState makes, on a chain
// always at nv29.
type claimNode struct {
	v1api.FullNode
	paramsErr     error
	claim         *verifreg9types.Claim
	activation    abi.ChainEpoch
	snappedAt     abi.ChainEpoch
	snapped       bool
	noPieceData   bool
	sectorErr     error
	sectorMissing bool
}

func (c *claimNode) StateNetworkVersion(context.Context, ltypes.TipSetKey) (network.Version, error) {
	return network.Version29, nil
}

func (c *claimNode) StateGetNetworkParams(context.Context) (*lapi.NetworkParams, error) {
	if c.paramsErr != nil {
		return nil, c.paramsErr
	}
	return &lapi.NetworkParams{
		ForkUpgradeParams: lapi.ForkUpgradeParams{UpgradeSolsticeHeight: nv29Height},
	}, nil
}

func (c *claimNode) StateGetClaim(context.Context, address.Address, verifreg9types.ClaimId, ltypes.TipSetKey) (*verifreg9types.Claim, error) {
	return c.claim, nil
}

func (c *claimNode) StateSectorGetInfo(_ context.Context, _ address.Address, sector abi.SectorNumber, _ ltypes.TipSetKey) (*miner.SectorOnChainInfo, error) {
	if c.sectorErr != nil {
		return nil, c.sectorErr
	}
	if c.sectorMissing {
		return nil, nil
	}

	si := &miner.SectorOnChainInfo{
		SectorNumber: sector,
		Activation:   c.activation,
		// Zero, not absent: the chain always writes both weights.
		DealWeight:         big.Zero(),
		VerifiedDealWeight: big.Zero(),
	}
	if !c.noPieceData {
		// Past nv29 every piece's spacetime lands here, verified or not.
		si.VerifiedDealWeight = big.NewInt(1 << 20)
	}
	if c.snapped {
		keyCid, err := cid.Parse("bafkqaaa")
		if err != nil {
			return nil, err
		}
		si.SectorKeyCID = &keyCid
		si.PowerBaseEpoch = c.snappedAt
	}
	return si, nil
}

// nv29Height is the epoch claimNode reports as nv29's.
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
	dr := newSealingStateResolver(t, &claimNode{activation: nv29Height + 1})

	state := dr.sealingState(context.Background())
	require.Equal(t, "Sealer: "+string(sealing.Proving), state)
	require.NotContains(t, state, "No claim found")
}

// TestSealingStateNoClaimSealedBeforeNv29: dating by the sector's own history keeps a real missing
// claim showing.
func TestSealingStateNoClaimSealedBeforeNv29(t *testing.T) {
	dr := newSealingStateResolver(t, &claimNode{activation: nv29Height - 1})

	require.Contains(t, dr.sealingState(context.Background()), "No claim found")
}

// TestSealingStateNoClaimSnappedAfterNv29: a sector sealed long before nv29 but snapped after it,
// where dating by activation alone would flag a healthy deal -- the update's epoch is the one that
// counts.
func TestSealingStateNoClaimSnappedAfterNv29(t *testing.T) {
	dr := newSealingStateResolver(t, &claimNode{
		activation: nv29Height - 5000,
		snapped:    true,
		snappedAt:  nv29Height + 1,
	})

	state := dr.sealingState(context.Background())
	require.Equal(t, "Sealer: "+string(sealing.Proving), state)
	require.NotContains(t, state, "No claim found")
}

// TestSealingStateNoClaimEmptySectorAfterNv29: a sector proven past nv29 with no piece data, as a
// snap that never landed leaves, must still flag a deal whose data is not on chain.
func TestSealingStateNoClaimEmptySectorAfterNv29(t *testing.T) {
	dr := newSealingStateResolver(t, &claimNode{activation: nv29Height + 1, noPieceData: true})

	require.Contains(t, dr.sealingState(context.Background()), "No claim found")
}

// TestSealingStateNoClaimActivationUnknown covers what cannot be dated: the
// report falls back to flagging the absence, the safer way to be wrong.
func TestSealingStateNoClaimActivationUnknown(t *testing.T) {
	for name, node := range map[string]*claimNode{
		"sector lookup fails": {sectorErr: context.DeadlineExceeded},
		"sector not on chain": {sectorMissing: true},
		"activation is zero":  {},
		"activation pre-nv29": {activation: nv29Height - 1},
		"params lookup fails": {paramsErr: context.DeadlineExceeded, activation: nv29Height + 1},
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
	dr := newSealingStateResolver(t, &claimNode{activation: nv29Height + 1, claim: claim})

	require.Contains(t, dr.sealingState(context.Background()), "Claim verified")
}

// TestSealingStateClaimSectorMismatch checks that a claim on another sector is
// still reported: a real inconsistency, not the expected nv29 absence.
func TestSealingStateClaimSectorMismatch(t *testing.T) {
	claim := &verifreg9types.Claim{Sector: abi.SectorNumber(3)}
	dr := newSealingStateResolver(t, &claimNode{activation: nv29Height + 1, claim: claim})

	require.Contains(t, dr.sealingState(context.Background()), "Sector mismatch")
}

// TestDirectDealResolverCarriesItsPlumbing pins that both query paths build the resolver through
// the same constructor, so it has the full node sealingState needs.
func TestDirectDealResolverCarriesItsPlumbing(t *testing.T) {
	node := &claimNode{activation: nv29Height + 1}
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

	require.Equal(t, "Sealer: "+string(sealing.Proving), dr.sealingState(context.Background()))
}
