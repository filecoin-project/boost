package gql

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	lapi "github.com/filecoin-project/lotus/api"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	minertypes "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"

	mocksealingpipeline "github.com/filecoin-project/boost/storagemarket/sealingpipeline/mock"
	"github.com/filecoin-project/boost/storagemarket/types"
)

// TestSealingState pins the operator-facing wording of a direct deal's sealing
// state. The post-nv29 rows matter most: FIP-0118 writes no claim, so a nil claim
// must be answered from the sector's weight. Reading it as "No claim found" would
// paint every post-nv29 direct deal as if it had failed to land, and the UI
// would show a healthy deal and a broken one identically.
func TestSealingState(t *testing.T) {
	const (
		sectorNum   = abi.SectorNumber(7)
		allocID     = verifreg9types.AllocationId(11)
		providerStr = "t01000"
	)
	provider, err := address.NewFromString(providerStr)
	require.NoError(t, err)

	weight := big.NewInt(1)
	zero := big.Zero()

	tests := []struct {
		name       string
		sealerErr  error
		state      lapi.SectorState
		claimErr   error
		claim      *verifreg9types.Claim
		sectorErr  error
		sector     *minertypes.SectorOnChainInfo
		want       string
		wantNoSect bool // the sector read must not even be attempted
		wantNoClm  bool
	}{
		{
			name:      "sealer is not proving yet",
			state:     lapi.SectorState(sealing.AddPiece),
			wantNoClm: true,
			want:      "Sealer: AddPiece",
		},
		{
			name:      "sealer read fails: fall back to the sealer wording, not an error",
			sealerErr: errors.New("sealer unreachable"),
			wantNoClm: true,
			want:      "Sealer: Sealing",
		},
		{
			name:  "nv28: claim on this sector",
			state: lapi.SectorState(sealing.Proving),
			claim: &verifreg9types.Claim{Sector: sectorNum},
			want:  "Sealer: Proving(Claim verified)",
		},
		{
			name:  "nv28: claim points at another sector",
			state: lapi.SectorState(sealing.Proving),
			claim: &verifreg9types.Claim{Sector: sectorNum + 1},
			want:  "Sealer: Proving(Sector mismatch)",
		},
		{
			name:      "nv29: no claim, the sector carries the data",
			state:     lapi.SectorState(sealing.Proving),
			sector:    &minertypes.SectorOnChainInfo{VerifiedDealWeight: weight},
			want:      "Sealer: Proving(Data in sector)",
			wantNoClm: false,
		},
		{
			name:      "nv29: no claim, plain deal weight also counts",
			state:     lapi.SectorState(sealing.Proving),
			sector:    &minertypes.SectorOnChainInfo{DealWeight: weight},
			want:      "Sealer: Proving(Data in sector)",
			wantNoClm: false,
		},
		{
			name:      "no claim and no weight: the data never landed",
			state:     lapi.SectorState(sealing.Proving),
			sector:    &minertypes.SectorOnChainInfo{DealWeight: zero, VerifiedDealWeight: zero},
			want:      "Sealer: Proving(No claim found)",
			wantNoClm: false,
		},
		{
			name:      "no claim and the sector is not on chain",
			state:     lapi.SectorState(sealing.Proving),
			sector:    nil,
			want:      "Sealer: Proving(No claim found)",
			wantNoClm: false,
		},
		{
			name:       "claim read fails: do not claim either way",
			state:      lapi.SectorState(sealing.Proving),
			claimErr:   errors.New("node lagging"),
			want:       "Sealer: Proving",
			wantNoSect: false,
		},
		{
			name:      "sector read fails: do not claim either way",
			state:     lapi.SectorState(sealing.Proving),
			sectorErr: errors.New("actor not found"),
			want:      "Sealer: Proving",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			sps := mocksealingpipeline.NewMockAPI(ctrl)
			node := lotusmocks.NewMockFullNode(ctrl)

			sps.EXPECT().SectorsStatus(gomock.Any(), sectorNum, false).
				Return(lapi.SectorInfo{State: tt.state}, tt.sealerErr)

			if !tt.wantNoClm {
				node.EXPECT().StateGetClaim(gomock.Any(), provider, verifreg9types.ClaimId(allocID), gomock.Any()).
					Return(tt.claim, tt.claimErr)
			}
			if tt.claim == nil && !tt.wantNoClm && tt.claimErr == nil {
				node.EXPECT().StateSectorGetInfo(gomock.Any(), provider, sectorNum, gomock.Any()).
					Return(tt.sector, tt.sectorErr)
			} else {
				node.EXPECT().StateSectorGetInfo(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			}

			dr := &directDealResolver{
				DirectDeal: types.DirectDeal{AllocationID: allocID, SectorID: sectorNum, Provider: provider},
				spApi:      sps,
				fullNode:   node,
			}

			require.Equal(t, tt.want, dr.sealingState(context.Background()))
		})
	}
}

// TestSealingStateBeforeSealing covers the checkpoint short-circuit: a deal that
// has not been handed to the sealer yet must not be reported from a sealer read
// at all (its piece is not in any sector).
func TestSealingStateBeforeSealing(t *testing.T) {
	dr := &directDealResolver{DirectDeal: types.DirectDeal{}}
	require.Equal(t, "To be sealed", dr.SealingState(context.Background()))
}
