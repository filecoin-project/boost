package storagemarket

import (
	"testing"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	minertypes "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
)

// The verdict has to hold across the two eras the deal flow straddles. A piece
// onboarded before nv29 has a claim pointing at its sector; a piece onboarded at
// nv29 or later has none, so its only evidence is the sector's own weight. The
// same handful of sector shapes must be judged the same way whether the data
// arrived before or after the upgrade.
func TestVerdictForSealedSector(t *testing.T) {
	const sector = abi.SectorNumber(7)
	const other = abi.SectorNumber(8)

	zero := big.Zero()
	nonZero := big.NewInt(1)

	// A sector of the given weights, with the attributes that matter here.
	sectorWith := func(dealWeight, verifiedWeight big.Int) *minertypes.SectorOnChainInfo {
		return &minertypes.SectorOnChainInfo{
			DealWeight:         dealWeight,
			VerifiedDealWeight: verifiedWeight,
		}
	}
	claimOn := func(sectorNum abi.SectorNumber) *verifreg9types.Claim {
		return &verifreg9types.Claim{Sector: sectorNum}
	}

	tests := []struct {
		name  string
		si    *minertypes.SectorOnChainInfo
		claim *verifreg9types.Claim
		want  directDealSealingVerdict
	}{
		{
			name:  "empty CC sector with no claim has no evidence",
			si:    sectorWith(zero, zero),
			claim: nil,
			want:  sealingNoEvidence,
		},
		{
			name:  "empty CC sector whose claim points here is done",
			si:    sectorWith(zero, zero),
			claim: claimOn(sector),
			want:  sealingDone,
		},
		{
			name:  "claim pointing at another sector is terminal",
			si:    sectorWith(zero, zero),
			claim: claimOn(other),
			want:  sealingClaimElsewhere,
		},
		{
			name:  "pre-nv29 plain deal carries DealWeight and needs no claim",
			si:    sectorWith(nonZero, zero),
			claim: nil,
			want:  sealingDone,
		},
		{
			name:  "pre-nv29 DDO deal carries VerifiedDealWeight",
			si:    sectorWith(zero, nonZero),
			claim: claimOn(sector),
			want:  sealingDone,
		},
		{
			name:  "post-nv29 snap carries VerifiedDealWeight and no claim",
			si:    sectorWith(zero, nonZero),
			claim: nil,
			want:  sealingDone,
		},
		{
			name:  "a sector carrying both weights is done",
			si:    sectorWith(nonZero, nonZero),
			claim: nil,
			want:  sealingDone,
		},
		{
			name:  "a sector this node cannot see yet is not a verdict",
			si:    nil,
			claim: nil,
			want:  sealingNoEvidence,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := verdictForSealedSector(tt.si, tt.claim, sector)
			if got != tt.want {
				t.Fatalf("verdictForSealedSector() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSectorCarriesData(t *testing.T) {
	zero := big.Zero()
	nonZero := big.NewInt(1)

	tests := []struct {
		name string
		si   *minertypes.SectorOnChainInfo
		want bool
	}{
		{name: "nil sector", si: nil, want: false},
		{name: "pure CC", si: &minertypes.SectorOnChainInfo{DealWeight: zero, VerifiedDealWeight: zero}, want: false},
		{name: "plain deal weight only", si: &minertypes.SectorOnChainInfo{DealWeight: nonZero, VerifiedDealWeight: zero}, want: true},
		{name: "verified deal weight only", si: &minertypes.SectorOnChainInfo{DealWeight: zero, VerifiedDealWeight: nonZero}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sectorCarriesData(tt.si); got != tt.want {
				t.Fatalf("sectorCarriesData() = %v, want %v", got, tt.want)
			}
		})
	}
}
