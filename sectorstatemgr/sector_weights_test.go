package sectorstatemgr

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
)

// TestSectorCarriesData pins that both spacetime weights are read, in every
// population: FIP-0118 records every piece in VerifiedDealWeight from nv29 on and
// leaves DealWeight at zero, while before it verified pieces were already in
// VerifiedDealWeight. DealWeight alone reads as no data for either.
func TestSectorCarriesData(t *testing.T) {
	tests := map[string]struct {
		sector miner.SectorOnChainInfo
		want   bool
	}{
		"committed capacity": {
			sector: miner.SectorOnChainInfo{DealWeight: big.Zero(), VerifiedDealWeight: big.Zero()},
		},
		"a legacy unverified deal": {
			sector: miner.SectorOnChainInfo{DealWeight: big.NewInt(1 << 20), VerifiedDealWeight: big.Zero()},
			want:   true,
		},
		"a verified deal before nv29": {
			sector: miner.SectorOnChainInfo{DealWeight: big.Zero(), VerifiedDealWeight: big.NewInt(1 << 20)},
			want:   true,
		},
		"any piece at or after nv29, where all spacetime is recorded as verified": {
			sector: miner.SectorOnChainInfo{DealWeight: big.Zero(), VerifiedDealWeight: big.NewInt(1 << 30)},
			want:   true,
		},
		"a weight the node never filled in": {
			sector: miner.SectorOnChainInfo{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := SectorCarriesData(tc.sector.DealWeight) || SectorCarriesData(tc.sector.VerifiedDealWeight)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestSectorCarriesDataRejectsAbsentWeight pins that an absent weight, which a
// node too old to fill the field hands back, reads as no data rather than
// panicking or comparing unequal.
func TestSectorCarriesDataRejectsAbsentWeight(t *testing.T) {
	var absent abi.DealWeight
	require.True(t, absent.Nil())
	require.False(t, SectorCarriesData(absent))
}
