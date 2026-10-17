package sectorstatemgr

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
)

// TestSectorCarriesData pins both weight arms. Reading only DealWeight is what
// let a post-nv29 sector look empty: FIP-0118 puts every piece's spacetime in
// VerifiedDealWeight and leaves DealWeight at zero.
func TestSectorCarriesData(t *testing.T) {
	zero := big.Zero()
	one := big.NewInt(1)

	tests := []struct {
		name string
		si   *miner.SectorOnChainInfo
		want bool
	}{
		{name: "nil sector", si: nil, want: false},
		{name: "pure CC", si: &miner.SectorOnChainInfo{DealWeight: zero, VerifiedDealWeight: zero}, want: false},
		{name: "plain deal weight only", si: &miner.SectorOnChainInfo{DealWeight: one, VerifiedDealWeight: zero}, want: true},
		{name: "verified weight only", si: &miner.SectorOnChainInfo{DealWeight: zero, VerifiedDealWeight: one}, want: true},
		{name: "both weights", si: &miner.SectorOnChainInfo{DealWeight: one, VerifiedDealWeight: one}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, SectorCarriesData(tt.si))
		})
	}
}
