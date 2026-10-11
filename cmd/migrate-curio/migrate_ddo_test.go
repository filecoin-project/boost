package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	"github.com/filecoin-project/boost/storagemarket/types"

	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
)

// TestDDODealOnChain covers the three ways a DDO deal recorded in boost's DB
// reads on chain when the migration runs, one per era plus the mismatch.
//
// The post-nv29 rows are the load-bearing ones: FIP-0118 writes no claim there,
// so a nil claim has to fall through to the sector's weight. Reading it as "the
// deal did not land" instead would quietly drop every post-nv29 direct deal from
// a lotus -> curio migration, and the row where the sector really is empty is
// what keeps the fall-through from turning into "migrate everything".
func TestDDODealOnChain(t *testing.T) {
	const dealSector = abi.SectorNumber(3)
	const otherSector = abi.SectorNumber(4)

	empty := &miner.SectorOnChainInfo{DealWeight: big.Zero(), VerifiedDealWeight: big.Zero()}
	carries := &miner.SectorOnChainInfo{DealWeight: big.Zero(), VerifiedDealWeight: big.NewInt(1)}

	tests := []struct {
		name string
		// claim and si are the two reads; si is nil on the paths where it is not
		// taken, which is how the migration avoids a second call before nv29.
		claim   *verifreg9types.Claim
		si      *miner.SectorOnChainInfo
		want    bool
		wantErr string
	}{
		{
			name:  "nv28: the claim hits the deal's sector",
			claim: &verifreg9types.Claim{Sector: dealSector},
			want:  true,
		},
		{
			name:    "nv28: the claim points at another sector",
			claim:   &verifreg9types.Claim{Sector: otherSector},
			wantErr: "sector mismatch for deal",
		},
		{
			name: "nv29: no claim, the sector carries data",
			si:   carries,
			want: true,
		},
		{
			name: "nv29: no claim and the sector is empty",
			si:   empty,
			want: false,
		},
		{
			name: "no claim and this node cannot see the sector",
			si:   nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ddoDealOnChain(tt.claim, tt.si, dealSector)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestFatalSkip pins which recorded fatal errors the DDO migration may decide
// again for itself.
//
// The no-claim row is the bug this guards: a released Boost reached that sentence
// on every post-nv29 direct deal, because FIP-0118 stopped writing the claim it
// was waiting for, so treating it as a settled verdict drops those deals from
// every lotus -> curio migration. The other rows are the counterweight -- a fatal
// error the chain check cannot see still has to stop the migration, or the
// excuse turns into "migrate every deal Boost ever rejected".
func TestFatalSkip(t *testing.T) {
	// Pinned verbatim: this is the string a released Boost wrote into
	// direct_deals.error, so the wire format is the thing under test, not a
	// symbol the migration happens to own.
	releasedNoClaimErr := "no claim found"

	tests := []struct {
		name  string
		err   string
		retry types.DealRetryType
		want  bool
	}{
		{
			name:  "no claim found, fatal: decided on chain instead",
			err:   releasedNoClaimErr,
			retry: types.DealRetryFatal,
			want:  false,
		},
		{
			name:  "a claim in the wrong sector is still fatal",
			err:   "sector mismatch for claim",
			retry: types.DealRetryFatal,
			want:  true,
		},
		{
			name:  "an unreadable piece is still fatal",
			err:   "failed to read piece data: unexpected EOF",
			retry: types.DealRetryFatal,
			want:  true,
		},
		{
			name:  "the no-claim wording is not excused when it is only a prefix",
			err:   "no claim found: sector 3 is on chain from before nv29 without a claim for allocation 5",
			retry: types.DealRetryFatal,
			want:  true,
		},
		{
			name:  "no claim found, but only a retryable failure",
			err:   releasedNoClaimErr,
			retry: types.DealRetryAuto,
			want:  false,
		},
		{
			name:  "no error recorded",
			retry: types.DealRetryAuto,
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, fatalSkip(tt.err, tt.retry))
		})
	}
}
