package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-state-types/abi"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
)

const (
	sectorID     = abi.SectorNumber(2)
	otherSector  = abi.SectorNumber(3)
	sealedAtNv29 = true
)

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

// TestMigratableDirectDeal covers the gate the migration runs over every direct
// deal. The nv29 cases matter most: a claim that will never exist must not read
// as the data having left the chain, since that leaves the deal record and its
// retrieval mapping behind in a migration that has already sealed the data.
func TestMigratableDirectDeal(t *testing.T) {
	tests := map[string]struct {
		deal            *types.DirectDeal
		claim           *verifreg9types.Claim
		sealedAfterNv29 bool
		sectorAlive     bool
		want            bool
		wantReason      string
		wantErr         string
	}{
		"a claimed deal sealed before nv29": {
			deal:        migratableDeal(),
			claim:       claimOnSector(sectorID),
			sectorAlive: true,
			want:        true,
		},
		"a deal sealed after nv29 with no claim, which is the steady state there": {
			deal:            migratableDeal(),
			sealedAfterNv29: sealedAtNv29,
			sectorAlive:     true,
			want:            true,
		},
		"a deal whose sector sealed after nv29 still gets its claim checked": {
			deal:            migratableDeal(),
			claim:           claimOnSector(otherSector),
			sealedAfterNv29: sealedAtNv29,
			sectorAlive:     true,
			wantErr:         "sector mismatch",
		},
		"a claim that will never exist must not strand a deal failed by an older boost": {
			// Boost used to fail such a deal once the claim lookup came back
			// empty. Skipping it here leaves the deal record and the retrieval
			// mapping out of Curio even though the data sealed.
			deal: func() *types.DirectDeal {
				d := migratableDeal()
				d.Err = "no claim found"
				d.Retry = types.DealRetryFatal
				return d
			}(),
			sealedAfterNv29: sealedAtNv29,
			sectorAlive:     true,
			want:            true,
		},
		"a deal sealed before nv29 with no claim is still skipped": {
			deal:        migratableDeal(),
			sectorAlive: true,
			wantReason:  "no claim was found for a sector sealed before nv29",
		},
		"a failed deal sealed before nv29 is still skipped": {
			deal: func() *types.DirectDeal {
				d := migratableDeal()
				d.Err = "no claim found"
				d.Retry = types.DealRetryFatal
				return d
			}(),
			sectorAlive: true,
			wantReason:  "the deal retry is fatal",
		},
		"a deal that never reached a sector": {
			deal: &types.DirectDeal{
				Checkpoint: dealcheckpoints.Accepted,
			},
			sealedAfterNv29: sealedAtNv29,
			sectorAlive:     true,
			wantReason:      "the checkpoint is below add piece",
		},
		"a deal whose sector is gone": {
			deal:            migratableDeal(),
			sealedAfterNv29: sealedAtNv29,
			wantReason:      "the deal sector is no longer alive",
		},
		"a deal whose sector is gone but whose claim is still there": {
			deal:       migratableDeal(),
			claim:      claimOnSector(sectorID),
			wantReason: "the deal sector is no longer alive",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ok, reason, err := migratableDirectDeal(tc.deal, tc.claim, tc.sealedAfterNv29, tc.sectorAlive)

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
