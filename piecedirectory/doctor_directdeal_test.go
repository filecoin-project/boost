package piecedirectory

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	verifregtypes "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/sectorstatemgr"

	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	"github.com/filecoin-project/lotus/chain/types"
)

// TestDirectDealOnChain covers both eras of the doctor's on-chain check. The
// post-nv29 rows are the ones that matter: FIP-0118 writes no claim, so a nil
// claim must fall through to the sector's weight rather than being read as "the
// deal is not on chain", which would skip every post-nv29 direct deal before the
// index and unsealed-copy checks that follow.
func TestDirectDealOnChain(t *testing.T) {
	const (
		allocID     = abi.DealID(5)
		sectorNum   = abi.SectorNumber(3)
		otherSector = abi.SectorNumber(4)
	)

	sectorID := abi.SectorID{Miner: 1011, Number: sectorNum}
	otherSectorID := abi.SectorID{Miner: 1011, Number: otherSector}

	claims := map[verifregtypes.ClaimId]verifregtypes.Claim{
		verifregtypes.ClaimId(allocID): {Sector: sectorNum},
		// A claim for a different allocation, landing in the same sector. It must
		// not stand in for this deal's own claim.
		verifregtypes.ClaimId(99): {Sector: sectorNum},
	}
	elsewhere := map[verifregtypes.ClaimId]verifregtypes.Claim{
		verifregtypes.ClaimId(allocID): {Sector: otherSector},
	}
	withDeals := map[abi.SectorID]struct{}{sectorID: {}}

	tests := []struct {
		name     string
		claims   map[verifregtypes.ClaimId]verifregtypes.Claim
		withDeal map[abi.SectorID]struct{}
		checkFor abi.SectorID
		want     bool
	}{
		{
			name:     "nv28: claim hits this sector",
			claims:   claims,
			checkFor: sectorID,
			want:     true,
		},
		{
			name:     "nv28: claim points at another sector",
			claims:   elsewhere,
			withDeal: withDeals,
			checkFor: sectorID,
			want:     false,
		},
		{
			name:     "another deal's claim in this sector does not stand in for this deal's",
			claims:   map[verifregtypes.ClaimId]verifregtypes.Claim{verifregtypes.ClaimId(99): {Sector: sectorNum}},
			withDeal: withDeals,
			checkFor: sectorID,
			want:     true, // no claim of its own, so the sector decides
		},
		{
			name:     "nv29: no claim, sector carries data",
			claims:   nil,
			withDeal: withDeals,
			checkFor: sectorID,
			want:     true,
		},
		{
			name:     "no claim and the sector is not on chain",
			claims:   nil,
			withDeal: withDeals,
			checkFor: otherSectorID,
			want:     false,
		},
		{
			name:     "no claim and no sector state at all",
			claims:   nil,
			withDeal: nil,
			checkFor: sectorID,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, directDealOnChain(allocID, tt.checkFor, tt.claims, tt.withDeal))
		})
	}
}

// fakePieceStore stands in for the local index directory, which the real
// doctor_test.go reaches through a yugabyte instance.
type fakePieceStore struct {
	md        model.Metadata
	isIndexed bool

	getMetadataCalls int
	isIndexedCalls   int
	unflagged        []cid.Cid
	flagged          []cid.Cid
}

func (f *fakePieceStore) NextPiecesToCheck(context.Context, address.Address) ([]cid.Cid, error) {
	return nil, nil
}

func (f *fakePieceStore) GetPieceMetadata(context.Context, cid.Cid) (model.Metadata, error) {
	f.getMetadataCalls++
	return f.md, nil
}

func (f *fakePieceStore) IsIndexed(context.Context, cid.Cid) (bool, error) {
	f.isIndexedCalls++
	return f.isIndexed, nil
}

func (f *fakePieceStore) FlagPiece(_ context.Context, piece cid.Cid, _ bool, _ address.Address) error {
	f.flagged = append(f.flagged, piece)
	return nil
}

func (f *fakePieceStore) UnflagPiece(_ context.Context, piece cid.Cid, _ address.Address) error {
	f.unflagged = append(f.unflagged, piece)
	return nil
}

func (f *fakePieceStore) UntrackPiece(_ context.Context, piece cid.Cid, _ address.Address) error {
	f.unflagged = append(f.unflagged, piece)
	return nil
}

// TestCheckPieceDirectDealOnChain drives the doctor all the way to its
// on-chain check, which is where the nv29 change bites: before it, a missing
// claim meant the deal was gone and the piece was unflagged; from nv29 on the
// claim is absent by design, so reading an absent claim that way would quietly
// unflag every post-nv29 direct deal's piece.
//
// The two rows differ only in whether the sector is in the doctor's set of
// sectors that carry data, so the pair pins the fall-through: the first row
// stops at the on-chain check (IsIndexed is never read), the second walks past
// it. A check that treated every absent claim as missing would unflag in both.
func TestCheckPieceDirectDealOnChain(t *testing.T) {
	const sectorNum = abi.SectorNumber(3)
	const allocID = abi.DealID(5)

	pieceCid := cid.MustParse("baga6ea4seaqlwk3idbvhtetlir6yl4gfx7ql2kctaszujoudjavzjor7aphiccy")
	maddr, err := address.NewIDAddress(1011)
	require.NoError(t, err)
	mid, err := address.IDFromAddress(maddr)
	require.NoError(t, err)
	sectorID := abi.SectorID{Miner: abi.ActorID(mid), Number: sectorNum}

	tests := []struct {
		name            string
		sectorWithDeals map[abi.SectorID]struct{}
		wantIsIndexed   bool
	}{
		{
			name:            "nv29: no claim and the sector does not carry data",
			sectorWithDeals: nil,
		},
		{
			name:            "nv29: no claim but the sector carries data",
			sectorWithDeals: map[abi.SectorID]struct{}{sectorID: {}},
			wantIsIndexed:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakePieceStore{
				md: model.Metadata{Deals: []model.DealInfo{{
					MinerAddr:    maddr,
					SectorID:     sectorNum,
					ChainDealID:  allocID,
					IsDirectDeal: true,
				}}},
				// Indexed, with an unsealed copy below, so a piece that reaches the
				// end of the check has nothing wrong with it.
				isIndexed: true,
			}
			// Non-nil so the on-chain check runs at all; a direct deal must never
			// reach for the market-deal state.
			fullnodeApi := lotusmocks.NewMockFullNode(gomock.NewController(t))

			d := &Doctor{maddr: maddr, store: store, fullnodeApi: fullnodeApi}
			lu := &sectorstatemgr.SectorStateUpdates{
				ActiveSectors:   map[abi.SectorID]struct{}{sectorID: {}},
				SectorWithDeals: tt.sectorWithDeals,
				SectorStates:    map[abi.SectorID]db.SealState{sectorID: db.SealStateUnsealed},
			}

			// No claims: post-nv29, so the sector's weight is the only evidence
			// left, and head is never reached for a direct deal.
			err := d.checkPiece(context.Background(), pieceCid, lu, &types.TipSet{}, nil)

			require.NoError(t, err)
			require.Equal(t, 1, store.getMetadataCalls)
			require.Empty(t, store.flagged, "nothing is wrong with this piece")
			// Unflagged once, and only once: either as a missing deal or, when it
			// is on chain, as a clean piece at the end of the check.
			require.Equal(t, []cid.Cid{pieceCid}, store.unflagged)

			if tt.wantIsIndexed {
				require.Equal(t, 1, store.isIndexedCalls)
			} else {
				require.Zero(t, store.isIndexedCalls,
					"an off-chain deal must stop the check, not fall through to the index checks")
			}
		})
	}
}
