package piecedirectory

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifregtypes "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/sectorstatemgr"

	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
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

	// Each read can be made to fail on demand, so the doctor's error branches are
	// reachable without an index directory to break.
	mdErr        error
	isIndexedErr error
	flagErr      error
	unflagErr    error

	getMetadataCalls int
	isIndexedCalls   int
	unflagged        []cid.Cid
	flagged          []cid.Cid
	untracked        []cid.Cid
}

func (f *fakePieceStore) NextPiecesToCheck(context.Context, address.Address) ([]cid.Cid, error) {
	return nil, nil
}

func (f *fakePieceStore) GetPieceMetadata(context.Context, cid.Cid) (model.Metadata, error) {
	f.getMetadataCalls++
	return f.md, f.mdErr
}

func (f *fakePieceStore) IsIndexed(context.Context, cid.Cid) (bool, error) {
	f.isIndexedCalls++
	return f.isIndexed, f.isIndexedErr
}

func (f *fakePieceStore) FlagPiece(_ context.Context, piece cid.Cid, _ bool, _ address.Address) error {
	if f.flagErr != nil {
		return f.flagErr
	}
	f.flagged = append(f.flagged, piece)
	return nil
}

func (f *fakePieceStore) UnflagPiece(_ context.Context, piece cid.Cid, _ address.Address) error {
	if f.unflagErr != nil {
		return f.unflagErr
	}
	f.unflagged = append(f.unflagged, piece)
	return nil
}

func (f *fakePieceStore) UntrackPiece(_ context.Context, piece cid.Cid, _ address.Address) error {
	f.untracked = append(f.untracked, piece)
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

// TestCheckPiecePostNv29WeightArm drives the doctor with a sector whose only
// spacetime weight is in VerifiedDealWeight -- the shape FIP-0118 gives every
// sector from nv29 on -- and builds SectorWithDeals with the same predicate the
// sector-state manager uses. Each half is unit-tested on its own; this pins the
// seam between them, because a doctor fed the manager's real output is the only
// thing that decides whether a post-nv29 piece goes unflagged.
//
// The pre-nv29 row is the control: it reaches the doctor through the other
// weight arm, so a regression that goes back to reading DealWeight alone breaks
// the post-nv29 row and leaves the control standing.
func TestCheckPiecePostNv29WeightArm(t *testing.T) {
	const sectorNum = abi.SectorNumber(3)
	const allocID = abi.DealID(5)

	pieceCid := cid.MustParse("baga6ea4seaqlwk3idbvhtetlir6yl4gfx7ql2kctaszujoudjavzjor7aphiccy")
	maddr, err := address.NewIDAddress(1011)
	require.NoError(t, err)
	mid, err := address.IDFromAddress(maddr)
	require.NoError(t, err)
	sectorID := abi.SectorID{Miner: abi.ActorID(mid), Number: sectorNum}

	tests := []struct {
		name          string
		si            *miner.SectorOnChainInfo
		wantCarries   bool
		wantIsIndexed bool
	}{
		{
			name:          "post-nv29: verified weight only",
			si:            &miner.SectorOnChainInfo{SectorNumber: sectorNum, DealWeight: big.Zero(), VerifiedDealWeight: abi.NewStoragePower(1)},
			wantCarries:   true,
			wantIsIndexed: true,
		},
		{
			name:          "pre-nv29 control: deal weight only",
			si:            &miner.SectorOnChainInfo{SectorNumber: sectorNum, DealWeight: abi.NewStoragePower(1), VerifiedDealWeight: big.Zero()},
			wantCarries:   true,
			wantIsIndexed: true,
		},
		{
			name:        "pure CC: neither weight",
			si:          &miner.SectorOnChainInfo{SectorNumber: sectorNum, DealWeight: big.Zero(), VerifiedDealWeight: big.Zero()},
			wantCarries: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build SectorWithDeals the way sectorstatemgr.refreshState does, so
			// the doctor is fed what production would feed it.
			sectorWithDeals := map[abi.SectorID]struct{}{}
			if sectorstatemgr.SectorCarriesData(tt.si) {
				sectorWithDeals[sectorID] = struct{}{}
			}
			require.Equal(t, tt.wantCarries, len(sectorWithDeals) == 1,
				"the predicate and the doctor have to agree on this sector")

			store := &fakePieceStore{
				md: model.Metadata{Deals: []model.DealInfo{{
					MinerAddr:    maddr,
					SectorID:     sectorNum,
					ChainDealID:  allocID,
					IsDirectDeal: true,
				}}},
				// Indexed with an unsealed copy, so a piece that gets past the
				// on-chain check has nothing wrong with it.
				isIndexed: true,
			}
			d := &Doctor{maddr: maddr, store: store, fullnodeApi: lotusmocks.NewMockFullNode(gomock.NewController(t))}
			lu := &sectorstatemgr.SectorStateUpdates{
				ActiveSectors:   map[abi.SectorID]struct{}{sectorID: {}},
				SectorWithDeals: sectorWithDeals,
				SectorStates:    map[abi.SectorID]db.SealState{sectorID: db.SealStateUnsealed},
			}

			err := d.checkPiece(context.Background(), pieceCid, lu, &types.TipSet{}, nil)
			require.NoError(t, err)

			if tt.wantIsIndexed {
				require.Equal(t, 1, store.isIndexedCalls)
			} else {
				require.Zero(t, store.isIndexedCalls,
					"a sector with no weight in either arm must stop the check")
			}
			require.Empty(t, store.flagged, "nothing is wrong with this piece")
			require.Equal(t, []cid.Cid{pieceCid}, store.unflagged)
		})
	}
}

// TestCheckPieceStoreErrors pins that a read against the local index directory
// is surfaced to the caller rather than being read as a verdict on the piece.
// None of these branches is nv29-specific, but they were unreachable before: the
// only other piece-doctor test needs a live index directory, so it cannot make a
// read fail on demand.
func TestCheckPieceStoreErrors(t *testing.T) {
	const sectorNum = abi.SectorNumber(3)
	const allocID = abi.DealID(5)

	pieceCid := cid.MustParse("baga6ea4seaqlwk3idbvhtetlir6yl4gfx7ql2kctaszujoudjavzjor7aphiccy")
	maddr, err := address.NewIDAddress(1011)
	require.NoError(t, err)
	mid, err := address.IDFromAddress(maddr)
	require.NoError(t, err)
	sectorID := abi.SectorID{Miner: abi.ActorID(mid), Number: sectorNum}

	// A piece on chain, indexed, with an unsealed copy in an active sector: the
	// check reaches its last store call unless a read is made to fail first.
	metadata := model.Metadata{Deals: []model.DealInfo{{
		MinerAddr:    maddr,
		SectorID:     sectorNum,
		ChainDealID:  allocID,
		IsDirectDeal: true,
	}}}
	sectorState := func() *sectorstatemgr.SectorStateUpdates {
		return &sectorstatemgr.SectorStateUpdates{
			ActiveSectors:   map[abi.SectorID]struct{}{sectorID: {}},
			SectorWithDeals: map[abi.SectorID]struct{}{sectorID: {}},
			SectorStates:    map[abi.SectorID]db.SealState{sectorID: db.SealStateUnsealed},
		}
	}

	tests := []struct {
		name    string
		store   *fakePieceStore
		wantErr string
	}{
		{
			name:    "metadata read fails",
			store:   &fakePieceStore{mdErr: errors.New("index directory down")},
			wantErr: "failed to get piece",
		},
		{
			name:  "piece not found unflags and untracks",
			store: &fakePieceStore{mdErr: errors.New("piece not found")},
		},
		{
			name:    "index status read fails",
			store:   &fakePieceStore{md: metadata, isIndexedErr: errors.New("index directory down")},
			wantErr: "failed to check index status",
		},
		{
			name:    "flag fails",
			store:   &fakePieceStore{md: metadata, isIndexed: false, flagErr: errors.New("index directory down")},
			wantErr: "failed to flag piece",
		},
		{
			name:    "unflag fails",
			store:   &fakePieceStore{md: metadata, isIndexed: true, unflagErr: errors.New("index directory down")},
			wantErr: "failed to unflag piece",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Doctor{maddr: maddr, store: tt.store, fullnodeApi: lotusmocks.NewMockFullNode(gomock.NewController(t))}
			err := d.checkPiece(context.Background(), pieceCid, sectorState(), &types.TipSet{}, nil)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			require.Equal(t, []cid.Cid{pieceCid}, tt.store.unflagged)
			require.Equal(t, []cid.Cid{pieceCid}, tt.store.untracked)
		})
	}
}

// TestClaimsForCheck pins that the claims are read whenever a batch of pieces is
// checked, and that a failure in the read is not passed on to the batch.
//
// The read is kept at nv29+ rather than skipped: the claims of the sectors being
// checked can predate the upgrade, and only FIP-0118's post-nv29 deals are
// claimless by construction. Not returning the error is what keeps a node that
// cannot answer from stopping the doctor; direct deals fall back to the sector
// weight for the round and the claims are read again on the next one.
func TestClaimsForCheck(t *testing.T) {
	maddr, err := address.NewIDAddress(1011)
	require.NoError(t, err)

	t.Run("the claims are read", func(t *testing.T) {
		api := lotusmocks.NewMockFullNode(gomock.NewController(t))
		want := map[verifregtypes.ClaimId]verifregtypes.Claim{verifregtypes.ClaimId(5): {Sector: 3}}
		api.EXPECT().StateGetClaims(gomock.Any(), maddr, types.EmptyTSK).Return(want, nil)
		d := &Doctor{maddr: maddr, fullnodeApi: api}

		claims := d.claimsForCheck(context.Background())

		require.Equal(t, want, claims)
	})

	t.Run("a read failure leaves the claims empty", func(t *testing.T) {
		api := lotusmocks.NewMockFullNode(gomock.NewController(t))
		api.EXPECT().StateGetClaims(gomock.Any(), maddr, types.EmptyTSK).Return(nil, errors.New("node down"))
		d := &Doctor{maddr: maddr, fullnodeApi: api}

		require.Nil(t, d.claimsForCheck(context.Background()))
	})
}
