package main

import (
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	zerocomm "github.com/filecoin-project/go-commp-utils/v2/zerocomm"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/boost/storagemarket/types"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
)

// pieceCID builds a distinct, valid piece cid per seed, so the tests can talk
// about "our piece" and "someone else's piece" without hand-copied CIDs. Only
// equality matters here, never the multihash contents.
func pieceCID(t *testing.T, seed string) cid.Cid {
	t.Helper()
	sum, err := mh.Sum([]byte(seed), mh.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, sum)
}

// sectorPiece is one entry of a sealer piece list.
func sectorPiece(size abi.PaddedPieceSize, pc cid.Cid) api.SectorPiece {
	return api.SectorPiece{Piece: abi.PieceInfo{Size: size, PieceCID: pc}}
}

// paddingPiece is the gap-filler the sealer inserts to align a piece, or to fill
// the sector out. It is a real entry in the piece list at a real offset, with no
// deal behind it, and lotus's committed cid for it is zerocomm's commitment to
// zero bytes -- this is what a migration will actually read out of
// sectors_meta_pieces, not an undefined cid.
func paddingPiece(size abi.PaddedPieceSize) api.SectorPiece {
	return sectorPiece(size, zerocomm.ZeroPieceCommitment(size.Unpadded()))
}

// TestDDODealOnChain covers how a DDO deal recorded in boost's DB reads when the
// migration runs.
//
// The rows that carry the change are the zero-claim ones: FIP-0118 writes no
// claim after nv29, so a nil claim is normal there and the sector has to answer
// instead. The sector's *weight* alone is not an answer -- it is a property of
// the whole sector and says "something landed here", not "this deal landed
// here". The snap-aborted-then-reused row is what keeps that distinction: the
// sector carries data from someone else's piece, our deal is gone, and
// recovering it would hand the client data that was never sealed.
func TestDDODealOnChain(t *testing.T) {
	const dealSector = abi.SectorNumber(3)
	const otherSector = abi.SectorNumber(4)
	const pieceSize = abi.PaddedPieceSize(512)

	ours := pieceCID(t, "our piece")
	theirs := pieceCID(t, "another deal's piece")

	// The deal as boost recorded it: our piece, 512 bytes, at offset 128 (one
	// padding piece in front of it).
	deal := &types.DirectDeal{
		SectorID:  dealSector,
		PieceCID:  ours,
		PieceSize: pieceSize,
		Offset:    128,
	}

	empty := &miner.SectorOnChainInfo{DealWeight: big.Zero(), VerifiedDealWeight: big.Zero()}
	carries := &miner.SectorOnChainInfo{DealWeight: big.Zero(), VerifiedDealWeight: big.NewInt(1)}

	// The sealer's list for a sector holding our piece at our offset, behind one
	// padding piece.
	withOurs := []api.SectorPiece{paddingPiece(128), sectorPiece(pieceSize, ours)}
	// The aborted-snap-then-reused sector: the same sector slot holds a
	// different piece, at the same offset, from a different deal. The sector's
	// weight is non-zero and our piece is simply not there.
	withTheirs := []api.SectorPiece{paddingPiece(128), sectorPiece(pieceSize, theirs)}
	// Our piece, but not at our offset -- an identical piece belonging to
	// another deal can sit anywhere else in the sector.
	withOursElsewhere := []api.SectorPiece{paddingPiece(128), sectorPiece(pieceSize, theirs), sectorPiece(pieceSize, ours)}

	tests := []struct {
		name string
		// claim, si, pieces and piecesKnown are the reads; a nil si is the path
		// where no claim was found and the sector is not on this node's chain,
		// and piecesKnown is false when the list could not be read at all.
		claim       *verifreg9types.Claim
		si          *miner.SectorOnChainInfo
		pieces      []api.SectorPiece
		piecesKnown bool
		want        ddoOnChainResult
		wantErr     string
	}{
		{
			name:  "nv28: the claim hits the deal's sector",
			claim: &verifreg9types.Claim{Sector: dealSector},
			want:  ddoOnChain,
		},
		{
			name:    "nv28: the claim points at another sector",
			claim:   &verifreg9types.Claim{Sector: otherSector},
			want:    ddoNotOnChain,
			wantErr: "sector mismatch for deal",
		},
		{
			name:  "nv28: the claim hits, so an unreadable piece list does not matter",
			claim: &verifreg9types.Claim{Sector: dealSector},
			want:  ddoOnChain,
		},
		{
			name: "nv29: no claim and this node cannot see the sector",
			want: ddoNotOnChain,
		},
		{
			name: "nv29: no claim and the sector is empty",
			si:   empty,
			want: ddoNotOnChain,
		},
		{
			name:        "nv29: no claim and the sealer lists our piece at our offset",
			si:          carries,
			pieces:      withOurs,
			piecesKnown: true,
			want:        ddoOnChain,
		},
		{
			name:        "snap aborted, another deal's piece took the sector: the weight alone would have recovered this deal",
			si:          carries,
			pieces:      withTheirs,
			piecesKnown: true,
			want:        ddoNotOnChain,
		},
		{
			name:        "our piece is in the sector but at another offset",
			si:          carries,
			pieces:      withOursElsewhere,
			piecesKnown: true,
			want:        ddoNotOnChain,
		},
		{
			name:        "no claim, the sector carries data and the list could not be read",
			si:          carries,
			pieces:      nil,
			piecesKnown: false,
			want:        ddoNoPieceSource,
		},
		{
			// A sector carrying data has pieces; an empty recorded list means the
			// list was never written, so this must not read as "our piece is not
			// there". Getting this wrong drops every post-nv29 deal silently.
			name:        "no claim, the sector carries data and its piece list came back empty",
			si:          carries,
			pieces:      []api.SectorPiece{},
			piecesKnown: true,
			want:        ddoNoPieceSource,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ddoDealOnChain(tt.claim, tt.si, tt.pieces, tt.piecesKnown, deal)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				// The error is a contradiction the run cannot repair, so the
				// result has to read as "no" rather than as a third state the
				// caller might mistake for "undecided".
				require.Equal(t, ddoNotOnChain, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestDDODealOnChainPieceIdentity pins the three fields the piece check matches
// on. The sector's weight is the same in every row, so a check that read the
// weight and stopped -- the behaviour this replaces -- would answer ddoOnChain
// to all of them.
func TestDDODealOnChainPieceIdentity(t *testing.T) {
	const dealSector = abi.SectorNumber(3)
	const pieceSize = abi.PaddedPieceSize(512)

	ours := pieceCID(t, "our piece")
	si := &miner.SectorOnChainInfo{VerifiedDealWeight: big.NewInt(1)}

	base := types.DirectDeal{SectorID: dealSector, PieceCID: ours, PieceSize: pieceSize, Offset: 0}

	tests := []struct {
		name  string
		deal  types.DirectDeal
		got   []api.SectorPiece
		want  ddoOnChainResult
		about string
	}{
		{
			name:  "the piece as recorded",
			deal:  base,
			got:   []api.SectorPiece{sectorPiece(pieceSize, ours)},
			want:  ddoOnChain,
			about: "the baseline: everything matches",
		},
		{
			name:  "same cid and size, different offset",
			deal:  func() types.DirectDeal { d := base; d.Offset = 128; return d }(),
			got:   []api.SectorPiece{sectorPiece(pieceSize, ours)},
			want:  ddoNotOnChain,
			about: "offset is what tells two copies of one piece apart",
		},
		{
			name:  "same cid, the sector's size differs",
			deal:  func() types.DirectDeal { d := base; d.PieceSize = 256; return d }(),
			got:   []api.SectorPiece{sectorPiece(pieceSize, ours)},
			want:  ddoNotOnChain,
			about: "the recorded size has to be the size that is in the sector",
		},
		{
			name:  "same cid and offset, a different piece of the same size",
			deal:  func() types.DirectDeal { d := base; d.Offset = 512; return d }(),
			got:   []api.SectorPiece{sectorPiece(pieceSize, ours), sectorPiece(pieceSize, pieceCID(t, "another deal's piece"))},
			want:  ddoNotOnChain,
			about: "a piece of ours elsewhere in the sector is not a piece at our offset",
		},
		{
			name:  "a piece whose cid is a different piece of the same size and offset",
			deal:  func() types.DirectDeal { d := base; d.PieceCID = pieceCID(t, "a piece boost never sealed"); return d }(),
			got:   []api.SectorPiece{sectorPiece(pieceSize, ours)},
			want:  ddoNotOnChain,
			about: "the cid is the piece's identity, not its position",
		},
		{
			name:  "the deal's piece is not in the sector and the sector is not empty",
			deal:  func() types.DirectDeal { d := base; d.PieceCID = pieceCID(t, "a piece boost never sealed"); return d }(),
			got:   []api.SectorPiece{sectorPiece(pieceSize, ours), sectorPiece(pieceSize, pieceCID(t, "another deal's piece"))},
			want:  ddoNotOnChain,
			about: "the snap-aborted case with other data present",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ddoDealOnChain(nil, si, tt.got, true, &tt.deal)
			require.NoError(t, err)
			require.Equal(t, tt.want, got, tt.about)
		})
	}
}

// TestDDOOffsetAccumulatesOverPaddingPieces is the premise the piece check rests
// on: boost's recorded Offset and PieceInSector's running total are the same
// number. Boost takes the offset the sealer returns for the piece it just added,
// which is the sum of everything already in the sector -- padding pieces
// included, because the sealer appends them to the piece list too -- and the
// migration rebuilds it by accumulating the recorded list in the same order.
//
// If padding were excluded from either side, an unsealed sector with a padding
// piece in front of ours would be skipped: the migration would be reading a
// layout that never existed and dropping a perfectly good deal.
func TestDDOOffsetAccumulatesOverPaddingPieces(t *testing.T) {
	ours := pieceCID(t, "our piece")
	other := pieceCID(t, "another deal's piece")

	// A 2048 sector holding, in order: a padding piece, another deal's piece,
	// ours, and a tail padding piece. Ours went in third, behind the leading
	// padding piece, which is what the sealer told boost when it returned the
	// offset -- and the same layout the migration reads back from
	// sectors_meta_pieces, padding rows and all.
	list := []api.SectorPiece{
		paddingPiece(256),
		sectorPiece(512, other),
		sectorPiece(512, ours),
		paddingPiece(768),
	}

	// The offset boost recorded: the sizes of everything ahead of our piece,
	// padding included.
	recorded := abi.PaddedPieceSize(256 + 512)

	require.True(t, sectorstatemgr.PieceInSector(list, ours, 512, recorded))

	// The counterfactual: an offset that ignores the leading padding piece -- the
	// arithmetic you get from a piece list without it -- lands 256 short of where
	// our piece really is, and the deal would be dropped on a sector it is in.
	require.False(t, sectorstatemgr.PieceInSector(list, ours, 512, recorded-256))

	// A padding piece is not "our piece" even at our offset and size: the cid is
	// what it carries, and a commitment to zero bytes is not our data.
	require.False(t, sectorstatemgr.PieceInSector(list, paddingPiece(512).Piece.PieceCID, 512, recorded))
}

// TestDDOPiecesFromRows covers the read of Curio's recorded piece list, both of
// the shape the accumulator depends on and the one failure that must not be
// swallowed.
func TestDDOPiecesFromRows(t *testing.T) {
	const sectorNum = abi.SectorNumber(7)
	ours := pieceCID(t, "our piece")
	pad := paddingPiece(128).Piece.PieceCID

	rows := []ddoSectorPieceRow{
		{PieceNum: 0, PieceCID: pad.String(), PieceSize: 128},
		{PieceNum: 1, PieceCID: ours.String(), PieceSize: 512},
	}

	pieces, err := ddoPiecesFromRows(sectorNum, rows)
	require.NoError(t, err)
	require.Len(t, pieces, 2)
	// The list has to come back in offset order with the padded sizes intact,
	// or the accumulator lands on the wrong offset.
	require.Equal(t, abi.PaddedPieceSize(128), pieces[0].Piece.Size)
	require.Equal(t, abi.PaddedPieceSize(512), pieces[1].Piece.Size)
	require.True(t, sectorstatemgr.PieceInSector(pieces, ours, 512, 128))

	// An unparseable cid cannot be skipped: dropping the row would shift every
	// piece behind it and silently mismatch the offsets.
	_, err = ddoPiecesFromRows(sectorNum, []ddoSectorPieceRow{
		{PieceNum: 0, PieceCID: "not a cid", PieceSize: 128},
		{PieceNum: 1, PieceCID: ours.String(), PieceSize: 512},
	})
	require.ErrorContains(t, err, "parsing piece cid")
	require.ErrorContains(t, err, "sector 7")
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
