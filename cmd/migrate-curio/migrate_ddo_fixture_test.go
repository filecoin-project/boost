package main

import (
	"bytes"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	commp "github.com/filecoin-project/go-commp-utils/v2"
	zerocomm "github.com/filecoin-project/go-commp-utils/v2/zerocomm"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/boost/storagemarket/types"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
)

// This file reads a recorded sector the way a migration does: a fixture laid out
// like the rows sectors_meta_pieces actually holds, converted by the production
// code, checked against the sector a sealer really produced.
//
// The fixture is not a dump from a live database -- it is built here -- but every
// value in it is the value the real writers produce: the piece cids are real
// CommP commitments computed by the same library the sealer uses, the padding
// rows are zerocomm's commitments to zero bytes from the same table lotus reads,
// and the sizes are padded piece sizes. What it pins is the read path: that a
// padded size is taken as padded, that piece_num order is the offset order, and
// that the accumulated offsets land exactly where boost recorded them. What it
// cannot pin is the SQL itself -- `hdb.Select` and its `db` struct tags need a
// live harmony database, which this package has no harness for; the query is
// instead identical in columns, tags and ORDER BY to curio's own read of the same
// table in lib/dealdata/dealdata.go:97-115.

// realCommP computes the piece cid of a piece of the given padded size whose
// contents are `seed` repeated, the way the sealer computes it. Distinct seeds
// give distinct pieces of the same size.
func realCommP(t *testing.T, padded abi.PaddedPieceSize, seed string) cid.Cid {
	t.Helper()

	data := bytes.Repeat([]byte(seed), int(padded.Unpadded()))
	pc, err := commp.GeneratePieceCIDFromFile(abi.RegisteredSealProof_StackedDrg32GiBV1_1, bytes.NewReader(data), padded.Unpadded())
	require.NoError(t, err)
	return pc
}

// fixtureSector is a sector as the two sides see it: the recorded piece list on
// the left, and the deals boost packed into it on the right.
type fixtureSector struct {
	sectorNum abi.SectorNumber
	// rows is the sectors_meta_pieces content, in piece_num order, with the
	// padded sizes Postgres returns.
	rows []ddoSectorPieceRow
	// deals are the direct deals boost recorded against this sector, with the
	// padded offsets SectorAddPieceToAny returned for each of them.
	deals []*types.DirectDeal
	// pieces is the same sector as the sealer reports it, for the cases that go
	// through the sealer's API rather than the recorded table.
	pieces []api.SectorPiece
}

// packedSector2048 is a 2 KiB sector packed with two deals and the padding that
// separated and tailed them -- the shape a real sector has, at a size a test can
// hold. Its layout:
//
//	piece_num 0   1024  deal A's piece
//	piece_num 1    512  padding, aligning our deal behind deal A
//	piece_num 2    256  OUR piece            -> offset 1536
//	piece_num 3    128  padding
//	piece_num 4    128  deal B's piece       -> offset 1920
//
// The two expected offsets are written out by hand from the layout rather than
// computed by the accumulator under test, which is what makes this a check on the
// accumulator instead of a restatement of it.
func packedSector2048(t *testing.T) *fixtureSector {
	t.Helper()

	const (
		sectorNum = abi.SectorNumber(9)
		dealASize = abi.PaddedPieceSize(1024)
		pad1Size  = abi.PaddedPieceSize(512)
		ourSize   = abi.PaddedPieceSize(256)
		pad2Size  = abi.PaddedPieceSize(128)
		dealBSize = abi.PaddedPieceSize(128)
	)

	dealA := realCommP(t, dealASize, "A")
	ours := realCommP(t, ourSize, "o")
	dealB := realCommP(t, dealBSize, "B")

	const (
		ourOffset   = abi.PaddedPieceSize(1024 + 512)             // deal A + padding
		dealBOffset = abi.PaddedPieceSize(1024 + 512 + 256 + 128) // + our piece + padding
	)

	return &fixtureSector{
		sectorNum: sectorNum,
		rows: []ddoSectorPieceRow{
			{PieceNum: 0, PieceCID: dealA.String(), PieceSize: int64(dealASize)},
			{PieceNum: 1, PieceCID: zerocomm.ZeroPieceCommitment(pad1Size.Unpadded()).String(), PieceSize: int64(pad1Size)},
			{PieceNum: 2, PieceCID: ours.String(), PieceSize: int64(ourSize)},
			{PieceNum: 3, PieceCID: zerocomm.ZeroPieceCommitment(pad2Size.Unpadded()).String(), PieceSize: int64(pad2Size)},
			{PieceNum: 4, PieceCID: dealB.String(), PieceSize: int64(dealBSize)},
		},
		deals: []*types.DirectDeal{
			{SectorID: sectorNum, PieceCID: ours, PieceSize: ourSize, Offset: ourOffset},
			{SectorID: sectorNum, PieceCID: dealB, PieceSize: dealBSize, Offset: dealBOffset},
		},
		pieces: []api.SectorPiece{
			sectorPiece(dealASize, dealA),
			paddingPiece(pad1Size),
			sectorPiece(ourSize, ours),
			paddingPiece(pad2Size),
			sectorPiece(dealBSize, dealB),
		},
	}
}

// TestDDORecordedSectorLayout checks the migration's read of a recorded sector
// against the sector that was really sealed: both deals in it must be found at
// the offsets boost recorded, and the offsets only come out right if the padded
// sizes are taken as padded and the rows are read in piece_num order.
func TestDDORecordedSectorLayout(t *testing.T) {
	fx := packedSector2048(t)

	pieces, err := ddoPiecesFromRows(fx.sectorNum, fx.rows)
	require.NoError(t, err)
	require.Len(t, pieces, 5)

	// The recorded rows and the sealer's own list are the same piece list: the
	// table is written from it. Asserting that here means the fixture is not
	// "some list the test made up" but the sector both sides see.
	require.Equal(t, fx.pieces, pieces)

	for i, deal := range fx.deals {
		require.True(t, sectorstatemgr.PieceInSector(pieces, deal.PieceCID, deal.PieceSize, deal.Offset),
			"deal %d is in the sector at the offset boost recorded", i)
	}

	// The offsets are not interchangeable: each deal's piece is a different size
	// at a different offset, so a shifted accumulator cannot pass both.
	require.False(t, sectorstatemgr.PieceInSector(pieces, fx.deals[0].PieceCID, fx.deals[0].PieceSize, fx.deals[1].Offset))
	require.False(t, sectorstatemgr.PieceInSector(pieces, fx.deals[1].PieceCID, fx.deals[1].PieceSize, fx.deals[0].Offset))

	// The padding rows are load-bearing. Drop them and our piece slides to 1024:
	// a sector we are really in reads as one we are not, and the deal is skipped.
	noPadding := make([]api.SectorPiece, 0, 3)
	for _, p := range pieces {
		if p.Piece.PieceCID.Equals(zerocomm.ZeroPieceCommitment(p.Piece.Size.Unpadded())) {
			continue
		}
		noPadding = append(noPadding, p)
	}
	ourDeal := fx.deals[0]
	require.False(t, sectorstatemgr.PieceInSector(noPadding, ourDeal.PieceCID, ourDeal.PieceSize, ourDeal.Offset))

	// Piece order is built from piece_num, so the rows have to be read in that
	// order. Reversed -- what a query without ORDER BY may return -- the same
	// sector places our piece at 256 and the deal is dropped.
	reversed := make([]ddoSectorPieceRow, 0, len(fx.rows))
	for i := len(fx.rows) - 1; i >= 0; i-- {
		reversed = append(reversed, fx.rows[i])
	}
	revPieces, err := ddoPiecesFromRows(fx.sectorNum, reversed)
	require.NoError(t, err)
	require.False(t, sectorstatemgr.PieceInSector(revPieces, ourDeal.PieceCID, ourDeal.PieceSize, ourDeal.Offset))
}

// TestDDORecordedSectorDecides is the same fixture through the decision the
// migration actually makes, so the read and the verdict are checked together
// rather than one imagined to follow from the other.
func TestDDORecordedSectorDecides(t *testing.T) {
	fx := packedSector2048(t)
	carries := &miner.SectorOnChainInfo{DealWeight: big.Zero(), VerifiedDealWeight: big.NewInt(1)}

	pieces, err := ddoPiecesFromRows(fx.sectorNum, fx.rows)
	require.NoError(t, err)

	t.Run("post-nv29: no claim, our piece recorded where boost put it", func(t *testing.T) {
		got, err := ddoDealOnChain(nil, carries, pieces, true, fx.deals[0])
		require.NoError(t, err)
		require.Equal(t, ddoOnChain, got)
	})

	t.Run("the same sector, read from the chain without the piece list", func(t *testing.T) {
		// The pre-change behaviour: the weight alone answers, and this deal is
		// migrated on a sector it shares with two other pieces.
		got, err := ddoDealOnChain(nil, carries, nil, false, fx.deals[0])
		require.NoError(t, err)
		require.Equal(t, ddoNoPieceSource, got)
	})

	t.Run("snap aborted and the sector was reused by another deal", func(t *testing.T) {
		// The recorded sector ended up holding deal A and deal B only. The weight
		// is non-zero, so nothing but the piece check can tell that our deal's
		// data is not there.
		reused := []ddoSectorPieceRow{fx.rows[0], fx.rows[4]}
		reusedPieces, err := ddoPiecesFromRows(fx.sectorNum, reused)
		require.NoError(t, err)

		got, err := ddoDealOnChain(nil, carries, reusedPieces, true, fx.deals[0])
		require.NoError(t, err)
		require.Equal(t, ddoNotOnChain, got)

		// Deal B is the counterweight: its piece is in this sector, but not at
		// the offset boost recorded for it, so it is refused too. Presence alone
		// is not the question; the piece has to be the one we sealed, where we
		// sealed it, and a rewritten sector moves it.
		got, err = ddoDealOnChain(nil, carries, reusedPieces, true, fx.deals[1])
		require.NoError(t, err)
		require.Equal(t, ddoNotOnChain, got)
	})

	t.Run("our deal is the only piece in a one-deal sector", func(t *testing.T) {
		single := []ddoSectorPieceRow{{PieceNum: 0, PieceCID: fx.deals[0].PieceCID.String(), PieceSize: int64(fx.deals[0].PieceSize)}}
		singlePieces, err := ddoPiecesFromRows(fx.sectorNum, single)
		require.NoError(t, err)

		deal := *fx.deals[0]
		deal.Offset = 0 // the only piece in the sector starts at 0

		got, err := ddoDealOnChain(nil, carries, singlePieces, true, &deal)
		require.NoError(t, err)
		require.Equal(t, ddoOnChain, got)
	})

	t.Run("the recorded list is empty because guided-setup has not run", func(t *testing.T) {
		got, err := ddoDealOnChain(nil, carries, nil, true, fx.deals[0])
		require.NoError(t, err)
		require.Equal(t, ddoNoPieceSource, got, "an empty list is not evidence that our piece is absent")
	})
}

// TestDDOSectorPiecesKnownFalseIsNotAbsence guards the other half of the same
// mistake at the decision level: a list that could not be read at all must not
// fall through to the piece check with a nil list either.
func TestDDOSectorPiecesKnownFalseIsNotAbsence(t *testing.T) {
	carries := &miner.SectorOnChainInfo{VerifiedDealWeight: big.NewInt(1)}
	deal := &types.DirectDeal{PieceCID: realCommP(t, 128, "o"), PieceSize: 128, Offset: 0}

	got, err := ddoDealOnChain(nil, carries, nil, false, deal)
	require.NoError(t, err)
	require.Equal(t, ddoNoPieceSource, got)
}

// TestDDORecordedSectorSizesArePadded is the units half of the same premise,
// stated on its own so a failure says which end went wrong: the fixture rows
// carry padded sizes, and the read must take them as they are. Re-padding or
// unpadding them moves every offset behind the first piece, which the layout
// test above would then report as a missing deal rather than as a unit bug.
func TestDDORecordedSectorSizesArePadded(t *testing.T) {
	fx := packedSector2048(t)

	pieces, err := ddoPiecesFromRows(fx.sectorNum, fx.rows)
	require.NoError(t, err)
	require.Len(t, pieces, len(fx.rows))

	for i, row := range fx.rows {
		require.Equal(t, abi.PaddedPieceSize(row.PieceSize), pieces[i].Piece.Size,
			"piece_num %d keeps the padded size it was stored with", row.PieceNum)
		require.Equal(t, row.PieceCID, pieces[i].Piece.PieceCID.String())
	}
}
