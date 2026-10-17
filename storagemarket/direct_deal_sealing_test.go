package storagemarket

import (
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	lapi "github.com/filecoin-project/lotus/api"
	minertypes "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
)

const (
	// sealingSectorNum is the sector the deal under test reports as its own;
	// otherSectorNum is a different sector a claim may point at instead.
	sealingSectorNum = abi.SectorNumber(7)
	otherSectorNum   = abi.SectorNumber(8)
)

var (
	zeroWeight = big.Zero()
	unitWeight = big.NewInt(1)

	ourPieceCID   = pieceCIDNamed("our-piece")
	otherPieceCID = pieceCIDNamed("other-piece")

	ourPieceSize = abi.PaddedPieceSize(1 << 30)
)

// pieceCIDNamed builds a distinct, well-formed CID per name so identity
// comparisons in the tests are real rather than trivially equal zero values.
func pieceCIDNamed(name string) cid.Cid {
	h, err := mh.Sum([]byte(name), mh.SHA2_256, -1)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

// sectorWithWeights builds the only on-chain facts the verdict reads. Every
// other field of SectorOnChainInfo -- Activation, PowerBaseEpoch, the QA power
// bit -- is deliberately left zero: the verdict must not depend on them, and
// TestBoundaryIsInvisible proves it.
func sectorWithWeights(dealWeight, verifiedWeight big.Int) *minertypes.SectorOnChainInfo {
	return &minertypes.SectorOnChainInfo{DealWeight: dealWeight, VerifiedDealWeight: verifiedWeight}
}

func claimOn(sn abi.SectorNumber) *verifreg9types.Claim {
	return &verifreg9types.Claim{Sector: sn}
}

func pieceEntry(pieceCID cid.Cid, size abi.PaddedPieceSize) lapi.SectorPiece {
	return lapi.SectorPiece{Piece: abi.PieceInfo{PieceCID: pieceCID, Size: size}}
}

// pieceListWith lays a piece at the given offset, preceded by a filler of
// exactly that size, so pieceInSector has to reconstruct offsets by
// accumulating Size the way lotus itself does.
func pieceListWith(pieceCID cid.Cid, size, offset abi.PaddedPieceSize) []lapi.SectorPiece {
	if offset == 0 {
		return []lapi.SectorPiece{pieceEntry(pieceCID, size)}
	}
	return []lapi.SectorPiece{
		pieceEntry(pieceCIDNamed("filler"), offset),
		pieceEntry(pieceCID, size),
	}
}

// sealingRead assembles a read in which our piece sits at offset 0, then lets
// each test perturb it.
func sealingRead(sector *minertypes.SectorOnChainInfo, claim *verifreg9types.Claim, opts ...func(*sealedSectorRead)) sealedSectorRead {
	r := sealedSectorRead{
		sector:    sector,
		claim:     claim,
		pieceCID:  ourPieceCID,
		pieceSize: ourPieceSize,
		offset:    0,
		pieces:    nil,
	}
	r.piecesKnown = sectorPiecesKnown(lapi.SectorInfo{Pieces: r.pieces})
	for _, opt := range opts {
		opt(&r)
	}
	return r
}

// withPieces sets the sealer's answer and reads it the way production does: an
// empty answer is not a list of zero pieces, it is no answer at all. Deriving
// the flag here rather than letting a test set it keeps the sweep honest -- the
// combination "list known to be readable, yet empty" is not a shape any sealer
// can produce, so no test can assert a verdict on it.
func withPieces(pieces []lapi.SectorPiece) func(*sealedSectorRead) {
	return func(r *sealedSectorRead) {
		r.pieces = pieces
		r.piecesKnown = sectorPiecesKnown(lapi.SectorInfo{Pieces: pieces})
	}
}

// verdict is the shorthand the era table reads best with: our piece at offset
// zero, a reported piece list.
func verdict(sector *minertypes.SectorOnChainInfo, claim *verifreg9types.Claim, pieces []lapi.SectorPiece) (sealingCase, directDealSealingVerdict) {
	return verdictForSealedSector(sealingRead(sector, claim, withPieces(pieces)), sealingSectorNum)
}

// TestVerdictEraMatrix walks every era x type the flow has to name. The case
// label is derived from the reads alone: a boundary CC -> snap and a post-nv29
// deal are the same on chain and must read the same, and the two pure-CC eras
// likewise.
func TestVerdictEraMatrix(t *testing.T) {
	our := pieceListWith(ourPieceCID, ourPieceSize, 0)

	tests := []struct {
		name     string
		sector   *minertypes.SectorOnChainInfo
		claim    *verifreg9types.Claim
		pieces   []lapi.SectorPiece
		wantCase sealingCase
		want     directDealSealingVerdict
	}{
		{
			name:     "nv28 pure CC, our piece staged but not landed",
			sector:   sectorWithWeights(zeroWeight, zeroWeight),
			pieces:   our,
			wantCase: caseEmptyPureCC,
			want:     sealingNoEvidence,
		},
		{
			name:     "nv29 pure CC, our piece staged but not landed",
			sector:   sectorWithWeights(zeroWeight, zeroWeight),
			pieces:   our,
			wantCase: caseEmptyPureCC,
			want:     sealingNoEvidence,
		},
		{
			name:     "nv28 plain deal: DealWeight, claim present",
			sector:   sectorWithWeights(unitWeight, zeroWeight),
			claim:    claimOn(sealingSectorNum),
			pieces:   our,
			wantCase: caseNv28SealedSector,
			want:     sealingDone,
		},
		{
			name:     "nv28 plain deal: claim present, piece list not needed",
			sector:   sectorWithWeights(unitWeight, zeroWeight),
			claim:    claimOn(sealingSectorNum),
			wantCase: caseNv28SealedSector,
			want:     sealingDone,
		},
		{
			// An unverified deal has no verifreg claim in any era, so this is
			// the plain-deal row as it actually occurs. It reads the same as
			// the nv29 plain deal below: the case label is a guess at the era,
			// and the verdict -- which is what the flow acts on -- is not.
			name:     "nv28 plain deal: DealWeight and no claim",
			sector:   sectorWithWeights(unitWeight, zeroWeight),
			pieces:   our,
			wantCase: casePostSealedSector,
			want:     sealingDone,
		},
		{
			name:     "nv28 DDO: VerifiedDealWeight, claim present",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			claim:    claimOn(sealingSectorNum),
			pieces:   our,
			wantCase: caseNv28SealedSector,
			want:     sealingDone,
		},
		{
			name:     "nv28 CC -> snap: verified weight, claim present",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			claim:    claimOn(sealingSectorNum),
			pieces:   our,
			wantCase: caseNv28SealedSector,
			want:     sealingDone,
		},
		{
			// Pre-nv29 the chain's claim is the authority, so it settles the
			// deal on its own -- an empty piece list cannot hold it back. This
			// is the one branch where an absent piece is still `done`.
			name:     "nv28 CC -> snap: claim present, piece list empty",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			claim:    claimOn(sealingSectorNum),
			wantCase: caseNv28SealedSector,
			want:     sealingDone,
		},
		{
			name:     "boundary CC -> snap: verified weight, no claim",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			pieces:   our,
			wantCase: casePostSealedSector,
			want:     sealingDone,
		},
		{
			name:     "nv29 plain deal: verified weight, no claim",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			pieces:   our,
			wantCase: casePostSealedSector,
			want:     sealingDone,
		},
		{
			name:     "nv29 DDO: verified weight, no claim",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			pieces:   our,
			wantCase: casePostSealedSector,
			want:     sealingDone,
		},
		{
			name:     "nv29 CC -> snap: verified weight, no claim",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			pieces:   our,
			wantCase: casePostSealedSector,
			want:     sealingDone,
		},
		{
			// Weight, no claim and no list: the sealer has told us nothing
			// about whose piece the sector carries, and the claim that would
			// have settled a pre-nv29 deal does not exist past the upgrade.
			// There is no verdict to give.
			name:     "nv29 deal on a sealer that does not report the piece list",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			pieces:   nil,
			wantCase: caseIndeterminate,
			want:     sealingNoEvidence,
		},
		{
			name:     "USQ: both weight arms on one sector, claim present",
			sector:   sectorWithWeights(unitWeight, unitWeight),
			claim:    claimOn(sealingSectorNum),
			pieces:   our,
			wantCase: caseNv28SealedSector,
			want:     sealingDone,
		},
		{
			name:     "rollback: settled, no weight, no claim, piece absent",
			sector:   sectorWithWeights(zeroWeight, zeroWeight),
			pieces:   pieceListWith(otherPieceCID, ourPieceSize, 0),
			wantCase: caseRolledBack,
			want:     sealingRolledBack,
		},
		{
			name:     "rollback is never inferred from an unreported piece list",
			sector:   sectorWithWeights(zeroWeight, zeroWeight),
			pieces:   nil,
			wantCase: caseIndeterminate,
			want:     sealingNoEvidence,
		},
		{
			name:     "claim hits a zero-weight sector",
			sector:   sectorWithWeights(zeroWeight, zeroWeight),
			claim:    claimOn(sealingSectorNum),
			wantCase: caseNv28SealedSector,
			want:     sealingDone,
		},
		{
			name:     "claim points at another sector",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			claim:    claimOn(otherSectorNum),
			pieces:   our,
			wantCase: caseClaimElsewhere,
			want:     sealingClaimElsewhere,
		},
		{
			name:     "sector not visible on this node",
			wantCase: caseIndeterminate,
			want:     sealingNoEvidence,
		},
		{
			name:     "weight non-zero but our piece not in the list, no claim",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			pieces:   pieceListWith(otherPieceCID, ourPieceSize, 0),
			wantCase: casePieceMissing,
			want:     sealingPieceMissing,
		},
		{
			name:     "weight non-zero, our piece at an offset we did not add it at",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			pieces:   pieceListWith(ourPieceCID, ourPieceSize, ourPieceSize),
			wantCase: casePieceMissing,
			want:     sealingPieceMissing,
		},
		{
			// The nv28 aborted snap: the sector holds another deal's
			// unverified spacetime, and the list says ours is not in it.
			name:     "nv28 aborted snap: deal weight, no claim, our piece not in the list",
			sector:   sectorWithWeights(unitWeight, zeroWeight),
			pieces:   pieceListWith(otherPieceCID, ourPieceSize, 0),
			wantCase: casePieceMissing,
			want:     sealingPieceMissing,
		},
		{
			// Pre-nv29 the claim is the authority: it hits this sector, so the
			// deal is done however the piece list reads.
			name:     "nv28: claim hits this sector, our piece not in the list",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			claim:    claimOn(sealingSectorNum),
			pieces:   pieceListWith(otherPieceCID, ourPieceSize, 0),
			wantCase: caseNv28SealedSector,
			want:     sealingDone,
		},
		{
			// nv29, fresh seal across the upgrade: pre-committed before nv29,
			// proved after it, so the sector activates at nv29 and FIP-0118
			// writes no claim. Reachable, and the weight arm has to settle it.
			name:     "nv29 fresh seal across the upgrade: verified weight, no claim",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			pieces:   our,
			wantCase: casePostSealedSector,
			want:     sealingDone,
		},
		{
			name:     "nv29 fresh seal across the upgrade: our piece not in the list",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			pieces:   pieceListWith(otherPieceCID, ourPieceSize, 0),
			wantCase: casePieceMissing,
			want:     sealingPieceMissing,
		},
		{
			// The same sector on a sealer that answers with no list at all:
			// the weight cannot be attributed to this deal, and no claim exists
			// past the upgrade to attribute it either. There is no verdict.
			name:     "nv29 fresh seal across the upgrade on a sealer that reports no list",
			sector:   sectorWithWeights(zeroWeight, unitWeight),
			pieces:   nil,
			wantCase: caseIndeterminate,
			want:     sealingNoEvidence,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotCase, got := verdictForSealedSector(sealingRead(tt.sector, tt.claim, withPieces(tt.pieces)), sealingSectorNum)
			if gotCase != tt.wantCase || got != tt.want {
				t.Fatalf("verdictForSealedSector() = (%v, %v), want (%v, %v)",
					gotCase, got, tt.wantCase, tt.want)
			}
		})
	}
}

type claimKind int

const (
	claimNone claimKind = iota
	claimHere
	claimElsewhere
)

// TestVerdictExhaustive sweeps the whole input space the verdict can see --
// whether the sector carries weight, what the claim says, whether our piece is
// in the list, and whether the list is readable at all -- against one literal
// statement of the rule. Fixtures are classified here directly (carries, in
// sector) rather than through the code under test, so the sweep cannot agree
// with a bug by construction.
func TestVerdictExhaustive(t *testing.T) {
	type shape struct {
		carries     bool
		claim       claimKind
		inSector    bool
		piecesKnown bool
	}

	type outcome struct {
		cse     sealingCase
		verdict directDealSealingVerdict
	}

	// The rule, stated once. claimElsewhere is not a key: it wins over every
	// other input, and TestClaimElsewhereWins covers it as a sweep of its own.
	want := map[shape]outcome{
		// No weight, no claim, readable list: the only shape where the piece
		// list is load-bearing -- staged means "not landed yet", absent means
		// "the snap fell back".
		{carries: false, claim: claimNone, inSector: true, piecesKnown: true}:   {caseEmptyPureCC, sealingNoEvidence},
		{carries: false, claim: claimNone, inSector: false, piecesKnown: true}:  {caseRolledBack, sealingRolledBack},
		{carries: false, claim: claimNone, inSector: false, piecesKnown: false}: {caseIndeterminate, sealingNoEvidence},

		// A claim on this sector is the pre-nv29 authority: decisive on its own.
		{carries: false, claim: claimHere, inSector: true, piecesKnown: true}:   {caseNv28SealedSector, sealingDone},
		{carries: false, claim: claimHere, inSector: false, piecesKnown: true}:  {caseNv28SealedSector, sealingDone},
		{carries: false, claim: claimHere, inSector: false, piecesKnown: false}: {caseNv28SealedSector, sealingDone},

		// Weight with no claim: post-nv29, the boundary and a fresh seal across
		// it. Weight is a fact about the sector, not about this deal, so it
		// completes one only where our piece is in a list the sealer reports; a
		// readable list that does not hold our piece is the aborted snap and the
		// sector's data is somebody else's; with no list at all nothing says
		// whose weight it is, and the deal keeps polling.
		{carries: true, claim: claimNone, inSector: true, piecesKnown: true}:   {casePostSealedSector, sealingDone},
		{carries: true, claim: claimNone, inSector: false, piecesKnown: false}: {caseIndeterminate, sealingNoEvidence},
		{carries: true, claim: claimNone, inSector: false, piecesKnown: true}:  {casePieceMissing, sealingPieceMissing},

		{carries: true, claim: claimHere, inSector: true, piecesKnown: true}:   {caseNv28SealedSector, sealingDone},
		{carries: true, claim: claimHere, inSector: false, piecesKnown: true}:  {caseNv28SealedSector, sealingDone},
		{carries: true, claim: claimHere, inSector: false, piecesKnown: false}: {caseNv28SealedSector, sealingDone},
	}

	// Fixture vocabularies. Every combination below is reachable.
	sectors := map[string]struct {
		sector  *minertypes.SectorOnChainInfo
		carries bool
	}{
		"no weight":       {sectorWithWeights(zeroWeight, zeroWeight), false},
		"deal weight":     {sectorWithWeights(unitWeight, zeroWeight), true},
		"verified weight": {sectorWithWeights(zeroWeight, unitWeight), true},
		"both weights":    {sectorWithWeights(unitWeight, unitWeight), true},
	}

	claims := map[string]struct {
		claim *verifreg9types.Claim
		kind  claimKind
	}{
		"no claim":        {nil, claimNone},
		"claim here":      {claimOn(sealingSectorNum), claimHere},
		"claim elsewhere": {claimOn(otherSectorNum), claimElsewhere},
	}

	// "list empty" is deliberately absent: a sealed sector's list is never
	// empty, so an empty answer is the sealer not reporting one, and there is
	// no shape in which a list is readable and holds nothing. The flag is
	// derived from the answer, the way production derives it.
	lists := map[string]struct {
		pieces    []lapi.SectorPiece
		known     bool
		holdsOurs bool
	}{
		"list unreported": {nil, false, false},
		"our piece":       {pieceListWith(ourPieceCID, ourPieceSize, 0), true, true},
		"another piece":   {pieceListWith(otherPieceCID, ourPieceSize, 0), true, false},
	}

	visited := map[shape]bool{}

	for sectorName, sectorFixture := range sectors {
		for claimName, claimFixture := range claims {
			for listName, listFixture := range lists {
				name := fmt.Sprintf("%s/%s/%s", sectorName, claimName, listName)
				t.Run(name, func(t *testing.T) {
					r := sealingRead(sectorFixture.sector, claimFixture.claim, withPieces(listFixture.pieces))

					gotCase, got := verdictForSealedSector(r, sealingSectorNum)

					// A claim pointing elsewhere is terminal whatever the rest
					// of the read says.
					if claimFixture.kind == claimElsewhere {
						if gotCase != caseClaimElsewhere || got != sealingClaimElsewhere {
							t.Fatalf("claim elsewhere = (%v, %v), want (claim-elsewhere, sealingClaimElsewhere)", gotCase, got)
						}
						return
					}

					sh := shape{
						carries:     sectorFixture.carries,
						claim:       claimFixture.kind,
						inSector:    listFixture.holdsOurs,
						piecesKnown: listFixture.known,
					}
					exp, ok := want[sh]
					if !ok {
						t.Fatalf("no expectation stated for read shape %+v", sh)
					}
					visited[sh] = true

					if gotCase != exp.cse || got != exp.verdict {
						t.Fatalf("verdictForSealedSector() = (%v, %v), want (%v, %v) for %+v",
							gotCase, got, exp.cse, exp.verdict, sh)
					}
				})
			}
		}
	}

	// Every stated rule must have been exercised, so a rule that silently
	// stopped being reachable fails here rather than passing unnoticed.
	for sh, exp := range want {
		if !visited[sh] {
			t.Errorf("read shape %+v (want %v/%v) was never exercised by the sweep", sh, exp.cse, exp.verdict)
		}
	}
}

// TestClaimElsewhereWins pins the precedence: a claim that points at another
// sector settles the deal even when this node cannot see the reported sector at
// all, which is the shape that would otherwise poll forever.
func TestClaimElsewhereWins(t *testing.T) {
	our := pieceListWith(ourPieceCID, ourPieceSize, 0)

	reads := map[string]sealedSectorRead{
		"no weight, piece staged":                  sealingRead(sectorWithWeights(zeroWeight, zeroWeight), claimOn(otherSectorNum), withPieces(our)),
		"no weight, piece absent":                  sealingRead(sectorWithWeights(zeroWeight, zeroWeight), claimOn(otherSectorNum)),
		"verified weight, piece staged":            sealingRead(sectorWithWeights(zeroWeight, unitWeight), claimOn(otherSectorNum), withPieces(our)),
		"sector not visible on this node":          sealingRead(nil, claimOn(otherSectorNum)),
		"sector not visible, list also unreported": sealingRead(nil, claimOn(otherSectorNum), withPieces(nil)),
	}

	for name, r := range reads {
		t.Run(name, func(t *testing.T) {
			gotCase, got := verdictForSealedSector(r, sealingSectorNum)
			if gotCase != caseClaimElsewhere || got != sealingClaimElsewhere {
				t.Fatalf("verdictForSealedSector() = (%v, %v), want (claim-elsewhere, sealingClaimElsewhere)", gotCase, got)
			}
		})
	}
}

// TestNodeLagIsNotAVerdict keeps the sector-visibility guard honest: a node that
// is behind sees nothing, in either era, and must never turn that into a
// failure. A claim on the reported sector is still just lag -- the sector has
// not appeared yet.
func TestNodeLagIsNotAVerdict(t *testing.T) {
	our := pieceListWith(ourPieceCID, ourPieceSize, 0)

	tests := map[string]sealedSectorRead{
		"nothing visible":                           sealingRead(nil, nil),
		"claim on our sector, sector not visible":   sealingRead(nil, claimOn(sealingSectorNum)),
		"piece list readable, sector not visible":   sealingRead(nil, nil, withPieces(our)),
		"piece list unreported, sector not visible": sealingRead(nil, nil, withPieces(nil)),
	}

	for name, r := range tests {
		t.Run(name, func(t *testing.T) {
			gotCase, got := verdictForSealedSector(r, sealingSectorNum)
			if gotCase != caseIndeterminate || got != sealingNoEvidence {
				t.Fatalf("lagging node = (%v, %v), want (indeterminate, sealingNoEvidence)", gotCase, got)
			}
		})
	}
}

// TestBoundaryIsInvisible verifies the claim the whole design rests on: a CC
// sector activated before nv29 and snapped after it is indistinguishable, by
// this verdict, from a sector committed after nv29. The fields that would tell
// them apart -- Activation and PowerBaseEpoch -- are present and differ, and the
// verdict must not move.
func TestBoundaryIsInvisible(t *testing.T) {
	const (
		preNv29  = abi.ChainEpoch(100)
		postNv29 = abi.ChainEpoch(200)
	)

	boundary := &minertypes.SectorOnChainInfo{
		Activation:         preNv29,
		PowerBaseEpoch:     postNv29,
		VerifiedDealWeight: unitWeight,
	}
	fresh := &minertypes.SectorOnChainInfo{
		Activation:         postNv29,
		PowerBaseEpoch:     postNv29,
		VerifiedDealWeight: unitWeight,
	}

	if boundary.Activation == fresh.Activation {
		t.Fatal("fixtures must differ in the dating fields, or the test is vacuous")
	}

	our := pieceListWith(ourPieceCID, ourPieceSize, 0)
	boundaryCase, boundaryVerdict := verdict(boundary, nil, our)
	freshCase, freshVerdict := verdict(fresh, nil, our)

	if boundaryCase != freshCase || boundaryVerdict != freshVerdict {
		t.Fatalf("boundary read as (%v, %v) but post-nv29 read as (%v, %v); the verdict must not date the sector",
			boundaryCase, boundaryVerdict, freshCase, freshVerdict)
	}
	if boundaryCase != casePostSealedSector || boundaryVerdict != sealingDone {
		t.Fatalf("verified weight with no claim = (%v, %v), want (post-nv29-sealed, sealingDone)",
			boundaryCase, boundaryVerdict)
	}

	// The same sector before the data lands: a pure CC in either era, where the
	// piece list is the only thing that can tell a staged piece from a rolled
	// back snap. A rollback is a list that holds somebody else's piece -- an
	// empty answer is not a list of zero pieces, so it says nothing either way.
	staged := &minertypes.SectorOnChainInfo{Activation: preNv29, PowerBaseEpoch: postNv29}
	stageCase, stageVerdict := verdict(staged, nil, our)
	rollCase, rollVerdict := verdict(staged, nil, pieceListWith(otherPieceCID, ourPieceSize, 0))

	if stageCase != caseEmptyPureCC || stageVerdict != sealingNoEvidence {
		t.Fatalf("staged piece = (%v, %v), want (empty-pure-cc, sealingNoEvidence)", stageCase, stageVerdict)
	}
	if rollCase != caseRolledBack || rollVerdict != sealingRolledBack {
		t.Fatalf("absent piece = (%v, %v), want (rolled-back, sealingRolledBack)", rollCase, rollVerdict)
	}
}

// TestFreshSealAcrossNv29 pins the other crossing a direct deal can make: the
// pre-commit was submitted before nv29 and the prove-commit landed after it.
//
// It is not a *boundary* in the activation/power-base sense -- the actor writes
// one epoch for both at prove-commit, so both are post-nv29 -- but it is
// perfectly reachable, and the deal that comes out of it is a deal that has to
// finish: the piece is in the sector, verifreg has been frozen, and FIP-0118
// wrote no claim. Watching such a deal for a claim it can never get is watching
// it forever, so the weight arm has to be enough on its own.
//
// The two crossings are listed here side by side because their on-chain reads
// are the same shape: data with no claim. Nothing in the verdict may depend on
// which of them produced it -- the sealer knows how the data got there, the
// chain does not.
func TestFreshSealAcrossNv29(t *testing.T) {
	const (
		preNv29  = abi.ChainEpoch(100)
		postNv29 = abi.ChainEpoch(200)
	)

	// The sector a straddling fresh seal produces: one epoch for both fields,
	// because prove-commit writes them together.
	freshSeal := &minertypes.SectorOnChainInfo{
		Activation:         postNv29,
		PowerBaseEpoch:     postNv29,
		VerifiedDealWeight: unitWeight,
	}
	// A snap across the same upgrade, for the equivalence: activation left on
	// the pre-nv29 side, power base moved by the update.
	snap := &minertypes.SectorOnChainInfo{
		Activation:         preNv29,
		PowerBaseEpoch:     postNv29,
		VerifiedDealWeight: unitWeight,
	}
	// A fresh seal that did not finish: the piece was handed to the sealer
	// before the upgrade and has not landed. This is the shape that must keep
	// polling rather than be mistaken for a landed deal or a rollback.
	staged := &minertypes.SectorOnChainInfo{Activation: preNv29, PowerBaseEpoch: preNv29}

	our := pieceListWith(ourPieceCID, ourPieceSize, 0)
	someoneElses := pieceListWith(otherPieceCID, ourPieceSize, 0)

	tests := []struct {
		name     string
		sector   *minertypes.SectorOnChainInfo
		pieces   []lapi.SectorPiece
		wantCase sealingCase
		want     directDealSealingVerdict
	}{
		{"fresh seal landed, no claim", freshSeal, our, casePostSealedSector, sealingDone},
		{"snap landed, no claim", snap, our, casePostSealedSector, sealingDone},
		{"fresh seal landed, another piece took the sector", freshSeal, someoneElses, casePieceMissing, sealingPieceMissing},
		{"fresh seal still staging", staged, our, caseEmptyPureCC, sealingNoEvidence},
		{"fresh seal rolled back to a CC", staged, someoneElses, caseRolledBack, sealingRolledBack},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// No claim in any row: this is the whole premise of the crossing.
			gotCase, got := verdict(tt.sector, nil, tt.pieces)
			if gotCase != tt.wantCase || got != tt.want {
				t.Fatalf("verdictForSealedSector() = (%v, %v), want (%v, %v)", gotCase, got, tt.wantCase, tt.want)
			}
		})
	}
}

// TestPieceInSector covers the identity check: one sector can hold two
// identical pieces, so all of (PieceCID, Size, offset) must match, and the
// offset is reconstructed from the order and sizes of the preceding entries.
func TestPieceInSector(t *testing.T) {
	twoCopiesAndACousin := []lapi.SectorPiece{
		pieceEntry(ourPieceCID, ourPieceSize), // offset 0
		pieceEntry(otherPieceCID, ourPieceSize),
		pieceEntry(ourPieceCID, ourPieceSize), // offset 2*ourPieceSize
	}

	tests := []struct {
		name   string
		pieces []lapi.SectorPiece
		piece  cid.Cid
		size   abi.PaddedPieceSize
		offset abi.PaddedPieceSize
		want   bool
	}{
		{
			name:   "single piece at the start",
			pieces: []lapi.SectorPiece{pieceEntry(ourPieceCID, ourPieceSize)},
			piece:  ourPieceCID, size: ourPieceSize, offset: 0,
			want: true,
		},
		{
			name:   "our piece after a filler",
			pieces: pieceListWith(ourPieceCID, ourPieceSize, ourPieceSize*3),
			piece:  ourPieceCID, size: ourPieceSize, offset: ourPieceSize * 3,
			want: true,
		},
		{
			name: "our piece in the middle",
			pieces: []lapi.SectorPiece{
				pieceEntry(pieceCIDNamed("left"), ourPieceSize),
				pieceEntry(ourPieceCID, ourPieceSize),
				pieceEntry(pieceCIDNamed("right"), ourPieceSize*2),
			},
			piece: ourPieceCID, size: ourPieceSize, offset: ourPieceSize,
			want: true,
		},
		{
			name:   "identical pieces told apart by offset",
			pieces: twoCopiesAndACousin,
			piece:  ourPieceCID, size: ourPieceSize, offset: ourPieceSize * 2,
			want: true,
		},
		{
			name:   "same piece, the other copy's offset",
			pieces: twoCopiesAndACousin,
			piece:  ourPieceCID, size: ourPieceSize, offset: ourPieceSize,
			want: false,
		},
		{
			name:   "same piece, past the end of the list",
			pieces: twoCopiesAndACousin,
			piece:  ourPieceCID, size: ourPieceSize, offset: ourPieceSize * 3,
			want: false,
		},
		{
			name:   "same piece, wrong size",
			pieces: pieceListWith(ourPieceCID, ourPieceSize, 0),
			piece:  ourPieceCID, size: ourPieceSize * 2, offset: 0,
			want: false,
		},
		{
			name:   "different piece at the right offset and size",
			pieces: pieceListWith(otherPieceCID, ourPieceSize, 0),
			piece:  ourPieceCID, size: ourPieceSize, offset: 0,
			want: false,
		},
		{
			name:   "empty list",
			pieces: nil,
			piece:  ourPieceCID, size: ourPieceSize, offset: 0,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pieceInSector(tt.pieces, tt.piece, tt.size, tt.offset); got != tt.want {
				t.Fatalf("pieceInSector() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSealingCaseString(t *testing.T) {
	tests := []struct {
		cse  sealingCase
		want string
	}{
		{caseIndeterminate, "indeterminate"},
		{caseNv28SealedSector, "nv28-sealed"},
		{casePostSealedSector, "post-nv29-sealed"},
		{caseEmptyPureCC, "empty-pure-cc"},
		{caseClaimElsewhere, "claim-elsewhere"},
		{caseRolledBack, "rolled-back"},
		{sealingCase(99), "indeterminate"},
	}

	for _, tt := range tests {
		if got := tt.cse.String(); got != tt.want {
			t.Errorf("sealingCase(%d).String() = %q, want %q", int(tt.cse), got, tt.want)
		}
	}
}

func TestSectorCarriesData(t *testing.T) {
	tests := []struct {
		name string
		si   *minertypes.SectorOnChainInfo
		want bool
	}{
		{name: "nil sector", si: nil, want: false},
		{name: "pure CC", si: sectorWithWeights(zeroWeight, zeroWeight), want: false},
		{name: "plain deal weight only", si: sectorWithWeights(unitWeight, zeroWeight), want: true},
		{name: "verified deal weight only", si: sectorWithWeights(zeroWeight, unitWeight), want: true},
		{name: "both weights", si: sectorWithWeights(unitWeight, unitWeight), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sectorCarriesData(tt.si); got != tt.want {
				t.Fatalf("sectorCarriesData() = %v, want %v", got, tt.want)
			}
		})
	}
}
