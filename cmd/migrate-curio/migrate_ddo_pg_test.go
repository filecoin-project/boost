package main

import (
	"context"
	"database/sql"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-bitfield"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	"github.com/filecoin-project/curio/harmony/harmonydb"

	lapi "github.com/filecoin-project/lotus/api"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	ltypes "github.com/filecoin-project/lotus/chain/types"
)

// This file runs the migration against a real database and real Curio schema:
// a real Postgres/YugabyteDB carrying Curio's own tables, the piece rows written
// the way `curio guided-setup` writes them, boost.db read through boost's own DB
// layer, and the deals' ledgers in real SQLite. Only the chain is mocked -- the
// chain's part in this decision is two reads (StateGetClaim and
// StateSectorGetInfo), and the test sets exactly those.
//
// What that buys is the half no unit test in this package can reach: the SQL and
// the `db` struct tags in ddoPiecesFromHarmony against the table they name, and
// the caller's counting, skipping and summary as they actually run rather than as
// a test imagines them. The fixture tests next door pin the arithmetic; this one
// pins the integration the arithmetic is fed by.
//
// guided-setup itself is not called, only reproduced. Its writing code lives in
// curio's cmd/curio/guidedsetup, which imports curio's lib/types/sector, and that
// package does not compile against the go-state-types boost is on (curio v1.27.4
// wants v0.18.0, where PieceDealInfo.Valid(nv) still takes the network version;
// boost requires v0.19.0, where it does not). The two modules can be on the same
// table, but their code cannot be in the same binary, so the rows are inserted
// with the statements guided-setup uses (cmd/curio/guidedsetup/shared.go:392 and
// :456) and the migration's read of them is what this test exercises.
//
// It needs a database, so it skips unless CURIO_HARMONYDB_HOSTS points at one:
//
//	CURIO_HARMONYDB_HOSTS=127.0.0.1 CURIO_HARMONYDB_PORT=5432 \
//	CURIO_HARMONYDB_USERNAME=yugabyte CURIO_HARMONYDB_PASSWORD=yugabyte \
//	CURIO_HARMONYDB_NAME=yugabyte \
//	    go test ./cmd/migrate-curio/ -run TestDDOMigrateAfterGuidedSetup -v
//
// The role has to be able to create databases: each run gets its own database
// cloned from a template carrying Curio's full schema (harmonyquery's itest
// mode) and drops it again at the end. Point it at a throwaway instance, not at
// a provider's.

// harmonyItestDB connects to the database named by the environment, or skips.
// The instance is treated as disposable: the per-run database is created from
// Curio's schema and dropped on cleanup.
func harmonyItestDB(t *testing.T) *harmonydb.DB {
	t.Helper()

	if os.Getenv("CURIO_HARMONYDB_HOSTS") == "" {
		t.Skip("set CURIO_HARMONYDB_HOSTS to a Postgres/YugabyteDB to run this test (see the file header)")
	}

	opts := harmonydb.DefaultItestOptions()
	opts.Hosts = strings.Split(os.Getenv("CURIO_HARMONYDB_HOSTS"), ",")
	setIfSet := func(env string, dst *string) {
		if v := os.Getenv(env); v != "" {
			*dst = v
		}
	}
	setIfSet("CURIO_HARMONYDB_PORT", &opts.Port)
	setIfSet("CURIO_HARMONYDB_USERNAME", &opts.Username)
	setIfSet("CURIO_HARMONYDB_PASSWORD", &opts.Password)
	setIfSet("CURIO_HARMONYDB_NAME", &opts.Database)

	hdb, err := harmonydb.NewFromConfig(opts.HarmonyConfig())
	require.NoError(t, err, "connecting to %v:%s and applying Curio's schema", opts.Hosts, opts.Port)

	t.Cleanup(hdb.ITestDeleteAll)

	return hdb
}

// seedGuidedSetupRows writes one sector into Curio's metadata tables the way
// guided-setup's import does: the sector row in sectors_meta, then one
// sectors_meta_pieces row per piece, numbered by position, with the padded piece
// sizes the sealer left in the miner's metadata. A nil piece list is a sector
// whose pieces were never imported, which is the case the migration has to
// refuse rather than guess at.
func seedGuidedSetupRows(t *testing.T, ctx context.Context, hdb *harmonydb.DB, spID uint64, sectorNum abi.SectorNumber, pieces []lapi.SectorPiece) {
	t.Helper()

	_, err := hdb.BeginTransaction(ctx, func(tx *harmonydb.Tx) (bool, error) {
		_, err := tx.Exec(`
			INSERT INTO sectors_meta (sp_id, sector_num, reg_seal_proof, ticket_epoch, ticket_value,
									  orig_sealed_cid, orig_unsealed_cid, cur_sealed_cid, cur_unsealed_cid,
									  msg_cid_precommit, msg_cid_commit, msg_cid_update, seed_epoch, seed_value, has_sector_key)
			VALUES ($1, $2, $3, 0, ''::bytea, $4, $4, $4, $4, NULL, NULL, NULL, 0, ''::bytea, FALSE)`,
			spID, sectorNum, abi.RegisteredSealProof_StackedDrg2KiBV1_1, placeholderSealedCID)
		if err != nil {
			return false, err
		}

		for j, piece := range pieces {
			_, err = tx.Exec(`
				INSERT INTO sectors_meta_pieces (
					sp_id, sector_num, piece_num, piece_cid, piece_size,
					requested_keep_data, raw_data_size, start_epoch, orig_end_epoch,
					f05_deal_id, ddo_pam, f05_deal_proposal
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
				spID, sectorNum, j, piece.Piece.PieceCID.String(), int64(piece.Piece.Size),
				false, nil, int64(0), int64(2880*400), int64(0), nil, nil)
			if err != nil {
				return false, err
			}
		}

		return true, nil
	}, harmonydb.OptionRetry())
	require.NoError(t, err)
}

// placeholderSealedCID fills sectors_meta's sealed-cid columns. It is not a real
// CommR: nothing in the migration reads it -- the piece rows are the whole input
// -- and the columns only have to be non-null.
const placeholderSealedCID = "bagboea4b5abcbjny7vx7lq3k5x5jmk5jrmhk6vz2wbxwjqjqqqqqqqqqqqqqqqqqqqq"

// ddoPGFixture is one run's world: three sectors on chain, the rows guided-setup
// would have imported for them, and the deals boost recorded.
//
//	Sector 9   pieces: deal A, OUR PIECE, tail padding
//	           our deal's piece is in the sector where boost says it is
//	Sector 10  pieces: deal A only
//	           the sector carries data, our piece was never sealed into it
//	Sector 11  no piece rows at all
//	           the sector carries data; there is no list to answer with
type ddoPGFixture struct {
	minerAddr  address.Address
	minerID    uint64
	clientAddr address.Address

	// sectors are the on-chain facts the mocked chain reports.
	sectors map[abi.SectorNumber]*miner.SectorOnChainInfo
	// pieces are what guided-setup would have imported, per sector.
	pieces map[abi.SectorNumber][]lapi.SectorPiece
	// deals are the rows in boost.db.
	deals []*types.DirectDeal

	inSector  *types.DirectDeal // sector 9: recovered by the migration
	goneFrom  *types.DirectDeal // sector 10: refused, the piece is not there
	noRecords *types.DirectDeal // sector 11: refused, nothing was imported
}

const (
	pgDealAPieceSize = abi.PaddedPieceSize(1024)
	pgOurPieceSize   = abi.PaddedPieceSize(256)
	pgOurPieceOffset = abi.PaddedPieceSize(1024) // behind deal A, in sector 9
)

func newDDOPGFixture(t *testing.T) *ddoPGFixture {
	t.Helper()

	minerAddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)
	mid, err := address.IDFromAddress(minerAddr)
	require.NoError(t, err)
	clientAddr, err := address.NewIDAddress(2000)
	require.NoError(t, err)

	ourPiece := realCommP(t, pgOurPieceSize, "ours")
	dealA := realCommP(t, pgDealAPieceSize, "A")

	fx := &ddoPGFixture{
		minerAddr:  minerAddr,
		minerID:    mid,
		clientAddr: clientAddr,
		sectors:    map[abi.SectorNumber]*miner.SectorOnChainInfo{},
		pieces:     map[abi.SectorNumber][]lapi.SectorPiece{},
	}

	// Every sector below carries data. That is the point of the test: the weight
	// says "something landed here" for all three, and only the piece list
	// separates the one our deal is in from the two it is not.
	for _, sn := range []abi.SectorNumber{9, 10, 11} {
		fx.sectors[sn] = &miner.SectorOnChainInfo{
			SectorNumber:       sn,
			DealWeight:         big.Zero(),
			VerifiedDealWeight: big.NewInt(int64(sn) * 1000),
		}
	}

	// Sector 9's piece list as the sealer left it: deal A, our piece behind it,
	// and the padding that fills the sector out. This is what guided-setup reads
	// out of the miner's metadata and what the migration then decides on.
	fx.pieces[9] = []lapi.SectorPiece{
		sectorPiece(pgDealAPieceSize, dealA),
		sectorPiece(pgOurPieceSize, ourPiece),
		paddingPiece(abi.PaddedPieceSize(256)),
	}
	// Sector 10 ended up holding somebody else's piece: the snap aborted, our
	// piece is not there, and the sector still carries data.
	fx.pieces[10] = []lapi.SectorPiece{sectorPiece(pgDealAPieceSize, dealA)}
	// Sector 11 has nothing recorded.
	fx.pieces[11] = nil

	// boost's own record of the three deals. All three are post-nv29: no claim
	// exists for any of them, which is why the sector is asked instead.
	newDeal := func(sn abi.SectorNumber, piece cid.Cid, size abi.PaddedPieceSize, offset abi.PaddedPieceSize, alloc uint64) *types.DirectDeal {
		return &types.DirectDeal{
			ID:               uuid.New(),
			CreatedAt:        time.Now(),
			PieceCID:         piece,
			PieceSize:        size,
			Client:           fx.clientAddr,
			Provider:         minerAddr,
			AllocationID:     verifreg9types.AllocationId(alloc),
			InboundFilePath:  "/tmp/car",
			InboundFileSize:  int64(size.Unpadded()),
			SectorID:         sn,
			Offset:           offset,
			Length:           size,
			Checkpoint:       dealcheckpoints.AddedPiece,
			CheckpointAt:     time.Now(),
			StartEpoch:       100,
			EndEpoch:         2880 * 400,
			KeepUnsealedCopy: false,
			AnnounceToIPNI:   true,
		}
	}

	fx.inSector = newDeal(9, ourPiece, pgOurPieceSize, pgOurPieceOffset, 1)
	fx.goneFrom = newDeal(10, ourPiece, pgOurPieceSize, 0, 2)
	fx.noRecords = newDeal(11, ourPiece, pgOurPieceSize, 0, 3)
	fx.deals = []*types.DirectDeal{fx.inSector, fx.goneFrom, fx.noRecords}

	return fx
}

// TestDDOMigrateAfterGuidedSetup is the middle of the boost -> Curio cutover run
// end to end: a provider's imported sector metadata, boost's direct deals
// migrated against it, and the deals that were and were not recovered read back
// out of the databases.
func TestDDOMigrateAfterGuidedSetup(t *testing.T) {
	ctx := context.Background()

	hdb := harmonyItestDB(t)
	fx := newDDOPGFixture(t)

	// 1. the import `curio guided-setup` performs before this migration runs.
	for sn, pieces := range fx.pieces {
		seedGuidedSetupRows(t, ctx, hdb, fx.minerID, sn, pieces)
	}

	// The import is what the migration reads. Check it holds our piece with the
	// padded size and in piece_num order, because the offsets below are rebuilt
	// from exactly that: a size that came back unpadded, or rows read out of
	// order, moves every piece behind the first one.
	var recorded []ddoSectorPieceRow
	require.NoError(t, hdb.Select(ctx, &recorded,
		`SELECT piece_num, piece_cid, piece_size FROM sectors_meta_pieces
		   WHERE sp_id = $1 AND sector_num = 9 ORDER BY piece_num`, fx.minerID))
	require.Len(t, recorded, 3)
	require.Equal(t, int64(1), recorded[1].PieceNum)
	require.Equal(t, fx.inSector.PieceCID.String(), recorded[1].PieceCID)
	require.Equal(t, int64(pgOurPieceSize), recorded[1].PieceSize,
		"the recorded piece size is the padded one")

	// 2. boost's databases: the deals to migrate, and the migration's own ledger.
	sqldb := db.CreateTestTmpDB(t)
	require.NoError(t, db.CreateAllBoostTables(ctx, sqldb, sqldb))
	require.NoError(t, migrations.Migrate(sqldb))
	ddb := db.NewDirectDealsDB(sqldb)
	for _, deal := range fx.deals {
		require.NoError(t, ddb.Insert(ctx, deal))
	}

	mdb := db.CreateTestTmpDB(t)
	_, err := mdb.Exec(`CREATE TABLE IF NOT EXISTS Deals (
							ID TEXT UNIQUE,
							DB BOOL NOT NULL DEFAULT FALSE,
							LID BOOL NOT NULL DEFAULT FALSE,
							Pipeline BOOL NOT NULL DEFAULT FALSE
						);`)
	require.NoError(t, err)

	// 3. the chain, as far as this decision is concerned: no claim for any deal
	// (post-nv29 FIP-0118 writes none, so nil here is the normal case, not an
	// error), and sectors that carry data.
	full := lotusmocks.NewMockFullNode(gomock.NewController(t))
	full.EXPECT().StateGetClaim(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return((*verifreg9types.Claim)(nil), nil).AnyTimes()
	full.EXPECT().StateSectorGetInfo(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ address.Address, sn abi.SectorNumber, _ ltypes.TipSetKey) (*miner.SectorOnChainInfo, error) {
			si, ok := fx.sectors[sn]
			require.True(t, ok, "the migration asked about sector %d, which this fixture does not have", sn)
			return si, nil
		}).AnyTimes()

	activeSectors := bitfield.NewFromSet([]uint64{9, 10, 11})

	// 4. the migration itself, with the summary it prints captured: the count of
	// deals it could not decide is the one thing the run reports rather than
	// records, and the operator's next step hangs on it.
	summary := captureStdout(t, func() {
		require.NoError(t, migrateDDODeals(ctx, full, activeSectors, fx.minerAddr, hdb, sqldb, mdb))
	})

	// Our piece is in sector 9 where boost recorded it, so the deal is recovered.
	require.True(t, harmonyHasDeal(t, ctx, hdb, fx.inSector))
	requireLedger(t, ctx, mdb, fx.inSector, true)

	// Sector 10 carries data and does not hold our piece: the snap aborted and
	// another piece took the sector. The weight alone would have recovered this
	// deal and handed the client data that was never sealed.
	require.False(t, harmonyHasDeal(t, ctx, hdb, fx.goneFrom))
	requireLedger(t, ctx, mdb, fx.goneFrom, false)

	// Sector 11 carries data with nothing imported, so this run cannot decide:
	// nothing is migrated, and the deal is counted and reported rather than
	// guessed at or dropped in silence.
	require.False(t, harmonyHasDeal(t, ctx, hdb, fx.noRecords))
	requireLedger(t, ctx, mdb, fx.noRecords, false)
	require.Contains(t, summary, "Skipped 1 direct deal(s)",
		"the run has to tell the operator that one deal could not be decided")
	require.Contains(t, summary, "curio guided-setup",
		"and what to run before running it again")
}

// harmonyHasDeal reports whether the deal reached Curio's direct deal table,
// which is what "migrated" means for a DDO deal.
func harmonyHasDeal(t *testing.T, ctx context.Context, hdb *harmonydb.DB, deal *types.DirectDeal) bool {
	t.Helper()

	var rows []struct {
		UUID string `db:"uuid"`
	}
	require.NoError(t, hdb.Select(ctx, &rows,
		`SELECT uuid FROM market_direct_deals WHERE uuid = $1`, deal.ID.String()))

	return len(rows) == 1
}

// requireLedger checks the migration's own ledger: present with all three steps
// done for a deal that was migrated, absent for one that was not.
func requireLedger(t *testing.T, ctx context.Context, mdb *sql.DB, deal *types.DirectDeal, migrated bool) {
	t.Helper()

	var inDB, inLID, inPipeline bool
	err := mdb.QueryRowContext(ctx,
		`SELECT DB, LID, Pipeline FROM Deals WHERE ID = ?`, deal.ID.String()).
		Scan(&inDB, &inLID, &inPipeline)

	if !migrated {
		require.ErrorIs(t, err, sql.ErrNoRows,
			"a deal that was not migrated must not appear in the migration's ledger")
		return
	}

	require.NoError(t, err)
	require.True(t, inDB, "the deal was added to Curio's database")
	require.True(t, inLID, "the deal's piece metadata was added")
	require.True(t, inPipeline, "the deal's indexing step ran")
}

// captureStdout runs f with stdout redirected and returns what it printed.
func captureStdout(t *testing.T, f func()) string {
	t.Helper()

	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)

	os.Stdout = w
	printed := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		printed <- string(b)
	}()

	f()

	require.NoError(t, w.Close())
	os.Stdout = old

	return <-printed
}
