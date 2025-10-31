//go:build test_lid
// +build test_lid

package piecedirectory

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/ldb"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/svc"
	"github.com/filecoin-project/boost/extern/boostd-data/yugabyte"
	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/stretchr/testify/require"
)

func TestPieceDoctor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	t.Run("leveldb", func(t *testing.T) {
		prev := ldb.MinPieceCheckPeriod
		ldb.MinPieceCheckPeriod = 1 * time.Second

		bdsvc, err := svc.NewLevelDB("")
		require.NoError(t, err)

		ln, err := bdsvc.Start(ctx, "localhost:0")
		require.NoError(t, err)

		cl := client.NewStore()
		err = cl.Dial(ctx, fmt.Sprintf("ws://%s", ln))
		require.NoError(t, err)
		defer cl.Close(ctx)

		t.Run("next pieces pagination", func(t *testing.T) {
			prevp := ldb.PiecesToTrackerBatchSize
			testNextPiecesPagination(ctx, t, cl, func(pageSize int) {
				ldb.PiecesToTrackerBatchSize = pageSize
			})
			ldb.PiecesToTrackerBatchSize = prevp
		})

		t.Run("check pieces", func(t *testing.T) {
			testCheckPieces(ctx, t, cl)
		})

		t.Run("pieces count", func(t *testing.T) {
			testPiecesCount(ctx, t, cl)
		})

		ldb.MinPieceCheckPeriod = prev
	})

	t.Run("yugabyte", func(t *testing.T) {
		prev := yugabyte.MinPieceCheckPeriod
		yugabyte.MinPieceCheckPeriod = 1 * time.Second

		bdsvc := svc.SetupYugabyte(t)
		ln, err := bdsvc.Start(ctx, "localhost:0")
		require.NoError(t, err)

		cl := client.NewStore()
		err = cl.Dial(ctx, fmt.Sprintf("ws://%s", ln))
		require.NoError(t, err)
		defer cl.Close(ctx)

		ybstore := bdsvc.Impl.(*yugabyte.Store)

		t.Run("next pieces", func(t *testing.T) {
			svc.RecreateTables(ctx, t, ybstore)
			testNextPieces(ctx, t, cl, yugabyte.MinPieceCheckPeriod)
		})

		t.Run("next pieces pagination", func(t *testing.T) {
			svc.RecreateTables(ctx, t, ybstore)
			prevp := yugabyte.TrackerCheckBatchSize
			testNextPiecesPagination(ctx, t, cl, func(pageSize int) {
				yugabyte.TrackerCheckBatchSize = pageSize
			})
			yugabyte.TrackerCheckBatchSize = prevp
		})

		t.Run("check pieces", func(t *testing.T) {
			svc.RecreateTables(ctx, t, ybstore)
			testCheckPieces(ctx, t, cl)
		})

		t.Run("pieces count", func(t *testing.T) {
			svc.RecreateTables(ctx, t, ybstore)
			testPiecesCount(ctx, t, cl)
		})

		yugabyte.MinPieceCheckPeriod = prev
	})
}

// Verify that after a new piece is added
// - NextPiecesToCheck immediately returns the piece
// - NextPiecesToCheck returns the piece every pieceCheckPeriod
func testNextPieces(ctx context.Context, t *testing.T, cl *client.Store, pieceCheckPeriod time.Duration) {
	// Add a new piece
	pieceCid := blocks.NewBlock([]byte(fmt.Sprintf("%d", time.Now().UnixMilli()))).Cid()
	minerAddr := address.TestAddress
	di := model.DealInfo{
		DealUuid:    uuid.New().String(),
		ChainDealID: 1,
		MinerAddr:   minerAddr,
		SectorID:    1,
		PieceOffset: 0,
		PieceLength: 2048,
	}
	err := cl.AddDealForPiece(ctx, pieceCid, di)
	require.NoError(t, err)

	// Add another piece with a different miner address - it should be ignored
	di2 := model.DealInfo{
		DealUuid:    uuid.New().String(),
		ChainDealID: 2,
		MinerAddr:   address.TestAddress2,
		SectorID:    2,
		PieceOffset: 0,
		PieceLength: 2048,
	}
	err = cl.AddDealForPiece(ctx, pieceCid, di2)
	require.NoError(t, err)

	// Sleep for half the piece check period
	time.Sleep(pieceCheckPeriod / 2)

	// NextPiecesToCheck should return the piece (because it hasn't been checked yet)
	pcids, err := cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.Contains(t, pcids, pieceCid)

	// Calling NextPiecesToCheck again should return nothing, because the piece
	// was just checked
	pcids, err = cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.NotContains(t, pcids, pieceCid)

	// Sleep for at least the piece check period
	time.Sleep(2 * pieceCheckPeriod)

	// Calling NextPiecesToCheck should return the piece, because it has not
	// been checked for at least one piece check period
	pcids, err = cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.Contains(t, pcids, pieceCid)
}

func testNextPiecesPagination(ctx context.Context, t *testing.T, cl *client.Store, setPageSize func(int)) {
	setPageSize(4)

	// Add 9 pieces
	seen := make(map[cid.Cid]int)

	minerAddr := address.TestAddress
	for i := 1; i <= 9; i++ {
		pieceCid := testutil.GenerateCid()
		di := model.DealInfo{
			DealUuid:    uuid.New().String(),
			ChainDealID: abi.DealID(i),
			MinerAddr:   minerAddr,
			SectorID:    abi.SectorNumber(i),
			PieceOffset: 0,
			PieceLength: 2048,
		}
		err := cl.AddDealForPiece(ctx, pieceCid, di)
		require.NoError(t, err)
	}

	// expect to get 4 pieces
	pcids, err := cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.Len(t, pcids, 4)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	// expect to get 4 pieces
	pcids, err = cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.Len(t, pcids, 4)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	// expect to get 1 pieces (end of table)
	pcids, err = cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.Len(t, pcids, 1)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	require.Len(t, seen, 9)

	// expect to get 0 pieces (first four)
	pcids, err = cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.Len(t, pcids, 0)

	// expect to get 0 pieces (second four)
	pcids, err = cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.Len(t, pcids, 0)

	// expect to get 0 pieces (end of table)
	pcids, err = cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.Len(t, pcids, 0)

	// Add 1 more piece
	for i := 1; i <= 1; i++ {
		pieceCid := testutil.GenerateCid()
		di := model.DealInfo{
			DealUuid:    uuid.New().String(),
			ChainDealID: abi.DealID(100),
			MinerAddr:   minerAddr,
			SectorID:    abi.SectorNumber(100),
			PieceOffset: 0,
			PieceLength: 2048,
		}
		err := cl.AddDealForPiece(ctx, pieceCid, di)
		require.NoError(t, err)
	}

	// wait to reset the interval and start from scratch
	time.Sleep(2 * time.Second)
	seen = make(map[cid.Cid]int)

	pcids, err = cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.Len(t, pcids, 4)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	pcids, err = cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.Len(t, pcids, 4)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	pcids, err = cl.NextPiecesToCheck(ctx, minerAddr)
	require.NoError(t, err)
	require.Len(t, pcids, 2)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	require.Len(t, seen, 10)
}

func testCheckPieces(ctx context.Context, t *testing.T, cl *client.Store) {
	// Create a random CAR file
	_, carFilePath := CreateCarFile(t)
	carFile, err := os.Open(carFilePath)
	require.NoError(t, err)
	defer carFile.Close()

	carReader, err := car.OpenReader(carFilePath)
	require.NoError(t, err)
	defer carReader.Close()
	carv1Reader, err := carReader.DataReader()
	require.NoError(t, err)

	commpCalc := CalculateCommp(t, carv1Reader)

	// Add deal info for the piece
	minerActorID := abi.ActorID(1011)
	minerAddr, err := address.NewIDAddress(uint64(minerActorID))
	require.NoError(t, err)
	di := model.DealInfo{
		DealUuid:    uuid.New().String(),
		ChainDealID: 1,
		MinerAddr:   minerAddr,
		SectorID:    1,
		PieceOffset: 0,
		PieceLength: commpCalc.PieceSize,
	}
	dlSectorID := abi.SectorID{
		Miner:  minerActorID,
		Number: di.SectorID,
	}
	err = cl.AddDealForPiece(ctx, commpCalc.PieceCID, di)
	require.NoError(t, err)

	// Initialize the sector state such that the deal's sector is active and
	// there is an unsealed copy
	ssu := &sectorstatemgr.SectorStateUpdates{
		ActiveSectors: map[abi.SectorID]struct{}{
			dlSectorID: {},
		},
		SectorStates: map[abi.SectorID]db.SealState{
			dlSectorID: db.SealStateUnsealed,
		},
	}

	// Create a doctor
	doc := NewDoctor(minerAddr, cl, nil, nil)

	// Check the piece
	err = doc.checkPiece(ctx, commpCalc.PieceCID, ssu, nil)
	require.NoError(t, err)

	// The piece should be flagged because there is no index for it
	count, err := cl.FlaggedPiecesCount(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	pcids, err := cl.FlaggedPiecesList(ctx, nil, nil, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(pcids))

	// Create an index for the piece
	recs := GetRecords(t, carv1Reader)
	err = cl.AddIndex(ctx, commpCalc.PieceCID, recs, true)
	require.NoError(t, err)

	// Check the piece
	err = doc.checkPiece(ctx, commpCalc.PieceCID, ssu, nil)
	require.NoError(t, err)

	// The piece should no longer be flagged
	count, err = cl.FlaggedPiecesCount(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	pcids, err = cl.FlaggedPiecesList(ctx, nil, nil, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 0, len(pcids))

	// Simulate deleting the unsealed copy
	ssu.SectorStates[dlSectorID] = db.SealStateSealed

	// Check the piece
	err = doc.checkPiece(ctx, commpCalc.PieceCID, ssu, nil)
	require.NoError(t, err)

	// The piece should be flagged because there is no unsealed copy
	count, err = cl.FlaggedPiecesCount(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	pcids, err = cl.FlaggedPiecesList(ctx, nil, nil, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(pcids))

	// Simulate a sector unseal
	ssu.SectorStates[dlSectorID] = db.SealStateUnsealed

	// Check the piece
	err = doc.checkPiece(ctx, commpCalc.PieceCID, ssu, nil)
	require.NoError(t, err)

	// The piece should no longer be flagged
	count, err = cl.FlaggedPiecesCount(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	pcids, err = cl.FlaggedPiecesList(ctx, nil, nil, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 0, len(pcids))
}

func testPiecesCount(ctx context.Context, t *testing.T, cl *client.Store) {
	// Create a random CAR file
	_, carFilePath := CreateCarFile(t)
	carFile, err := os.Open(carFilePath)
	require.NoError(t, err)
	defer carFile.Close()

	carReader, err := car.OpenReader(carFilePath)
	require.NoError(t, err)
	defer carReader.Close()
	carv1Reader, err := carReader.DataReader()
	require.NoError(t, err)

	commpCalc := CalculateCommp(t, carv1Reader)

	// Add deal info for the piece on miner 1001
	minerAddr, err := address.NewIDAddress(1001)
	require.NoError(t, err)
	di := model.DealInfo{
		DealUuid:    uuid.New().String(),
		ChainDealID: 1,
		MinerAddr:   minerAddr,
		SectorID:    1,
		PieceOffset: 0,
		PieceLength: commpCalc.PieceSize,
	}
	err = cl.AddDealForPiece(ctx, commpCalc.PieceCID, di)
	require.NoError(t, err)

	// Add deal info for the piece on miner 1002
	minerAddr2, err := address.NewIDAddress(1002)
	require.NoError(t, err)
	di2 := model.DealInfo{
		DealUuid:    uuid.New().String(),
		ChainDealID: 2,
		MinerAddr:   minerAddr2,
		SectorID:    2,
		PieceOffset: 0,
		PieceLength: commpCalc.PieceSize,
	}
	err = cl.AddDealForPiece(ctx, commpCalc.PieceCID, di2)
	require.NoError(t, err)

	// In the yugabyte implementation PiecesCount queries the postgres
	// PieceTracker table. We need to first call NextPiecesToCheck so that the
	// piece information is copied from the cassandra database over to the
	// postgres database PieceTracker table.
	_, err = cl.NextPiecesToCheck(ctx, minerAddr)

	// There should be one deal for the piece on the first miner address
	count, err := cl.PiecesCount(ctx, minerAddr)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// There should be zero pieces for another random miner address
	otherMinerAddr, err := address.NewIDAddress(1234)
	require.NoError(t, err)

	count, err = cl.PiecesCount(ctx, otherMinerAddr)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}
