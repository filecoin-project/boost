//go:build test_lid
// +build test_lid

package piecedirectory

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/boostd-data/client"
	"github.com/filecoin-project/boostd-data/couchbase"
	"github.com/filecoin-project/boostd-data/ldb"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/filecoin-project/boostd-data/yugabyte"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
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

		addr := "localhost:8050"
		err = bdsvc.Start(ctx, addr)
		require.NoError(t, err)

		cl := client.NewStore()
		err = cl.Dial(ctx, fmt.Sprintf("http://%s", addr))
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

		ldb.MinPieceCheckPeriod = prev
	})

	t.Run("couchbase", func(t *testing.T) {
		// TODO: Unskip this test once the couchbase instance can be created
		//  from a docker container in CI
		t.Skip()

		prev := couchbase.MinPieceCheckPeriod
		couchbase.MinPieceCheckPeriod = 1 * time.Second

		svc.SetupCouchbase(t, testCouchSettings)
		bdsvc := svc.NewCouchbase(testCouchSettings)

		addr := "localhost:8051"
		err := bdsvc.Start(ctx, addr)
		require.NoError(t, err)

		cl := client.NewStore()
		err = cl.Dial(ctx, fmt.Sprintf("http://%s", addr))
		require.NoError(t, err)
		defer cl.Close(ctx)

		t.Run("next pieces", func(t *testing.T) {
			testNextPieces(ctx, t, cl, couchbase.MinPieceCheckPeriod)
		})

		t.Run("check pieces", func(t *testing.T) {
			testCheckPieces(ctx, t, cl)
		})

		couchbase.MinPieceCheckPeriod = prev
	})

	t.Run("yugabyte", func(t *testing.T) {
		prev := yugabyte.MinPieceCheckPeriod
		yugabyte.MinPieceCheckPeriod = 1 * time.Second

		svc.SetupYugabyte(t)

		bdsvc := svc.NewYugabyte(svc.TestYugabyteSettings)

		addr := "localhost:8044"
		err := bdsvc.Start(ctx, addr)
		require.NoError(t, err)

		cl := client.NewStore()
		err = cl.Dial(ctx, fmt.Sprintf("http://%s", addr))
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

		yugabyte.MinPieceCheckPeriod = prev
	})
}

// Verify that after a new piece is added
// - NextPiecesToCheck immediately returns the piece
// - NextPiecesToCheck returns the piece every pieceCheckPeriod
func testNextPieces(ctx context.Context, t *testing.T, cl *client.Store, pieceCheckPeriod time.Duration) {
	// Add a new piece
	pieceCid := blocks.NewBlock([]byte(fmt.Sprintf("%d", time.Now().UnixMilli()))).Cid()
	di := model.DealInfo{
		DealUuid:    uuid.New().String(),
		ChainDealID: 1,
		SectorID:    1,
		PieceOffset: 0,
		PieceLength: 2048,
	}
	err := cl.AddDealForPiece(ctx, pieceCid, di)
	require.NoError(t, err)

	// Sleep for half the piece check period
	time.Sleep(pieceCheckPeriod / 2)

	// NextPiecesToCheck should return the piece (because it hasn't been checked yet)
	pcids, err := cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Contains(t, pcids, pieceCid)

	// Calling NextPiecesToCheck again should return nothing, because the piece
	// was just checked
	pcids, err = cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.NotContains(t, pcids, pieceCid)

	// Sleep for at least the piece check period
	time.Sleep(2 * pieceCheckPeriod)

	// Calling NextPiecesToCheck should return the piece, because it has not
	// been checked for at least one piece check period
	pcids, err = cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Contains(t, pcids, pieceCid)
}

func testNextPiecesPagination(ctx context.Context, t *testing.T, cl *client.Store, setPageSize func(int)) {
	setPageSize(4)

	// Add 9 pieces
	seen := make(map[cid.Cid]int)

	for i := 1; i <= 9; i++ {
		pieceCid := testutil.GenerateCid()
		di := model.DealInfo{
			DealUuid:    uuid.New().String(),
			ChainDealID: abi.DealID(i),
			SectorID:    abi.SectorNumber(i),
			PieceOffset: 0,
			PieceLength: 2048,
		}
		err := cl.AddDealForPiece(ctx, pieceCid, di)
		require.NoError(t, err)
	}

	// expect to get 4 pieces
	pcids, err := cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Len(t, pcids, 4)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	// expect to get 4 pieces
	pcids, err = cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Len(t, pcids, 4)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	// expect to get 1 pieces (end of table)
	pcids, err = cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Len(t, pcids, 1)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	require.Len(t, seen, 9)

	// expect to get 0 pieces (first four)
	pcids, err = cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Len(t, pcids, 0)

	// expect to get 0 pieces (second four)
	pcids, err = cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Len(t, pcids, 0)

	// expect to get 0 pieces (end of table)
	pcids, err = cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Len(t, pcids, 0)

	// Add 1 more piece
	for i := 1; i <= 1; i++ {
		pieceCid := testutil.GenerateCid()
		di := model.DealInfo{
			DealUuid:    uuid.New().String(),
			ChainDealID: abi.DealID(100),
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

	pcids, err = cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Len(t, pcids, 4)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	pcids, err = cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Len(t, pcids, 4)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	pcids, err = cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Len(t, pcids, 2)
	for _, cid := range pcids {
		seen[cid] = 1
	}

	require.Len(t, seen, 10)
}

func testCheckPieces(ctx context.Context, t *testing.T, cl *client.Store) {
	// Create a random CAR file
	carFilePath := CreateCarFile(t)
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
	di := model.DealInfo{
		DealUuid:    uuid.New().String(),
		ChainDealID: 1,
		SectorID:    1,
		PieceOffset: 0,
		PieceLength: commpCalc.PieceSize,
	}
	err = cl.AddDealForPiece(ctx, commpCalc.PieceCID, di)
	require.NoError(t, err)

	// Create a doctor
	sapi := CreateMockDoctorSealingApi()
	doc := NewDoctor(cl, sapi)

	// Check the piece
	err = doc.checkPiece(ctx, commpCalc.PieceCID)
	require.NoError(t, err)

	// The piece should be flagged because there is no index for it
	count, err := cl.FlaggedPiecesCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	pcids, err := cl.FlaggedPiecesList(ctx, nil, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(pcids))

	// Create an index for the piece
	recs := GetRecords(t, carv1Reader)
	err = cl.AddIndex(ctx, commpCalc.PieceCID, recs, true)
	require.NoError(t, err)

	// Check the piece
	err = doc.checkPiece(ctx, commpCalc.PieceCID)
	require.NoError(t, err)

	// The piece should no longer be flagged
	count, err = cl.FlaggedPiecesCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	pcids, err = cl.FlaggedPiecesList(ctx, nil, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 0, len(pcids))

	// Mark the piece as not being unsealed
	sapi.isUnsealed = false

	// Check the piece
	err = doc.checkPiece(ctx, commpCalc.PieceCID)
	require.NoError(t, err)

	// The piece should be flagged because there is no unsealed copy
	count, err = cl.FlaggedPiecesCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	pcids, err = cl.FlaggedPiecesList(ctx, nil, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(pcids))

	// Mark the piece as being unsealed
	sapi.isUnsealed = true

	// Check the piece
	err = doc.checkPiece(ctx, commpCalc.PieceCID)
	require.NoError(t, err)

	// The piece should no longer be flagged
	count, err = cl.FlaggedPiecesCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	pcids, err = cl.FlaggedPiecesList(ctx, nil, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 0, len(pcids))
}
