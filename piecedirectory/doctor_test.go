package piecedirectory

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/boostd-data/client"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-car/v2"
	"github.com/stretchr/testify/require"
)

func TestPieceDoctor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	t.Run("leveldb", func(t *testing.T) {
		bdsvc, err := svc.NewLevelDB("")
		require.NoError(t, err)
		testPieceDoctor(ctx, t, bdsvc)
	})
	t.Run("couchbase", func(t *testing.T) {
		// TODO: Unskip this test once the couchbase instance can be created
		//  from a docker container as part of the test
		//t.Skip()
		bdsvc := svc.NewCouchbase(TestCouchSettings)
		testPieceDoctor(ctx, t, bdsvc)
	})
}

func testPieceDoctor(ctx context.Context, t *testing.T, bdsvc *svc.Service) {
	err := bdsvc.Start(ctx, 8042)
	require.NoError(t, err)

	cl := client.NewStore()
	err = cl.Dial(ctx, "http://localhost:8042")
	require.NoError(t, err)
	defer cl.Close(ctx)

	t.Run("check pieces", func(t *testing.T) {
		testCheckPieces(ctx, t, cl)
	})

	t.Run("next pieces", func(t *testing.T) {
		testNextPieces(ctx, t, cl)
	})
}

// Verify that after a new piece is added
// - NextPiecesToCheck immediately returns the piece
// - NextPiecesToCheck returns the piece every PieceCheckPeriod
func testNextPieces(ctx context.Context, t *testing.T, cl *client.Store) {
	// Add a new piece
	pieceCid := blocks.NewBlock([]byte(fmt.Sprintf("%d", time.Now().UnixMilli()))).Cid()
	fmt.Println(pieceCid)
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
	time.Sleep(TestCouchSettings.PieceCheckPeriod / 2)

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
	time.Sleep(2 * TestCouchSettings.PieceCheckPeriod)

	// Calling NextPiecesToCheck should return the piece, because it has not
	// been checked for at least one piece check period
	pcids, err = cl.NextPiecesToCheck(ctx)
	require.NoError(t, err)
	require.Contains(t, pcids, pieceCid)
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
	err = cl.AddIndex(ctx, commpCalc.PieceCID, recs)
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
