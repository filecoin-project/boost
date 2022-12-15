package piecedirectory

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	mock_piecedirectory "github.com/filecoin-project/boost/piecedirectory/types/mocks"
	"github.com/filecoin-project/boostd-data/client"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/stretchr/testify/require"
)

func TestPieceDirectory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("leveldb", func(t *testing.T) {
		bdsvc, err := svc.NewLevelDB("")
		require.NoError(t, err)
		testPieceDirectory(ctx, t, bdsvc)
	})
	t.Run("couchbase", func(t *testing.T) {
		// TODO: Unskip this test once the couchbase instance can be created
		//  from a docker container as part of the test
		t.Skip()
		bdsvc := svc.NewCouchbase(TestCouchSettings)
		testPieceDirectory(ctx, t, bdsvc)
	})
}

func testPieceDirectory(ctx context.Context, t *testing.T, bdsvc *svc.Service) {
	err := bdsvc.Start(ctx, 8042)
	require.NoError(t, err)

	cl := client.NewStore()
	err = cl.Dial(ctx, "http://localhost:8042")
	require.NoError(t, err)
	defer cl.Close(ctx)

	t.Run("not found", func(t *testing.T) {
		testPieceDirectoryNotFound(ctx, t, cl)
	})

	t.Run("basic blockstore", func(t *testing.T) {
		testBasicBlockstoreMethods(ctx, t, cl)
	})

	t.Run("imported index", func(t *testing.T) {
		testImportedIndex(ctx, t, cl)
	})

	t.Run("car file size", func(t *testing.T) {
		testCarFileSize(ctx, t, cl)
	})

	t.Run("flagging pieces", func(t *testing.T) {
		testFlaggingPieces(ctx, t, cl)
	})
}

func testPieceDirectoryNotFound(ctx context.Context, t *testing.T, cl *client.Store) {
	ctrl := gomock.NewController(t)
	pr := mock_piecedirectory.NewMockPieceReader(ctrl)
	pm := NewPieceDirectory(cl, pr, 1)

	nonExistentPieceCid, err := cid.Parse("bafkqaaa")
	require.NoError(t, err)

	_, err = pm.GetPieceMetadata(ctx, nonExistentPieceCid)
	require.True(t, types.IsNotFound(err))
	require.Error(t, err)

	_, err = pm.GetPieceDeals(ctx, nonExistentPieceCid)
	require.True(t, types.IsNotFound(err))
	require.Error(t, err)

	_, err = pm.GetOffsetSize(ctx, nonExistentPieceCid, nonExistentPieceCid.Hash())
	require.True(t, types.IsNotFound(err))
	require.Error(t, err)

	_, err = pm.GetIterableIndex(ctx, nonExistentPieceCid)
	require.True(t, types.IsNotFound(err))
	require.Error(t, err)

	_, err = pm.PiecesContainingMultihash(ctx, nonExistentPieceCid.Hash())
	require.True(t, types.IsNotFound(err))
	require.Error(t, err)
}

// Verify that Has, GetSize and Get block work
func testBasicBlockstoreMethods(ctx context.Context, t *testing.T, cl *client.Store) {
	carFilePath := CreateCarFile(t)
	carFile, err := os.Open(carFilePath)
	require.NoError(t, err)
	defer carFile.Close()

	// Create a random CAR file
	carReader, err := car.OpenReader(carFilePath)
	require.NoError(t, err)
	defer carReader.Close()
	carv1Reader, err := carReader.DataReader()
	require.NoError(t, err)

	// Any calls to get a reader over data should return a reader over the random CAR file
	pr := CreateMockPieceReader(t, carv1Reader)

	pm := NewPieceDirectory(cl, pr, 1)
	pieceCid := CalculateCommp(t, carv1Reader).PieceCID

	// Add deal info for the piece - it doesn't matter what it is, the piece
	// just needs to have at least one deal associated with it
	di := model.DealInfo{
		DealUuid:    uuid.New().String(),
		ChainDealID: 1,
		SectorID:    2,
		PieceOffset: 0,
		PieceLength: 0,
	}
	err = pm.AddDealForPiece(ctx, pieceCid, di)
	require.NoError(t, err)

	// Create a CARv2 index over the CAR file
	carFileIdxReader, err := os.Open(carFilePath)
	require.NoError(t, err)
	defer carFileIdxReader.Close()
	idx, err := car.ReadOrGenerateIndex(carFileIdxReader)
	require.NoError(t, err)

	// Create a blockstore from the CAR file and index
	_, err = carFile.Seek(0, io.SeekStart)
	require.NoError(t, err)
	bs, err := blockstore.NewReadOnly(carFile, idx)
	require.NoError(t, err)

	// Get the index (offset and size information)
	recs := GetRecords(t, carv1Reader)

	// Verify that blockstore has, get and get size work
	for _, rec := range recs {
		blk, err := bs.Get(ctx, rec.Cid)
		require.NoError(t, err)

		has, err := pm.BlockstoreHas(ctx, rec.Cid)
		require.NoError(t, err)
		require.True(t, has)

		sz, err := pm.BlockstoreGetSize(ctx, rec.Cid)
		require.NoError(t, err)
		require.Equal(t, len(blk.RawData()), sz)

		pmblk, err := pm.BlockstoreGet(ctx, rec.Cid)
		require.NoError(t, err)
		require.True(t, bytes.Equal(blk.RawData(), pmblk))
	}
}

// Verify that if the index has been imported from the DAG store, meaning
// it has offset information but not block size information, the piece directory
// will re-build the index
func testImportedIndex(ctx context.Context, t *testing.T, cl *client.Store) {
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

	// Any calls to get a reader over data should return a reader over the random CAR file
	pr := CreateMockPieceReader(t, carv1Reader)

	recs := GetRecords(t, carv1Reader)
	pieceCid := CalculateCommp(t, carv1Reader).PieceCID
	err = cl.AddIndex(ctx, pieceCid, recs)
	require.NoError(t, err)

	// Add deal info for the piece - it doesn't matter what it is, the piece
	// just needs to have at least one deal associated with it
	di := model.DealInfo{
		DealUuid:    uuid.New().String(),
		ChainDealID: 1,
		SectorID:    2,
		PieceOffset: 0,
		PieceLength: 0,
	}
	err = cl.AddDealForPiece(ctx, pieceCid, di)
	require.NoError(t, err)

	// Remove size information from the index records.
	// This is to simulate what happens when an index is imported from the
	// DAG store: only the offset information is imported (not size
	// information)
	for i, r := range recs {
		recs[i] = model.Record{
			Cid: r.Cid,
			OffsetSize: model.OffsetSize{
				Offset: r.Offset,
				Size:   0,
			},
		}
	}

	// Add the index to the piece directory
	err = cl.AddIndex(ctx, pieceCid, recs)
	require.NoError(t, err)

	// Create a CARv2 index over the CAR file
	carFileIdxReader, err := os.Open(carFilePath)
	require.NoError(t, err)
	defer carFileIdxReader.Close()
	idx, err := car.ReadOrGenerateIndex(carFileIdxReader)
	require.NoError(t, err)

	// Create a blockstore from the CAR file and index
	_, err = carFile.Seek(0, io.SeekStart)
	require.NoError(t, err)
	bs, err := blockstore.NewReadOnly(carFile, idx)
	require.NoError(t, err)

	// Pick a record in the middle of the DAG
	rec := recs[len(recs)/2]
	blk, err := bs.Get(ctx, rec.Cid)
	require.NoError(t, err)

	// Verify that getting the size of a block works correctly:
	// There is no size information in the index so the piece
	// directory should re-build the index and then return the size.
	pm := NewPieceDirectory(cl, pr, 1)
	sz, err := pm.BlockstoreGetSize(ctx, rec.Cid)
	require.NoError(t, err)
	require.Equal(t, len(blk.RawData()), sz)
}

// Verify that if the deal info has been imported from the DAG store, meaning
// it does not have CAR size information, GetCarSize will correctly calculate
// the CAR size from the index + piece data
func testCarFileSize(ctx context.Context, t *testing.T, cl *client.Store) {
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

	// Read the CAR bytes
	carBytes, err := io.ReadAll(carv1Reader)
	require.NoError(t, err)

	// Any calls to get a reader over data should return a reader over the random CAR file
	pr := CreateMockPieceReader(t, carv1Reader)

	recs := GetRecords(t, carv1Reader)
	commpCalc := CalculateCommp(t, carv1Reader)
	err = cl.AddIndex(ctx, commpCalc.PieceCID, recs)
	require.NoError(t, err)

	// Add deal info for the piece without a CAR file
	di := model.DealInfo{
		DealUuid:    uuid.New().String(),
		ChainDealID: 1,
		SectorID:    1,
		PieceOffset: 0,
		PieceLength: commpCalc.PieceSize,
	}
	err = cl.AddDealForPiece(ctx, commpCalc.PieceCID, di)
	require.NoError(t, err)

	// Verify that getting the size of the CAR file works correctly:
	// There is no CAR size information in the deal info, so the piece
	// directory should work it out from the index and piece data.
	pm := NewPieceDirectory(cl, pr, 1)
	size, err := pm.GetCarSize(ctx, commpCalc.PieceCID)
	require.NoError(t, err)
	require.Equal(t, len(carBytes), int(size))
}

func testFlaggingPieces(ctx context.Context, t *testing.T, cl *client.Store) {
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

	recs := GetRecords(t, carv1Reader)
	commpCalc := CalculateCommp(t, carv1Reader)
	err = cl.AddIndex(ctx, commpCalc.PieceCID, recs)
	require.NoError(t, err)

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

	// No pieces flagged, count and list of pieces should be empty
	count, err := cl.FlaggedPiecesCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	pcids, err := cl.FlaggedPiecesList(ctx, nil, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 0, len(pcids))

	// Flag a piece
	err = cl.FlagPiece(ctx, commpCalc.PieceCID)
	require.NoError(t, err)

	// Count and list of pieces should contain one piece
	count, err = cl.FlaggedPiecesCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	pcids, err = cl.FlaggedPiecesList(ctx, nil, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(pcids))

	// Unflag the piece
	err = cl.UnflagPiece(ctx, commpCalc.PieceCID)
	require.NoError(t, err)

	// Count and list of pieces should be empty
	count, err = cl.FlaggedPiecesCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	pcids, err = cl.FlaggedPiecesList(ctx, nil, 0, 10)
	require.NoError(t, err)
	require.Equal(t, 0, len(pcids))
}
