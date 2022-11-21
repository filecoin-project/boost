package piecemeta_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/boost/piecemeta"
	mock_piecemeta "github.com/filecoin-project/boost/piecemeta/mocks"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/boostd-data/client"
	"github.com/filecoin-project/boostd-data/couchbase"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/filecoin-project/boostd-data/svc/types"
	"github.com/filecoin-project/go-commp-utils/writer"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/stretchr/testify/require"
)

var testCouchSettings = couchbase.DBSettings{
	ConnectString: "couchbase://127.0.0.1",
	Auth: couchbase.DBSettingsAuth{
		Username: "Administrator",
		Password: "boostdemo",
	},
	PieceMetadataBucket: couchbase.DBSettingsBucket{
		RAMQuotaMB: 128,
	},
	MultihashToPiecesBucket: couchbase.DBSettingsBucket{
		RAMQuotaMB: 128,
	},
	PieceOffsetsBucket: couchbase.DBSettingsBucket{
		RAMQuotaMB: 128,
	},
}

func TestPieceMeta(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("level db", func(t *testing.T) {
		bdsvc, err := svc.NewLevelDB("")
		require.NoError(t, err)
		testPieceMeta(ctx, t, bdsvc)
	})
	t.Run("couchbase", func(t *testing.T) {
		// TODO: Unskip this test once the couchbase instance can be created
		//  from a docker container as part of the test
		t.Skip()
		bdsvc := svc.NewCouchbase(testCouchSettings)
		testPieceMeta(ctx, t, bdsvc)
	})
}

func testPieceMeta(ctx context.Context, t *testing.T, bdsvc *svc.Service) {
	err := bdsvc.Start(ctx, 8042)
	require.NoError(t, err)

	cl := client.NewStore()
	err = cl.Dial(ctx, "http://localhost:8042")
	require.NoError(t, err)
	defer cl.Close(ctx)

	t.Run("not found", func(t *testing.T) {
		testPieceMetaNotFound(ctx, t, cl)
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
}

func testPieceMetaNotFound(ctx context.Context, t *testing.T, cl *client.Store) {
	ctrl := gomock.NewController(t)
	pr := mock_piecemeta.NewMockPieceReader(ctrl)
	pm := piecemeta.NewPieceMeta(cl, pr, 1)

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
	carFilePath := createCarFile(t)
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
	pr := createMockPieceReader(t, carv1Reader)

	pm := piecemeta.NewPieceMeta(cl, pr, 1)
	pieceCid := calculateCommp(t, carv1Reader).PieceCID

	// Add deal info for the piece - it doesn't matter what it is, the piece
	// just needs to have at least one deal associated with it
	di := model.DealInfo{
		DealUuid:    uuid.New(),
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
	recs := getRecords(t, carv1Reader)

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
	carFilePath := createCarFile(t)
	carFile, err := os.Open(carFilePath)
	require.NoError(t, err)
	defer carFile.Close()

	carReader, err := car.OpenReader(carFilePath)
	require.NoError(t, err)
	defer carReader.Close()
	carv1Reader, err := carReader.DataReader()
	require.NoError(t, err)

	// Any calls to get a reader over data should return a reader over the random CAR file
	pr := createMockPieceReader(t, carv1Reader)

	recs := getRecords(t, carv1Reader)
	pieceCid := calculateCommp(t, carv1Reader).PieceCID
	err = cl.AddIndex(ctx, pieceCid, recs)
	require.NoError(t, err)

	// Add deal info for the piece - it doesn't matter what it is, the piece
	// just needs to have at least one deal associated with it
	di := model.DealInfo{
		DealUuid:    uuid.New(),
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
	pm := piecemeta.NewPieceMeta(cl, pr, 1)
	sz, err := pm.BlockstoreGetSize(ctx, rec.Cid)
	require.NoError(t, err)
	require.Equal(t, len(blk.RawData()), sz)
}

// Verify that if the deal info has been imported from the DAG store, meaning
// it does not have CAR size information, GetCarSize will correctly calculate
// the CAR size from the index + piece data
func testCarFileSize(ctx context.Context, t *testing.T, cl *client.Store) {
	// Create a random CAR file
	carFilePath := createCarFile(t)
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
	pr := createMockPieceReader(t, carv1Reader)

	recs := getRecords(t, carv1Reader)
	commpCalc := calculateCommp(t, carv1Reader)
	err = cl.AddIndex(ctx, commpCalc.PieceCID, recs)
	require.NoError(t, err)

	// Add deal info for the piece without a CAR file
	di := model.DealInfo{
		DealUuid:    uuid.New(),
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
	pm := piecemeta.NewPieceMeta(cl, pr, 1)
	size, err := pm.GetCarSize(ctx, commpCalc.PieceCID)
	require.NoError(t, err)
	require.Equal(t, len(carBytes), int(size))
}

type MockSectionReader struct {
	car.SectionReader
}

func (MockSectionReader) Close() error { return nil }

func createMockPieceReader(t *testing.T, reader car.SectionReader) *mock_piecemeta.MockPieceReader {
	ctrl := gomock.NewController(t)
	pr := mock_piecemeta.NewMockPieceReader(ctrl)
	pr.EXPECT().GetReader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, _ abi.SectorNumber, _ abi.PaddedPieceSize, _ abi.PaddedPieceSize) (piecemeta.SectionReader, error) {
			_, err := reader.Seek(0, io.SeekStart)
			return MockSectionReader{reader}, err
		})
	return pr
}

// Get the index records from the CAR file
func getRecords(t *testing.T, reader car.SectionReader) []model.Record {
	_, err := reader.Seek(0, io.SeekStart)
	require.NoError(t, err)

	blockReader, err := car.NewBlockReader(reader)
	require.NoError(t, err)

	var recs []model.Record
	blockMetadata, err := blockReader.SkipNext()
	for err == nil {
		recs = append(recs, model.Record{
			Cid: blockMetadata.Cid,
			OffsetSize: model.OffsetSize{
				Offset: blockMetadata.Offset,
				Size:   blockMetadata.Size,
			},
		})

		blockMetadata, err = blockReader.SkipNext()
	}
	require.ErrorIs(t, err, io.EOF)

	return recs
}

func createCarFile(t *testing.T) string {
	rseed := rand.Int()
	randomFilePath, err := testutil.CreateRandomFile(t.TempDir(), rseed, 64*1024)
	require.NoError(t, err)
	_, carFilePath, err := testutil.CreateDenseCARv2(t.TempDir(), randomFilePath)
	require.NoError(t, err)
	return carFilePath
}

func calculateCommp(t *testing.T, rdr io.ReadSeeker) writer.DataCIDSize {
	_, err := rdr.Seek(0, io.SeekStart)
	require.NoError(t, err)

	w := &writer.Writer{}
	_, err = io.CopyBuffer(w, rdr, make([]byte, writer.CommPBuf))
	require.NoError(t, err)

	commp, err := w.Sum()
	require.NoError(t, err)

	return commp
}
