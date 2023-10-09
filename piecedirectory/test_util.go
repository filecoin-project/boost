package piecedirectory

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/piecedirectory/types"
	mock_piecedirectory "github.com/filecoin-project/boost/piecedirectory/types/mocks"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-commp-utils/writer"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/stretchr/testify/require"
)

// Get the index records from the CAR file
func GetRecords(t *testing.T, reader car.SectionReader) []model.Record {
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

func CreateCarFile(t *testing.T, seed ...int) (cid.Cid, string) {
	var rseed int
	if len(seed) == 0 {
		rseed = int(time.Now().UnixMilli())
	} else {
		rseed = seed[0]
	}

	randomFilePath, err := testutil.CreateRandomFile(t.TempDir(), rseed, 64*1024)
	require.NoError(t, err)
	root, carFilePath, err := testutil.CreateDenseCARv2(t.TempDir(), randomFilePath)
	require.NoError(t, err)
	return root, carFilePath
}

func CalculateCommp(t *testing.T, rdr io.ReadSeeker) writer.DataCIDSize {
	_, err := rdr.Seek(0, io.SeekStart)
	require.NoError(t, err)

	w := &writer.Writer{}
	_, err = io.CopyBuffer(w, rdr, make([]byte, writer.CommPBuf))
	require.NoError(t, err)

	commp, err := w.Sum()
	require.NoError(t, err)

	return commp
}

func CreateMockPieceReader(t *testing.T, reader car.SectionReader) *mock_piecedirectory.MockPieceReader {
	ctrl := gomock.NewController(t)
	pr := mock_piecedirectory.NewMockPieceReader(ctrl)
	pr.EXPECT().GetReader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, _ address.Address, _ abi.SectorNumber, _ abi.PaddedPieceSize, _ abi.PaddedPieceSize) (types.SectionReader, error) {
			_, err := reader.Seek(0, io.SeekStart)
			return &MockSectionReader{reader}, err
		})
	return pr
}

func CreateMockPieceReaders(t *testing.T, readers map[abi.SectorNumber]car.SectionReader) *mock_piecedirectory.MockPieceReader {
	ctrl := gomock.NewController(t)
	pr := mock_piecedirectory.NewMockPieceReader(ctrl)
	for sectorNumber := range readers {
		pr.EXPECT().GetReader(gomock.Any(), gomock.Any(), sectorNumber, gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
			func(_ context.Context, _ address.Address, sectorNumber abi.SectorNumber, _ abi.PaddedPieceSize, _ abi.PaddedPieceSize) (types.SectionReader, error) {
				r := readers[sectorNumber]
				_, err := r.Seek(0, io.SeekStart)
				return &MockSectionReader{r}, err
			})
	}
	return pr
}

type MockSectionReader struct {
	car.SectionReader
}

func (*MockSectionReader) Close() error { return nil }
