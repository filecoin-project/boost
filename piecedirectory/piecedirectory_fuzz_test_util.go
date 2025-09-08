package piecedirectory

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/client"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/extern/boostd-data/svc"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/ipld/go-car/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func testPieceDirectoryFuzz(ctx context.Context, t *testing.T, bdsvc *svc.Service) {
	ln, err := bdsvc.Start(ctx, "localhost:0")
	require.NoError(t, err)

	cl := client.NewStore()
	err = cl.Dial(ctx, fmt.Sprintf("ws://%s", ln))
	require.NoError(t, err)
	defer cl.Close(ctx)

	t.Run("blockstore get", func(t *testing.T) {
		testPieceDirectoryBlockstoreGetFuzz(ctx, t, cl)
	})
}

func testPieceDirectoryBlockstoreGetFuzz(ctx context.Context, t *testing.T, cl *client.Store) {
	pieceCount := 5
	readers := make(map[abi.SectorNumber]car.SectionReader)
	for i := 0; i < pieceCount; i++ {
		// Create a random CAR file
		_, carFilePath := CreateCarFile(t, i+1)
		carFile, err := os.Open(carFilePath)
		require.NoError(t, err)
		defer func() {
			_ = carFile.Close()
		}()

		carReader, err := car.OpenReader(carFilePath)
		require.NoError(t, err)
		defer func() {
			_ = carReader.Close()
		}()
		carv1Reader, err := carReader.DataReader()
		require.NoError(t, err)

		readers[abi.SectorNumber(i+1)] = carv1Reader
	}

	// Any calls to get a reader over data should return a reader over the random CAR file
	pr := CreateMockPieceReaders(t, readers)

	pm := NewPieceDirectory(cl, pr, 1)
	pm.Start(ctx)

	for sectorNumber, reader := range readers {
		pieceCid := CalculateCommp(t, reader).PieceCID

		// Add deal info for each piece
		di := model.DealInfo{
			DealUuid:    uuid.New().String(),
			ChainDealID: 1,
			SectorID:    sectorNumber,
			PieceOffset: 0,
			PieceLength: 0,
		}
		err := pm.AddDealForPiece(ctx, pieceCid, di)
		require.NoError(t, err)
	}

	var eg errgroup.Group
	for _, reader := range readers {
		reader := reader
		eg.Go(func() error {
			// Get the index (offset and size information)
			recs := GetRecords(t, reader)

			// Fetch many blocks in parallel
			var fetchEg errgroup.Group
			for _, rec := range recs {
				rec := rec
				fetchEg.Go(func() error {
					// Add a bit of random delay so that we don't always hit the cache
					time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)

					blk, err := pm.BlockstoreGet(ctx, rec.Cid)
					if err != nil {
						return err
					}
					if len(blk) != int(rec.Size) {
						return fmt.Errorf("wrong block size")
					}
					chkc, err := rec.Cid.Prefix().Sum(blk)
					if err != nil {
						return err
					}
					if !chkc.Equals(rec.Cid) {
						return fmt.Errorf("wrong cid")
					}
					return nil
				})
			}
			return fetchEg.Wait()
		})
	}
	require.NoError(t, eg.Wait())
}
