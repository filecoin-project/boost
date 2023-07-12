package framework

import (
	"errors"
	"io"
	"math/bits"
	"os"

	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-data-segment/datasegment"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
)

type CarDetails struct {
	CarPath  string
	Root     cid.Cid
	FilePath string
}

type segmentDetails struct {
	Piece    *abi.PieceInfo
	Segments []*CarDetails
	CarPath  string
}

func GenerateDataSegmentFiles(tmpdir string, num int) (segmentDetails, error) {
	if num < 2 {
		return segmentDetails{}, errors.New("at least 2 deals are required to test data segment index")
	}

	fileSize := 200000

	var cars []*CarDetails
	for i := 1; i <= num; i++ {
		fileName, err := testutil.CreateRandomFile(tmpdir, i, fileSize)
		if err != nil {
			return segmentDetails{}, err
		}

		rootCid, carFilepath, err := testutil.CreateDenseCARv2(tmpdir, fileName)
		if err != nil {
			return segmentDetails{}, err
		}

		cars = append(cars, &CarDetails{
			Root:     rootCid,
			CarPath:  carFilepath,
			FilePath: fileName,
		})
	}

	finalCar, err := os.CreateTemp(tmpdir, "finalcar")
	if err != nil {
		return segmentDetails{}, err
	}

	err = generateDataSegmentCar(cars, finalCar)
	if err != nil {
		return segmentDetails{}, err
	}

	finalCarName := finalCar.Name()
	err = finalCar.Close()
	if err != nil {
		return segmentDetails{}, err
	}

	//finalCarPath := path.Join(tmpdir, finalCarName)

	cidAndSize, err := storagemarket.GenerateCommP(finalCarName)
	if err != nil {
		return segmentDetails{}, err
	}

	return segmentDetails{
		Piece:    cidAndSize,
		Segments: cars,
		CarPath:  finalCarName,
	}, nil
}

func generateDataSegmentCar(cars []*CarDetails, outputFile *os.File) error {

	readers := make([]io.Reader, 0)
	deals := make([]abi.PieceInfo, 0)

	for _, car := range cars {

		r, err := os.Open(car.CarPath)

		if err != nil {
			return err
		}

		readers = append(readers, r)
		cp := new(commp.Calc)

		_, err = io.Copy(cp, r)
		if err != nil {
			return err
		}

		rawCommP, size, err := cp.Digest()
		if err != nil {
			return err
		}

		_, err = r.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}

		c, _ := commcid.DataCommitmentV1ToCID(rawCommP)

		subdeal := abi.PieceInfo{
			Size:     abi.PaddedPieceSize(size),
			PieceCID: c,
		}
		deals = append(deals, subdeal)
	}

	_, size, err := datasegment.ComputeDealPlacement(deals)
	if err != nil {
		return err
	}

	overallSize := abi.PaddedPieceSize(size)
	// we need to make this the 'next' power of 2 in order to have space for the index
	next := 1 << (64 - bits.LeadingZeros64(uint64(overallSize+256)))

	a, err := datasegment.NewAggregate(abi.PaddedPieceSize(next), deals)
	if err != nil {
		return err
	}
	out, err := a.AggregateObjectReader(readers)
	if err != nil {
		return err
	}

	_, err = io.Copy(outputFile, out)
	if err != nil {
		return err
	}

	return nil
}
