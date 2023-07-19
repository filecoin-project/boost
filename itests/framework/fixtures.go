package framework

import (
	"errors"
	"io"
	"math/bits"
	"os"
	"testing"

	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/go-data-segment/datasegment"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/itests/kit"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
)

type CarDetails struct {
	CarPath  string
	Root     []cid.Cid
	FilePath string
}

type SegmentDetails struct {
	Piece    *abi.PieceInfo
	Segments []*CarDetails
	CarPath  string
	CarSize  int64
}

func GenerateDataSegmentFiles(t *testing.T, tmpdir string, num int) (SegmentDetails, error) {
	if num < 2 {
		return SegmentDetails{}, errors.New("at least 2 deals are required to test data segment index")
	}

	fileSize := 1572864

	var cars []*CarDetails
	for i := 1; i <= num; i++ {

		carPath, filePath := kit.CreateRandomCARv1(t, i, fileSize)
		rd, err := car.OpenReader(carPath)
		if err != nil {
			return SegmentDetails{}, err
		}

		roots, err := rd.Roots()
		if err != nil {
			return SegmentDetails{}, err
		}

		err = rd.Close()
		if err != nil {
			return SegmentDetails{}, err
		}

		cars = append(cars, &CarDetails{
			CarPath:  carPath,
			FilePath: filePath,
			Root:     roots,
		})
	}

	finalCar, err := os.CreateTemp(tmpdir, "finalcar")
	if err != nil {
		return SegmentDetails{}, err
	}

	err = generateDataSegmentCar(cars, finalCar)
	if err != nil {
		return SegmentDetails{}, err
	}

	finalCarName := finalCar.Name()
	carStat, err := finalCar.Stat()
	if err != nil {
		return SegmentDetails{}, err
	}
	carSize := carStat.Size()
	err = finalCar.Close()
	if err != nil {
		return SegmentDetails{}, err
	}

	cidAndSize, err := storagemarket.GenerateCommP(finalCarName)
	if err != nil {
		return SegmentDetails{}, err
	}

	return SegmentDetails{
		Piece:    cidAndSize,
		Segments: cars,
		CarPath:  finalCarName,
		CarSize:  carSize,
	}, nil
}

func generateDataSegmentCar(cars []*CarDetails, outputFile *os.File) error {

	readers := make([]io.Reader, 0)
	deals := make([]abi.PieceInfo, 0)

	for _, cf := range cars {

		r, err := os.Open(cf.CarPath)

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
