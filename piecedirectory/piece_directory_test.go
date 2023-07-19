package piecedirectory

import (
	"context"
	"os"
	"testing"

	"github.com/filecoin-project/boostd-data/svc"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestPieceDirectoryLevelDB(t *testing.T) {
	bdsvc, err := svc.NewLevelDB("")
	require.NoError(t, err)
	testPieceDirectory(context.Background(), t, bdsvc)
}

func TestSegmentParsing(t *testing.T) {
	//pieceCid, err := cid.Parse(string("baga6ea4seaqjqmsaznxm4iq43dtb5xghvlwodloby2gso5ai4whqkw5abtrryjq"))
	//require.NoError(t, err)
	//pieceInfo := abi.PieceInfo{
	//	Size:     16777216,
	//	PieceCID: pieceCid,
	//}
	//
	//carSize := int64(16646144)
	carSize := int64(8323072)
	pieceCid, err := cid.Parse(string("baga6ea4seaqly4jqbnjbw5dz4gpcu5uuu3o3t7ohzjpjx7x6z3v53tkfutogwga"))
	require.NoError(t, err)

	rd, err := os.Open("testdata/finalcar2221360956")
	require.NoError(t, err)

	recs, err := parsePieceWithDataSegmentIndex(pieceCid, carSize, rd)
	require.NoError(t, err)

	t.Log(recs)

	err = rd.Close()
	require.NoError(t, err)
}
