package storagemarket

import (
	"os"
	"path/filepath"
	"testing"

	carv2 "github.com/ipld/go-car/v2"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/boost/testutil"
)

func TestValidatePieceData(t *testing.T) {
	dir := t.TempDir()
	bigPieceSize := abi.PaddedPieceSize(1 << 20)

	// A valid CARv2 file is accepted
	src, err := testutil.CreateRandomFile(dir, 1, 1024)
	require.NoError(t, err)
	_, carv2Path, err := testutil.CreateDenseCARv2(dir, src)
	require.NoError(t, err)
	require.NoError(t, validatePieceData(carv2Path, bigPieceSize))

	// A valid CARv1 file is accepted
	carv1Path, _ := testutil.CreateRandomUnixfsFileInCar(t, dir, 1, 1024)
	require.NoError(t, validatePieceData(carv1Path, bigPieceSize))

	// A file larger than the declared piece size is rejected
	require.Error(t, validatePieceData(carv2Path, abi.PaddedPieceSize(128)))

	// An empty file is rejected
	emptyPath := filepath.Join(dir, "empty.car")
	require.NoError(t, os.WriteFile(emptyPath, nil, 0644))
	require.Error(t, validatePieceData(emptyPath, bigPieceSize))

	// A CARv2 file with a valid pragma but an invalid (zeroed) header is
	// rejected. carv2.ReadVersion accepts this file, but carv2.OpenReader (used
	// by addPiece) rejects it, so it must be rejected before publishing.
	malformed := append([]byte{}, carv2.Pragma...)
	malformed = append(malformed, make([]byte, carv2.HeaderSize)...)
	malformed = append(malformed, make([]byte, 973)...)
	malformedPath := filepath.Join(dir, "malformed.car")
	require.NoError(t, os.WriteFile(malformedPath, malformed, 0644))
	require.Error(t, validatePieceData(malformedPath, bigPieceSize))

	// Sanity check that the malformed file really does pass the lax check that
	// is being replaced, i.e. this is a regression test for the vulnerability.
	rd, err := os.Open(malformedPath)
	require.NoError(t, err)
	_, err = carv2.ReadVersion(rd)
	require.NoError(t, err)
	require.NoError(t, rd.Close())
}
