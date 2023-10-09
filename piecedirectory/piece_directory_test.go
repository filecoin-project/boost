package piecedirectory

import (
	"context"
	"testing"

	"github.com/filecoin-project/boost/extern/boostd-data/svc"
	"github.com/stretchr/testify/require"
)

func TestPieceDirectoryLevelDB(t *testing.T) {
	bdsvc, err := svc.NewLevelDB("")
	require.NoError(t, err)
	testPieceDirectory(context.Background(), t, bdsvc)
}

func TestPieceDirectoryLevelDBFuzz(t *testing.T) {
	//_ = logging.SetLogLevel("piecedirectory", "debug")
	bdsvc, err := svc.NewLevelDB("")
	require.NoError(t, err)
	testPieceDirectoryFuzz(context.Background(), t, bdsvc)
}
