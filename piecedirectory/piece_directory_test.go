package piecedirectory

import (
	"context"
	"github.com/filecoin-project/boostd-data/svc"
	"github.com/stretchr/testify/require"
	"testing"
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
