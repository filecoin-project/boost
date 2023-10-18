//go:build test_lid
// +build test_lid

package piecedirectory

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/svc"
)

func TestPieceDirectoryYugabyte(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	bdsvc := svc.SetupYugabyte(t)
	testPieceDirectory(ctx, t, bdsvc)
}
