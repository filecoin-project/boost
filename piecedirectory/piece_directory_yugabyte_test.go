//go:build test_lid
// +build test_lid

package piecedirectory

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/boostd-data/svc"
)

func TestPieceDirectoryYugabyte(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	svc.SetupYugabyte(t)
	bdsvc := svc.NewYugabyte(svc.TestYugabyteSettings)
	testPieceDirectory(ctx, t, bdsvc)
}
