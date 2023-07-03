package svc

import (
	"context"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestServiceLevelDB(t *testing.T) {
	_ = logging.SetLogLevel("cbtest", "debug")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bdsvc, err := NewLevelDB("")
	require.NoError(t, err)

	testService(ctx, t, bdsvc, "localhost:0")
}

func TestServiceFuzzLevelDB(t *testing.T) {
	t.Skip()
	_ = logging.SetLogLevel("*", "info")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bdsvc, err := NewLevelDB("")
	require.NoError(t, err)
	addr := "localhost:0"
	ln, err := bdsvc.Start(ctx, addr)
	require.NoError(t, err)
	testServiceFuzz(ctx, t, ln.String())
}

func TestCleanupLevelDB(t *testing.T) {
	_ = logging.SetLogLevel("*", "debug")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bdsvc, err := NewLevelDB("")
	require.NoError(t, err)
	testCleanup(ctx, t, bdsvc, "localhost:0")
}
