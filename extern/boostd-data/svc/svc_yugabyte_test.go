//go:build test_lid
// +build test_lid

package svc

import (
	"context"
	logging "github.com/ipfs/go-log/v2"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestServiceYugabyte(t *testing.T) {
	_ = logging.SetLogLevel("cbtest", "debug")
	_ = logging.SetLogLevel("boostd-data-yb", "debug")

	// Running yugabyte tests may require download the docker container
	// so set a high timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	bdsvc := SetupYugabyte(t)
	addr := "localhost:0"
	testService(ctx, t, bdsvc, addr)
}

func TestServiceFuzzYugabyte(t *testing.T) {
	t.Skip()
	_ = logging.SetLogLevel("*", "info")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bdsvc := SetupYugabyte(t)

	addr := "localhost:0"
	ln, err := bdsvc.Start(ctx, addr)
	require.NoError(t, err)

	testServiceFuzz(ctx, t, ln.String())
}

func TestCleanupYugabyte(t *testing.T) {
	_ = logging.SetLogLevel("*", "debug")

	// Running yugabyte tests may require download the docker container
	// so set a high timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	bdsvc := SetupYugabyte(t)
	testCleanup(ctx, t, bdsvc, "localhost:0")
}
