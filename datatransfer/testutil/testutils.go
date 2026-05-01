package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/boost/datatransfer"
)

// StartAndWaitForReady is a utility function to start a module and verify it reaches the ready state
func StartAndWaitForReady(ctx context.Context, t *testing.T, manager datatransfer.Manager) {
	ready := make(chan error, 1)
	manager.OnReady(func(err error) {
		ready <- err
	})
	require.NoError(t, manager.Start(ctx))
	select {
	case <-ctx.Done():
		t.Fatal("did not finish starting up module")
	case err := <-ready:
		require.NoError(t, err)
	}
}
