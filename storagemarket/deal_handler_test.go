package storagemarket

import (
	"context"
	"errors"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/stretchr/testify/require"
)

// TestCancellationSimple tests that we are able to cancel an ongoing transfer.
func TestCancellationSimple(t *testing.T) {
	_, tCancel := context.WithCancel(context.Background())

	dh := &dealHandler{
		providerCtx:    context.Background(),
		transferCancel: tCancel,
		transferDone:   make(chan error, 1),
	}

	require.False(t, dh.TransferCancelledByUser())
	dh.setCancelTransferResponse(nil)
	require.NoError(t, dh.cancelTransfer())
	require.True(t, dh.TransferCancelledByUser())
}

// TestCancellationByCancel tests that concurrent calls to cancel a transfer lead to the transfer being cancelled only once and
// other calls are no-op
func TestCancellationByCancel(t *testing.T) {
	smErr := errors.New("some error")
	transferCtx, tCancel := context.WithCancel(context.Background())
	dh := &dealHandler{
		transferCtx:    transferCtx,
		providerCtx:    context.Background(),
		transferCancel: tCancel,
		transferDone:   make(chan error, 1),
	}
	require.False(t, dh.TransferCancelledByUser())

	// even if transfer is cancelled multiple time concurrently, we will see it only once
	for i := 0; i < 20; i++ {
		go func() {

			<-transferCtx.Done()
			dh.setCancelTransferResponse(smErr)

		}()
	}

	var errGrp errgroup.Group
	// once cancellation is in effect, all callers see it as cancelled
	for i := 0; i < 100; i++ {
		errGrp.Go(func() error {
			if err := dh.cancelTransfer(); err != smErr {
				return err
			} else if err == nil {
				return errors.New("expected error")
			}
			return nil
		})
	}

	require.NoError(t, errGrp.Wait())
	require.True(t, dh.TransferCancelledByUser())
}
