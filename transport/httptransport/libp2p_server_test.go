package httptransport

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/boost/transport/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

// TestLibp2pDataServerAuth verifies that authorization works as expected
func TestLibp2pDataServerAuth(t *testing.T) {
	ctx := context.Background()

	rawSize := 2 * 1024 * 1024
	st := newServerTest(t, rawSize)

	clientHost, srvHost := setupLibp2pHosts(t)
	defer srvHost.Close()
	defer clientHost.Close()

	srv := NewLibp2pCarServer(srvHost, st.ds, st.bs, ServerConfig{
		AnnounceAddr: srvHost.Addrs()[0],
		RetryTimeout: time.Minute,
	})
	err := srv.Start()
	require.NoError(t, err)
	defer srv.Stop() //nolint:errcheck

	// Add an auth token
	carSize := len(st.carBytes)
	proposalCid, err := cid.Parse("bafkqaaa")
	require.NoError(t, err)
	dbid := uint(1)
	xfer, err := srv.PrepareForDataRequest(context.Background(), dbid, proposalCid, st.root.Cid(), uint64(carSize))
	require.NoError(t, err)

	srvEvts := []types.TransferState{}
	srv.Subscribe(func(sdbid uint, st types.TransferState) {
		if dbid != sdbid {
			return
		}
		srvEvts = append(srvEvts, st)
	})

	// Perform retrieval with the auth token
	req := newLibp2pHttpRequest(srvHost, xfer.AuthToken)
	of := getTempFilePath(t)
	th := executeTransfer(t, ctx, New(clientHost), carSize, req, of)
	require.NotNil(t, th)

	// Wait for the transfer to complete
	clientEvts := waitForTransferComplete(th)
	require.NotEmpty(t, clientEvts)
	lastClientEvt := clientEvts[len(clientEvts)-1]
	require.EqualValues(t, carSize, lastClientEvt.NBytesReceived)
	assertFileContents(t, of, st.carBytes)

	// Check that the server event subscription is working correctly
	require.NotEmpty(t, srvEvts)
	lastSrvEvt := srvEvts[len(srvEvts)-1]
	require.Equal(t, types.TransferStatusStarted, srvEvts[0].Status)
	require.Equal(t, types.TransferStatusCompleted, lastSrvEvt.Status)
	require.EqualValues(t, lastClientEvt.NBytesReceived, lastSrvEvt.Sent)

	// Remove the auth token from the server
	err = srv.Cleanup(xfer.AuthToken)
	require.NoError(t, err)

	// Attempt a second retrieval - it should fail with a 401 HTTP error
	of2 := getTempFilePath(t)
	th2 := executeTransfer(t, ctx, New(clientHost), carSize, req, of2)
	require.NotNil(t, th2)

	evts2 := waitForTransferComplete(th2)
	require.NotEmpty(t, evts2)
	require.Error(t, evts2[len(evts2)-1].Error)
}

func newLibp2pHttpRequest(h host.Host, token string) types.HttpRequest {
	return types.HttpRequest{
		URL: "libp2p://" + h.Addrs()[0].String() + "/p2p/" + h.ID().Pretty(),
		Headers: map[string]string{
			"Authorization": BasicAuthHeader("", token),
		},
	}
}

// TestLibp2pDataServerRetry verifies that there is an error event when the
// client does not retry downloading the data before the retry timeout
func TestLibp2pDataServerRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rawSize := 2 * 1024 * 1024

	testCases := []struct {
		name              string
		retryTimeout      time.Duration
		attemptRetryAfter time.Duration
	}{{
		name:              "client retries before timeout",
		attemptRetryAfter: 100 * time.Millisecond,
		retryTimeout:      1000 * time.Millisecond,
	}, {
		name:              "client retries after timeout",
		attemptRetryAfter: 500 * time.Millisecond,
		retryTimeout:      200 * time.Millisecond,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expectTimeout := tc.attemptRetryAfter < tc.retryTimeout
			st := newServerTest(t, rawSize)

			clientHost, srvHost := setupLibp2pHosts(t)
			defer srvHost.Close()
			defer clientHost.Close()

			srv := NewLibp2pCarServer(srvHost, st.ds, st.bs, ServerConfig{
				AnnounceAddr: srvHost.Addrs()[0],
				RetryTimeout: tc.retryTimeout,
			})
			err := srv.Start()
			require.NoError(t, err)
			defer srv.Stop() //nolint:errcheck

			// Add an auth token
			carSize := len(st.carBytes)
			proposalCid, err := cid.Parse("bafkqaaa")
			require.NoError(t, err)
			dbid := uint(1)
			xfer, err := srv.PrepareForDataRequest(context.Background(), dbid, proposalCid, st.root.Cid(), uint64(carSize))
			require.NoError(t, err)

			// Perform retrieval with the auth token
			req := newLibp2pHttpRequest(srvHost, xfer.AuthToken)
			of := getTempFilePath(t)
			backoff := BackOffRetryOpt(tc.attemptRetryAfter, tc.attemptRetryAfter, 1, 5)
			ht := New(clientHost, backoff)

			// Listen for server events
			srvEvts := make(chan types.TransferState, rawSize)
			srv.Subscribe(func(sdbid uint, st types.TransferState) {
				if dbid != sdbid {
					return
				}
				srvEvts <- st
				if st.Status == types.TransferStatusCompleted || st.Status == types.TransferStatusFailed {
					close(srvEvts)
				}
			})

			th := executeTransfer(t, ctx, ht, carSize, req, of)
			require.NotNil(t, th)

			// Wait for some data to be received by the client
			clientSub := th.Sub()
			evt := <-clientSub
			require.NoError(t, evt.Error)

			// Disconnect the connection
			err = clientHost.Network().ClosePeer(srvHost.ID())
			require.NoError(t, err)

			// Wait for the transfer to complete or error out on the client
			var lastClientEvt types.TransportEvent
			for evt := range clientSub {
				lastClientEvt = evt
			}

			// Wait for the transfer to complete or error out on the server
			var lastSrvEvt types.TransferState
			for evt := range srvEvts {
				lastSrvEvt = evt
			}

			if expectTimeout {
				// The client is configured to retry before the retry timeout
				// expires, so expect the transfer to succeed
				require.NoError(t, lastClientEvt.Error)
				require.Equal(t, types.TransferStatusCompleted, lastSrvEvt.Status)
			} else {
				// The client is configured to retry after the retry timeout
				// expires, so expect the transfer to fail.
				require.Error(t, lastClientEvt.Error)
				// Expect a 401 error to occur because the auth token gets
				// deleted when the retry timeout expires.
				require.Contains(t, lastClientEvt.Error.Error(), "401")
				// Expect a failure event on the server
				require.Equal(t, types.TransferStatusFailed, lastSrvEvt.Status)
			}
		})
	}
}

func setupLibp2pHosts(t *testing.T) (host.Host, host.Host) {
	m1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	m2, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	srvHost := newHost(t, m1)
	clientHost := newHost(t, m2)

	srvHost.Peerstore().AddAddrs(clientHost.ID(), clientHost.Addrs(), peerstore.PermanentAddrTTL)
	clientHost.Peerstore().AddAddrs(srvHost.ID(), srvHost.Addrs(), peerstore.PermanentAddrTTL)

	return clientHost, srvHost
}
