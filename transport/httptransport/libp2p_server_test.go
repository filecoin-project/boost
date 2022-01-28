package httptransport

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/filecoin-project/boost/transport/types"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

// TestLibp2pCarServerAuth verifies that authorization works as expected
func TestLibp2pCarServerAuth(t *testing.T) {
	ctx := context.Background()

	rawSize := 2 * 1024 * 1024
	st := newServerTest(t, rawSize)

	clientHost, srvHost := setupLibp2pHosts(t)
	defer srvHost.Close()
	defer clientHost.Close()

	authDB := NewAuthTokenDB(st.ds)
	srv := NewLibp2pCarServer(srvHost, authDB, st.bs, ServerConfig{
		AnnounceAddr: srvHost.Addrs()[0],
	})
	err := srv.Start()
	require.NoError(t, err)
	defer srv.Stop() //nolint:errcheck

	// Create an auth token
	carSize := len(st.carBytes)
	proposalCid, err := cid.Parse("bafkqaaa")
	require.NoError(t, err)
	dbid := uint(1)
	xfer, err := srv.PrepareForDataRequest(context.Background(), dbid, proposalCid, st.root.Cid(), uint64(carSize))
	require.NoError(t, err)

	srvEvts := []types.TransferState{}
	srv.Subscribe(func(sdbid uint, st types.TransferState) {
		if dbid == sdbid {
			srvEvts = append(srvEvts, st)
		}
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
	require.EqualValues(t, int(lastClientEvt.NBytesReceived), int(lastSrvEvt.Sent))

	// Remove the auth token from the server
	err = authDB.Delete(ctx, xfer.AuthToken)
	require.NoError(t, err)

	// Attempt a second retrieval - it should fail with a 401 HTTP error
	of2 := getTempFilePath(t)
	th2 := executeTransfer(t, ctx, New(clientHost), carSize, req, of2)
	require.NotNil(t, th2)

	evts2 := waitForTransferComplete(th2)
	require.NotEmpty(t, evts2)
	require.Error(t, evts2[len(evts2)-1].Error)
}

// TestLibp2pCarServerResume verifies that a transfer can resume from an
// arbitrary place in the stream
func TestLibp2pCarServerResume(t *testing.T) {
	ctx := context.Background()

	rawSize := 2 * 1024 * 1024
	st := newServerTest(t, rawSize)

	clientHost, srvHost := setupLibp2pHosts(t)
	defer srvHost.Close()
	defer clientHost.Close()

	authDB := NewAuthTokenDB(st.ds)
	srv := NewLibp2pCarServer(srvHost, authDB, st.bs, ServerConfig{
		AnnounceAddr: srvHost.Addrs()[0],
	})
	err := srv.Start()
	require.NoError(t, err)
	defer srv.Stop() //nolint:errcheck

	// Create an auth token
	carSize := len(st.carBytes)
	proposalCid, err := cid.Parse("bafkqaaa")
	require.NoError(t, err)
	dbid := uint(1)
	xfer, err := srv.PrepareForDataRequest(context.Background(), dbid, proposalCid, st.root.Cid(), uint64(carSize))
	require.NoError(t, err)

	srvEvts := []types.TransferState{}
	srv.Subscribe(func(sdbid uint, st types.TransferState) {
		if dbid == sdbid {
			srvEvts = append(srvEvts, st)
		}
	})

	outFile := getTempFilePath(t)
	retrieveData := func(readCount int, of string) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Perform retrieval with the auth token
		req := newLibp2pHttpRequest(srvHost, xfer.AuthToken)
		th := executeTransfer(t, ctx, New(clientHost), carSize, req, of)
		require.NotNil(t, th)

		// Wait for some data to be received by the client
		clientSub := th.Sub()
		for i := 0; i < readCount; i++ {
			evt := <-clientSub
			require.NoError(t, evt.Error)
		}

		// Disconnect the connection
		err = clientHost.Network().ClosePeer(srvHost.ID())
		require.NoError(t, err)

		// Wait for the transfer to stall on the client
		for {
			select {
			case evt := <-clientSub:
				t.Logf("performed read till offset %d", evt.NBytesReceived)
			default:
				t.Logf("done reading")
				return
			}
		}
	}

	truncateFromEnd := func(of string, fromEnd int) []byte {
		f, err := os.Open(of)
		require.NoError(t, err)

		bz, err := io.ReadAll(f)
		require.NoError(t, err)

		err = f.Close()
		require.NoError(t, err)

		backtrack := min(len(bz), 1024)
		err = os.WriteFile(outFile, bz[:len(bz)-backtrack], 0666)
		require.NoError(t, err)

		return bz
	}

	// Perform approximately one read (may read a few more times before the
	// disconnect event propagates)
	retrieveData(1, outFile)

	// Remove the last 1k bytes from the file so that the next read will be
	// from an earlier offset
	truncateFromEnd(outFile, 1024)

	// Do the same again
	retrieveData(1, outFile)
	truncateFromEnd(outFile, 1024)

	// Now retrieve all bytes
	req := newLibp2pHttpRequest(srvHost, xfer.AuthToken)
	th := executeTransfer(t, ctx, New(clientHost), carSize, req, outFile)
	require.NotNil(t, th)

	// Wait for the transfer to complete
	clientEvts := waitForTransferComplete(th)

	// Check that all bytes were transferred successfully on the client
	require.NotEmpty(t, clientEvts)
	lastClientEvt := clientEvts[len(clientEvts)-1]
	require.EqualValues(t, carSize, lastClientEvt.NBytesReceived)
	assertFileContents(t, outFile, st.carBytes)

	// Check that all bytes were transferred successfully on the server
	require.NotEmpty(t, srvEvts)
	lastSrvEvt := srvEvts[len(srvEvts)-1]
	require.Equal(t, types.TransferStatusCompleted, lastSrvEvt.Status)
	require.EqualValues(t, carSize, int(lastSrvEvt.Sent))
}

// TestLibp2pCarServerCancelTransfer verifies that cancelling a transfer
// works as expected
func TestLibp2pCarServerCancelTransfer(t *testing.T) {
	ctx := context.Background()

	rawSize := 2 * 1024 * 1024
	st := newServerTest(t, rawSize)

	clientHost, srvHost := setupLibp2pHosts(t)
	defer srvHost.Close()
	defer clientHost.Close()

	authDB := NewAuthTokenDB(st.ds)
	srv := NewLibp2pCarServer(srvHost, authDB, st.bs, ServerConfig{
		AnnounceAddr: srvHost.Addrs()[0],
	})
	err := srv.Start()
	require.NoError(t, err)
	defer srv.Stop() //nolint:errcheck

	// Create an auth token
	carSize := len(st.carBytes)
	proposalCid, err := cid.Parse("bafkqaaa")
	require.NoError(t, err)
	dbid := uint(1)
	xfer, err := srv.PrepareForDataRequest(context.Background(), dbid, proposalCid, st.root.Cid(), uint64(carSize))
	require.NoError(t, err)

	srvEvts := []types.TransferState{}
	srv.Subscribe(func(sdbid uint, st types.TransferState) {
		if dbid == sdbid {
			srvEvts = append(srvEvts, st)
		}
	})

	// Perform retrieval with the auth token
	req := newLibp2pHttpRequest(srvHost, xfer.AuthToken)
	of := getTempFilePath(t)
	noRetry := BackOffRetryOpt(0, 0, 1, 1)
	th := executeTransfer(t, ctx, New(clientHost, noRetry), carSize, req, of)
	require.NotNil(t, th)

	// Wait for some data to be received by the client
	clientSub := th.Sub()
	evt := <-clientSub
	require.NoError(t, evt.Error)
	clientReceived := evt.NBytesReceived

	// Cancel the transfer on the server side
	var srvXfer *Libp2pTransfer
	err = srv.ForEach(func(lt *Libp2pTransfer) error {
		srvXfer = lt
		return nil
	})
	require.NoError(t, err)

	srvXfer.Cancel(fmt.Errorf("cancelled"))

	// Wait for the transfer to complete on the client
	clientEvts := waitForTransferComplete(th)
	require.NotEmpty(t, clientEvts)
	lastClientEvt := clientEvts[len(clientEvts)-1]

	// Expect not all bytes to have been transferred
	require.Error(t, lastClientEvt.Error)
	require.Less(t, int(clientReceived), carSize)

	require.NotEmpty(t, srvEvts)
	lastSrvEvt := srvEvts[len(srvEvts)-1]
	require.Equal(t, types.TransferStatusFailed, lastSrvEvt.Status)
	require.Less(t, int(lastSrvEvt.Sent), carSize)
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func newLibp2pHttpRequest(h host.Host, token string) types.HttpRequest {
	return types.HttpRequest{
		URL: "libp2p://" + h.Addrs()[0].String() + "/p2p/" + h.ID().Pretty(),
		Headers: map[string]string{
			"Authorization": BasicAuthHeader("", token),
		},
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
