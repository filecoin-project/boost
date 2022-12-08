package requestcounter_test

import (
	"testing"

	"github.com/filecoin-project/boost/cmd/booster-bitswap/requestcounter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestRequestCounter(t *testing.T) {
	peer1, err := peer.Decode("Qma9T5YraSnpRDZqRR4krcSJabThc8nwZuJV3LercPHufi")
	require.NoError(t, err)
	peer2, err := peer.Decode("QmYyQSo1c1Ym7orWxLYvCrM2EmxFTANf8wXmmE7DWjhx5N")
	require.NoError(t, err)
	peer3, err := peer.Decode("QmcfgsJsMtx6qJb74akCw1M24X1zFwgGo11h1cuhwQjtJP")
	require.NoError(t, err)
	cid1, err := cid.Parse("QmWATWQ7fVPP2EFGu71UkfnqhYXDYH566qy47CnJDgvs8u")
	require.NoError(t, err)
	cid2, err := cid.Parse("QmTn7prGSqKUd7cqvAjnULrH7zxBEBWrnj9kE7kZSGtDuQ")
	require.NoError(t, err)
	cid3, err := cid.Parse("QmajLDwZLH6bKTzd8jkq913ZbxaB2nFGRrkDAuygYNNv39")
	require.NoError(t, err)

	type op struct {
		add bool
		p   peer.ID
		c   cid.Cid
	}
	testCases := []struct {
		name                       string
		ops                        []op
		expTotalRequestsInProgress uint64
		expRequestsInProgressPeer1 uint64
		expRequestsInProgressPeer2 uint64
		expRequestsInProgressPeer3 uint64
	}{
		{
			name: "add several",
			ops: []op{
				{true, peer1, cid1}, {true, peer1, cid2}, {true, peer1, cid3},
				{true, peer2, cid1}, {true, peer2, cid2}, {true, peer2, cid3},
			},
			expTotalRequestsInProgress: 6,
			expRequestsInProgressPeer1: 3,
			expRequestsInProgressPeer2: 3,
		},
		{
			name: "add several, then take away",
			ops: []op{
				{true, peer1, cid1}, {true, peer1, cid2}, {true, peer1, cid3},
				{true, peer2, cid1}, {true, peer2, cid2}, {true, peer2, cid3},
				{false, peer1, cid1}, {false, peer1, cid2},
			},
			expTotalRequestsInProgress: 4,
			expRequestsInProgressPeer1: 1,
			expRequestsInProgressPeer2: 3,
		},
		{
			name: "add duplicate",
			ops: []op{
				{true, peer1, cid1}, {true, peer1, cid1}, {true, peer1, cid1},
				{true, peer2, cid1}, {true, peer2, cid2}, {true, peer2, cid3},
			},
			expTotalRequestsInProgress: 4,
			expRequestsInProgressPeer1: 1,
			expRequestsInProgressPeer2: 3,
		},
		{
			name: "remove duplicate",
			ops: []op{
				{true, peer1, cid1}, {true, peer1, cid2}, {true, peer1, cid3},
				{true, peer2, cid1}, {true, peer2, cid2}, {true, peer2, cid3},
				{false, peer1, cid1}, {false, peer1, cid1},
			},
			expTotalRequestsInProgress: 5,
			expRequestsInProgressPeer1: 2,
			expRequestsInProgressPeer2: 3,
		},
		{
			name: "remove non-existant",
			ops: []op{
				{true, peer1, cid1}, {true, peer1, cid2},
				{true, peer2, cid1}, {true, peer2, cid2},
				{false, peer1, cid3}, {false, peer2, cid3},
			},
			expTotalRequestsInProgress: 4,
			expRequestsInProgressPeer1: 2,
			expRequestsInProgressPeer2: 2,
		},
		{
			name: "remove empty peer",
			ops: []op{
				{true, peer1, cid1}, {true, peer1, cid2},
				{true, peer2, cid1}, {true, peer2, cid2},
				{false, peer3, cid1}, {false, peer3, cid2},
			},
			expTotalRequestsInProgress: 4,
			expRequestsInProgressPeer1: 2,
			expRequestsInProgressPeer2: 2,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			requestCounter := requestcounter.NewRequestCounter()
			for _, op := range testCase.ops {
				if op.add {
					requestCounter.AddRequest(op.p, op.c)
				} else {
					requestCounter.RemoveRequest(op.p, op.c)
				}
			}
			peer1State := requestCounter.StateForPeer(peer1)
			require.Equal(t, requestcounter.ServerState{
				TotalRequestsInProgress:   testCase.expTotalRequestsInProgress,
				RequestsInProgressForPeer: testCase.expRequestsInProgressPeer1,
			}, peer1State)
			peer2State := requestCounter.StateForPeer(peer2)
			require.Equal(t, requestcounter.ServerState{
				TotalRequestsInProgress:   testCase.expTotalRequestsInProgress,
				RequestsInProgressForPeer: testCase.expRequestsInProgressPeer2,
			}, peer2State)
			peer3State := requestCounter.StateForPeer(peer3)
			require.Equal(t, requestcounter.ServerState{
				TotalRequestsInProgress:   testCase.expTotalRequestsInProgress,
				RequestsInProgressForPeer: testCase.expRequestsInProgressPeer3,
			}, peer3State)

		})
	}
}
