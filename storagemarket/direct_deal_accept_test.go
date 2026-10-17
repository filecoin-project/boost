package storagemarket

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/network"

	ltypes "github.com/filecoin-project/lotus/chain/types"
)

// tipsetAt is the chain head Accept sees. The height is the only field it reads
// from the head, but the header still has to hash, so it goes through the same
// mockTipset the rest of the package uses.
func tipsetAt(t *testing.T, height abi.ChainEpoch) *ltypes.TipSet {
	ts, err := mockTipset(address.TestAddress, height)
	require.NoError(t, err)
	return ts
}

// TestRejectedAtNv29 pins the comparison itself: the gate is >=, not ==, so a
// future version must not slip past it.
func TestRejectedAtNv29(t *testing.T) {
	require.False(t, RejectedAtNv29(network.Version28))
	require.True(t, RejectedAtNv29(network.Version29))
	require.True(t, RejectedAtNv29(network.Version(30)))
}

// TestAcceptRejectsDirectDealsAtNv29 pins the gate that makes every other nv29
// case in this package a legacy deal: from nv29 on boost accepts no direct deal
// at all, because FIP-0118 deprecates the datacap the flow is built on. A deal
// entering before the upgrade and landing after it never passes through here
// again, so this is the only place the refusal is decided.
func TestAcceptRejectsDirectDealsAtNv29(t *testing.T) {
	for _, tt := range []struct {
		name string
		nv   network.Version
	}{
		{"nv29", network.Version29},
		{"a later version", network.Version(30)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			nv := tt.nv

			f := newChainFixture(t)
			f.node.EXPECT().ChainHead(gomock.Any()).Return(tipsetAt(t, 100), nil)
			f.node.EXPECT().StateNetworkVersion(gomock.Any(), gomock.Any()).Return(nv, nil)
			// The refusal must land before any nv28-era read: no allocation
			// lookup, no deal-DB work.
			f.node.EXPECT().StateGetAllocation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

			rej, err := f.ddp.Accept(context.Background(), f.deal())

			require.NoError(t, err)
			require.NotNil(t, rej)
			require.False(t, rej.Accepted)
			require.Equal(t, DirectDealRejectionAtNv29, rej.Reason)
		})
	}
}

// TestAcceptBelowNv29ReachesTheAllocationCheck is the other side of the gate: at
// nv28 the deal is judged on its merits, so it has to get past the version check
// and reach the reads that actually decide. Without this row a gate stuck open
// (or shut) would still pass the rejection test above.
func TestAcceptBelowNv29ReachesTheAllocationCheck(t *testing.T) {
	f := newChainFixture(t)
	f.node.EXPECT().ChainHead(gomock.Any()).Return(tipsetAt(t, 100), nil)
	f.node.EXPECT().StateNetworkVersion(gomock.Any(), gomock.Any()).Return(network.Version28, nil)
	f.node.EXPECT().StateGetAllocation(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)

	entry := f.deal()
	entry.StartEpoch = 1000 // far from the head at 100, so the epoch check passes too

	rej, err := f.ddp.Accept(context.Background(), entry)

	require.NoError(t, err)
	require.NotNil(t, rej)
	require.False(t, rej.Accepted)
	require.NotEqual(t, DirectDealRejectionAtNv29, rej.Reason)
	require.Contains(t, rej.Reason, "not found")
}

// TestAcceptFailsWhenTheNetworkVersionCannotBeRead: a version that could not be
// read is not a verdict. Returning a rejection here would refuse every deal for
// as long as the node is behind, and the deal would never be retried.
func TestAcceptFailsWhenTheNetworkVersionCannotBeRead(t *testing.T) {
	f := newChainFixture(t)
	f.node.EXPECT().ChainHead(gomock.Any()).Return(tipsetAt(t, 100), nil)
	f.node.EXPECT().StateNetworkVersion(gomock.Any(), gomock.Any()).Return(network.Version(0), errors.New("node behind"))

	rej, err := f.ddp.Accept(context.Background(), f.deal())

	require.Error(t, err)
	require.Nil(t, rej)
}
