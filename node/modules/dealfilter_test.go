package modules

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	acrypto "github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/boost/storagemarket/dealfilter"
	"github.com/filecoin-project/boost/storagemarket/types"

	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	chaintypes "github.com/filecoin-project/lotus/chain/types"

	markettypes "github.com/filecoin-project/go-state-types/builtin/v9/market"
)

// TestBasicDealFilterVerifiedSettingsAtNv29 covers what FIP-0118 does to the two
// settings that sort verified traffic. From nv29 no deal is verified, so the
// common FIL+-only configuration -- verified on, unverified off -- would reject
// every deal unless the two are read as the one switch that is left.
func TestBasicDealFilterVerifiedSettingsAtNv29(t *testing.T) {
	tests := map[string]struct {
		nv                 network.Version
		verifiedDeal       bool
		considerVerified   bool
		considerUnverified bool
		wantAccept         bool
		wantReason         string
	}{
		"before nv29 an unverified deal is still turned away when unverified is off": {
			nv:                 network.Version28,
			considerVerified:   true,
			considerUnverified: false,
			wantReason:         "miner is not accepting unverified storage deals",
		},
		"before nv29 a verified deal is still turned away when verified is off": {
			nv:                 network.Version28,
			verifiedDeal:       true,
			considerVerified:   false,
			considerUnverified: true,
			wantReason:         "miner is not accepting verified storage deals",
		},
		"at nv29 a FIL+-only miner keeps accepting deals": {
			// Every deal past nv29 is unverified, and this miner has unverified off.
			nv:                 network.Version29,
			considerVerified:   true,
			considerUnverified: false,
			wantAccept:         true,
		},
		"at nv29 a miner accepting both kinds is unaffected": {
			// The configuration the upgrade did not change.
			nv:                 network.Version29,
			considerVerified:   true,
			considerUnverified: true,
			wantAccept:         true,
		},
		"at nv29 a miner accepting unverified deals is unaffected": {
			nv:                 network.Version29,
			considerVerified:   false,
			considerUnverified: true,
			wantAccept:         true,
		},
		"at nv29 a miner accepting neither still accepts nothing": {
			nv:         network.Version29,
			wantReason: "miner is not accepting storage deals",
		},
		"before nv29 a miner accepting neither still accepts nothing": {
			// The pre-nv29 branch keeps its behaviour verbatim.
			nv:         network.Version28,
			wantReason: "miner is not accepting unverified storage deals",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			fullNode := lotusmocks.NewMockFullNode(ctrl)

			maddr, err := address.NewIDAddress(1000)
			require.NoError(t, err)

			head, err := mockTipset(maddr, 100)
			require.NoError(t, err)

			fullNode.EXPECT().StateNetworkVersion(gomock.Any(), gomock.Any()).Return(tc.nv, nil).AnyTimes()
			fullNode.EXPECT().ChainHead(gomock.Any()).Return(head, nil).AnyTimes()

			filter := BasicDealFilter(nil)(
				func() (bool, error) { return true, nil },
				func() (bool, error) { return true, nil },
				func() (bool, error) { return tc.considerVerified, nil },
				func() (bool, error) { return tc.considerUnverified, nil },
				func() ([]cid.Cid, error) { return nil, nil },
				func() (time.Duration, error) { return 0, nil },
				func() (time.Duration, error) { return time.Hour, nil },
				fullNode,
				nil,
			)

			accept, reason, err := filter(context.Background(), dealFilterParams(t, maddr, tc.verifiedDeal))
			require.NoError(t, err)
			require.Equal(t, tc.wantAccept, accept, "reason: %s", reason)
			if !tc.wantAccept {
				require.Equal(t, tc.wantReason, reason)
			}
		})
	}
}

// TestBasicDealFilterPassesNetworkVersionToUserFilter checks that the external
// filter is told the network version: it cannot ask the chain, and past nv29 it
// would otherwise see every deal turn unverified with nothing to explain it.
func TestBasicDealFilterPassesNetworkVersionToUserFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	fullNode := lotusmocks.NewMockFullNode(ctrl)

	maddr, err := address.NewIDAddress(1000)
	require.NoError(t, err)

	head, err := mockTipset(maddr, 100)
	require.NoError(t, err)

	fullNode.EXPECT().StateNetworkVersion(gomock.Any(), gomock.Any()).Return(network.Version29, nil).AnyTimes()
	fullNode.EXPECT().ChainHead(gomock.Any()).Return(head, nil).AnyTimes()

	var seen network.Version
	userCmd := func(_ context.Context, params dealfilter.DealFilterParams) (bool, string, error) {
		seen = params.NetworkVersion
		return true, "", nil
	}

	filter := BasicDealFilter(userCmd)(
		func() (bool, error) { return true, nil },
		func() (bool, error) { return true, nil },
		func() (bool, error) { return true, nil },
		func() (bool, error) { return true, nil },
		func() ([]cid.Cid, error) { return nil, nil },
		func() (time.Duration, error) { return 0, nil },
		func() (time.Duration, error) { return time.Hour, nil },
		fullNode,
		nil,
	)

	accept, _, err := filter(context.Background(), dealFilterParams(t, maddr, false))
	require.NoError(t, err)
	require.True(t, accept)
	require.Equal(t, network.Version29, seen)
}

// TestBasicDealFilterFailsClosedWithoutTheChainVerdict covers the check the
// collapse depends on: which era a deal is arriving in is the chain's answer, so
// a node that cannot get it must not guess either way.
func TestBasicDealFilterFailsClosedWithoutTheChainVerdict(t *testing.T) {
	for name, nv := range map[string]network.Version{"before nv29": network.Version28, "at nv29": network.Version29} {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			fullNode := lotusmocks.NewMockFullNode(ctrl)

			maddr, err := address.NewIDAddress(1000)
			require.NoError(t, err)

			// The call failed, so the version it would have reported is not a verdict.
			fullNode.EXPECT().StateNetworkVersion(gomock.Any(), gomock.Any()).
				Return(nv, errors.New("node is not synced")).AnyTimes()

			filter := BasicDealFilter(nil)(
				func() (bool, error) { return true, nil },
				func() (bool, error) { return true, nil },
				func() (bool, error) { return true, nil },
				func() (bool, error) { return true, nil },
				func() ([]cid.Cid, error) { return nil, nil },
				func() (time.Duration, error) { return 0, nil },
				func() (time.Duration, error) { return time.Hour, nil },
				fullNode,
				nil,
			)

			accept, _, err := filter(context.Background(), dealFilterParams(t, maddr, false))

			require.Error(t, err, "the chain could not be asked, so the filter has no verdict to give")
			require.False(t, accept)
		})
	}
}

// dealFilterParams builds the smallest deal only the settings under test can turn away.
func dealFilterParams(t *testing.T, client address.Address, verified bool) dealfilter.DealFilterParams {
	t.Helper()

	pieceCid, err := cid.Parse("baga6ea4seaqjtovkwk4myyzj56eztkh5pzsk5upksan6f5outesy62bsvl4dsha")
	require.NoError(t, err)

	return dealfilter.DealFilterParams{
		DealParams: types.DealParams{
			ClientDealProposal: markettypes.ClientDealProposal{
				Proposal: markettypes.DealProposal{
					PieceCID:     pieceCid,
					Client:       client,
					VerifiedDeal: verified,
					// Inside the window the seal-time and start-delay checks allow.
					StartEpoch: 200,
					EndEpoch:   500,
				},
			},
		},
	}
}

// mockTipset is the chain head the seal-time check reads the current epoch from.
func mockTipset(minerAddr address.Address, height abi.ChainEpoch) (*chaintypes.TipSet, error) {
	dummyCid, err := cid.Parse("bafkqaaa")
	if err != nil {
		return nil, err
	}
	return chaintypes.NewTipSet([]*chaintypes.BlockHeader{{
		Miner:                 minerAddr,
		Ticket:                &chaintypes.Ticket{VRFProof: []byte("ticket")},
		Height:                height,
		ParentStateRoot:       dummyCid,
		Messages:              dummyCid,
		ParentMessageReceipts: dummyCid,
		BlockSig:              &acrypto.Signature{Type: acrypto.SigTypeBLS},
		BLSAggregate:          &acrypto.Signature{Type: acrypto.SigTypeBLS},
		Timestamp:             1,
	}})
}
