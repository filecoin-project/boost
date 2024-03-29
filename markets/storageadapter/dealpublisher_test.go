// stm: #unit
package storageadapter

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	cborutil "github.com/filecoin-project/go-cbor-util"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/raulk/clock"
	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	markettypes "github.com/filecoin-project/go-state-types/builtin/v9/market"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	tutils "github.com/filecoin-project/specs-actors/v2/support/testing"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/types"
)

func TestDealPublisher(t *testing.T) {
	//stm: @MARKET_DEAL_PUBLISHER_PUBLISH_001, @MARKET_DEAL_PUBLISHER_GET_PENDING_DEALS_001
	oldClock := build.Clock
	t.Cleanup(func() { build.Clock = oldClock })
	mc := clock.NewMock()
	build.Clock = mc

	testCases := []struct {
		name                            string
		publishPeriod                   time.Duration
		maxDealsPerMsg                  uint64
		dealCountWithinPublishPeriod    int
		ctxCancelledWithinPublishPeriod int
		expiredDeals                    int
		dealCountAfterPublishPeriod     int
		expectedDealsPerMsg             []int
		failOne                         bool
	}{{
		name:                         "publish one deal within publish period",
		publishPeriod:                10 * time.Millisecond,
		maxDealsPerMsg:               5,
		dealCountWithinPublishPeriod: 1,
		dealCountAfterPublishPeriod:  0,
		expectedDealsPerMsg:          []int{1},
	}, {
		name:                         "publish two deals within publish period",
		publishPeriod:                10 * time.Millisecond,
		maxDealsPerMsg:               5,
		dealCountWithinPublishPeriod: 2,
		dealCountAfterPublishPeriod:  0,
		expectedDealsPerMsg:          []int{2},
	}, {
		name:                         "publish one deal within publish period, and one after",
		publishPeriod:                10 * time.Millisecond,
		maxDealsPerMsg:               5,
		dealCountWithinPublishPeriod: 1,
		dealCountAfterPublishPeriod:  1,
		expectedDealsPerMsg:          []int{1, 1},
	}, {
		name:                         "publish deals that exceed max deals per message within publish period, and one after",
		publishPeriod:                10 * time.Millisecond,
		maxDealsPerMsg:               2,
		dealCountWithinPublishPeriod: 3,
		dealCountAfterPublishPeriod:  1,
		expectedDealsPerMsg:          []int{2, 1, 1},
	}, {
		name:                            "ignore deals with cancelled context",
		publishPeriod:                   10 * time.Millisecond,
		maxDealsPerMsg:                  5,
		dealCountWithinPublishPeriod:    2,
		ctxCancelledWithinPublishPeriod: 2,
		dealCountAfterPublishPeriod:     1,
		expectedDealsPerMsg:             []int{2, 1},
	}, {
		name:                         "ignore expired deals",
		publishPeriod:                10 * time.Millisecond,
		maxDealsPerMsg:               5,
		dealCountWithinPublishPeriod: 2,
		expiredDeals:                 2,
		dealCountAfterPublishPeriod:  1,
		expectedDealsPerMsg:          []int{2, 1},
	}, {
		name:                            "zero config",
		publishPeriod:                   0,
		maxDealsPerMsg:                  0,
		dealCountWithinPublishPeriod:    2,
		ctxCancelledWithinPublishPeriod: 0,
		dealCountAfterPublishPeriod:     2,
		expectedDealsPerMsg:             []int{1, 1, 1, 1},
	}, {
		name:                         "one deal failing doesn't fail the entire batch",
		publishPeriod:                10 * time.Millisecond,
		maxDealsPerMsg:               5,
		dealCountWithinPublishPeriod: 2,
		dealCountAfterPublishPeriod:  0,
		failOne:                      true,
		expectedDealsPerMsg:          []int{1},
	}}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mc.Set(time.Now())
			dpapi := newDPAPI(t)

			// Create a deal publisher
			dp := newDealPublisher(dpapi, nil, PublishMsgConfig{
				Period:         tc.publishPeriod,
				MaxDealsPerMsg: tc.maxDealsPerMsg,
			}, &api.MessageSendSpec{MaxFee: abi.NewTokenAmount(1)})

			// Keep a record of the deals that were submitted to be published
			var dealsToPublish []markettypes.ClientDealProposal

			// Publish deals within publish period
			for i := 0; i < tc.dealCountWithinPublishPeriod; i++ {
				if tc.failOne && i == 1 {
					publishDeal(t, dp, i, false, false)
				} else {
					deal := publishDeal(t, dp, 0, false, false)
					dealsToPublish = append(dealsToPublish, deal)
				}
			}
			for i := 0; i < tc.ctxCancelledWithinPublishPeriod; i++ {
				publishDeal(t, dp, 0, true, false)
			}
			for i := 0; i < tc.expiredDeals; i++ {
				publishDeal(t, dp, 0, false, true)
			}

			// Wait until publish period has elapsed
			if tc.publishPeriod > 0 {
				// If we expect deals to get stuck in the queue, wait until that happens
				if tc.maxDealsPerMsg != 0 && tc.dealCountWithinPublishPeriod%int(tc.maxDealsPerMsg) != 0 {
					require.Eventually(t, func() bool {
						dp.lk.Lock()
						defer dp.lk.Unlock()
						return !dp.publishPeriodStart.IsZero()
					}, time.Second, time.Millisecond, "failed to queue deals")
				}

				// Then wait to send
				require.Eventually(t, func() bool {
					dp.lk.Lock()
					defer dp.lk.Unlock()

					// Advance if necessary.
					if mc.Since(dp.publishPeriodStart) <= tc.publishPeriod {
						dp.lk.Unlock()
						mc.Set(dp.publishPeriodStart.Add(tc.publishPeriod + 1))
						dp.lk.Lock()
					}

					return len(dp.pending) == 0
				}, time.Second, time.Millisecond, "failed to send pending messages")
			}

			// Publish deals after publish period
			for i := 0; i < tc.dealCountAfterPublishPeriod; i++ {
				deal := publishDeal(t, dp, 0, false, false)
				dealsToPublish = append(dealsToPublish, deal)
			}

			if tc.publishPeriod > 0 && tc.dealCountAfterPublishPeriod > 0 {
				require.Eventually(t, func() bool {
					dp.lk.Lock()
					defer dp.lk.Unlock()
					if mc.Since(dp.publishPeriodStart) <= tc.publishPeriod {
						dp.lk.Unlock()
						mc.Set(dp.publishPeriodStart.Add(tc.publishPeriod + 1))
						dp.lk.Lock()
					}
					return len(dp.pending) == 0
				}, time.Second, time.Millisecond, "failed to send pending messages")
			}

			checkPublishedDeals(t, dpapi, dealsToPublish, tc.expectedDealsPerMsg)
		})
	}
}

func TestForcePublish(t *testing.T) {
	//stm: @MARKET_DEAL_PUBLISHER_PUBLISH_001, @MARKET_DEAL_PUBLISHER_GET_PENDING_DEALS_001
	//stm: @MARKET_DEAL_PUBLISHER_FORCE_PUBLISH_ALL_001
	dpapi := newDPAPI(t)

	// Create a deal publisher
	start := build.Clock.Now()
	publishPeriod := time.Hour
	dp := newDealPublisher(dpapi, nil, PublishMsgConfig{
		Period:         publishPeriod,
		MaxDealsPerMsg: 10,
	}, &api.MessageSendSpec{MaxFee: abi.NewTokenAmount(1)})

	// Queue three deals for publishing, one with a cancelled context
	var dealsToPublish []markettypes.ClientDealProposal
	// 1. Regular deal
	deal := publishDeal(t, dp, 0, false, false)
	dealsToPublish = append(dealsToPublish, deal)
	// 2. Deal with cancelled context
	publishDeal(t, dp, 0, true, false)
	// 3. Regular deal
	deal = publishDeal(t, dp, 0, false, false)
	dealsToPublish = append(dealsToPublish, deal)

	// Allow a moment for them to be queued
	build.Clock.Sleep(10 * time.Millisecond)

	// Should be two deals in the pending deals list
	// (deal with cancelled context is ignored)
	pendingInfo := dp.PendingDeals()
	require.Len(t, pendingInfo.Deals, 2)
	require.Equal(t, publishPeriod, pendingInfo.PublishPeriod)
	require.True(t, pendingInfo.PublishPeriodStart.After(start))
	require.True(t, pendingInfo.PublishPeriodStart.Before(build.Clock.Now()))

	// Force publish all pending deals
	dp.ForcePublishPendingDeals()

	// Should be no pending deals
	pendingInfo = dp.PendingDeals()
	require.Len(t, pendingInfo.Deals, 0)

	// Make sure the expected deals were published
	checkPublishedDeals(t, dpapi, dealsToPublish, []int{2})
}

func TestPublishPendingDeals(t *testing.T) {
	dpapi := newDPAPI(t)

	// Create a deal publisher
	publishPeriod := time.Hour
	dp := newDealPublisher(dpapi, nil, PublishMsgConfig{
		Period:            publishPeriod,
		MaxDealsPerMsg:    10,
		ManualDealPublish: true,
	}, &api.MessageSendSpec{MaxFee: abi.NewTokenAmount(1)})

	// Queue three deals for publishing, one with a cancelled context
	// 1. Regular deal
	publishDeal(t, dp, 0, false, false)
	// 2. Deal with cancelled context
	publishDeal(t, dp, 0, true, false)
	// 3. Regular deal
	publishDeal(t, dp, 0, false, false)
	// 4. Regular deal
	publishDeal(t, dp, 0, false, false)

	// Allow a moment for them to be queued
	build.Clock.Sleep(10 * time.Millisecond)

	// Should be three deals in the pending deals list
	// (deal with cancelled context is ignored)
	pendingInfo := dp.PendingDeals()
	require.Len(t, pendingInfo.Deals, 3)

	var pcids []cid.Cid
	props := pendingInfo.Deals
	for _, p := range props {
		signedProp, err := cborutil.AsIpld(&p)
		require.NoError(t, err)
		pcids = append(pcids, signedProp.Cid())
	}

	toPublish := pcids[1:]
	pending := []cid.Cid{pcids[0]}

	// Send an additional CID not present in publisher
	c, err := cid.Decode("bafy2bzacea3wsdh6y3a36tb3skempjoxqpuyompjbmfeyf34fi3uy6uue42v4")
	require.NoError(t, err)

	// Publish three pending deals and verify all deals whose context has not expired have been published
	publishedDeals := dp.PublishQueuedDeals(append(toPublish, c))
	require.Equal(t, toPublish, publishedDeals)

	// Should be one remaining pending deal
	pendingInfo1 := dp.PendingDeals()
	var ppcids []cid.Cid
	require.Len(t, pendingInfo1.Deals, 1)
	for _, p := range pendingInfo1.Deals {
		signedProp, err := cborutil.AsIpld(&p)
		require.NoError(t, err)
		ppcids = append(ppcids, signedProp.Cid())
	}
	require.Equal(t, pending, ppcids)

	// Make sure the expected deals were published
	checkPublishedDeals(t, dpapi, props[1:], []int{2})
}

func publishDeal(t *testing.T, dp *DealPublisher, invalid int, ctxCancelled bool, expired bool) markettypes.ClientDealProposal {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	pctx := ctx
	if ctxCancelled {
		pctx, cancel = context.WithCancel(ctx)
		cancel()
	}

	startEpoch := abi.ChainEpoch(20)
	if expired {
		startEpoch = abi.ChainEpoch(5)
	}
	deal := markettypes.ClientDealProposal{
		Proposal: markettypes.DealProposal{
			PieceCID:   generateCids(1)[0],
			Client:     getClientActor(t),
			Provider:   getProviderActor(t),
			StartEpoch: startEpoch,
			EndEpoch:   abi.ChainEpoch(120),
			PieceSize:  abi.PaddedPieceSize(invalid), // pass invalid into StateCall below
		},
		ClientSignature: crypto.Signature{
			Type: crypto.SigTypeSecp256k1,
			Data: []byte("signature data"),
		},
	}

	go func() {
		_, err := dp.Publish(pctx, deal)

		// If the test has completed just bail out without checking for errors
		if ctx.Err() != nil {
			return
		}

		if ctxCancelled || expired || invalid == 1 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}()

	return deal
}

func checkPublishedDeals(t *testing.T, dpapi *dpAPI, dealsToPublish []markettypes.ClientDealProposal, expectedDealsPerMsg []int) {
	// For each message that was expected to be sent
	var publishedDeals []markettypes.ClientDealProposal
	for _, expectedDealsInMsg := range expectedDealsPerMsg {
		// Should have called StateMinerInfo with the provider address
		stateMinerInfoAddr := <-dpapi.stateMinerInfoCalls
		require.Equal(t, getProviderActor(t), stateMinerInfoAddr)

		// Check the fields of the message that was sent
		msg := <-dpapi.pushedMsgs
		require.Equal(t, getWorkerActor(t), msg.From)
		require.Equal(t, market.Address, msg.To)
		require.Equal(t, market.Methods.PublishStorageDeals, msg.Method)

		// Check that the expected number of deals was included in the message
		var params markettypes.PublishStorageDealsParams
		err := params.UnmarshalCBOR(bytes.NewReader(msg.Params))
		require.NoError(t, err)
		require.Len(t, params.Deals, expectedDealsInMsg)

		// Keep track of the deals that were sent
		publishedDeals = append(publishedDeals, params.Deals...)
	}

	// Verify that all deals that were submitted to be published were
	// sent out (we do this by ensuring all the piece CIDs are present)
	require.True(t, matchPieceCids(publishedDeals, dealsToPublish))
}

func matchPieceCids(sent []markettypes.ClientDealProposal, exp []markettypes.ClientDealProposal) bool {
	cidsA := dealPieceCids(sent)
	cidsB := dealPieceCids(exp)

	if len(cidsA) != len(cidsB) {
		return false
	}

	s1 := cid.NewSet()
	for _, c := range cidsA {
		s1.Add(c)
	}

	for _, c := range cidsB {
		if !s1.Has(c) {
			return false
		}
	}

	return true
}

func dealPieceCids(deals []markettypes.ClientDealProposal) []cid.Cid {
	cids := make([]cid.Cid, 0, len(deals))
	for _, dl := range deals {
		cids = append(cids, dl.Proposal.PieceCID)
	}
	return cids
}

type dpAPI struct {
	t      *testing.T
	worker address.Address

	stateMinerInfoCalls chan address.Address
	pushedMsgs          chan *types.Message
}

func newDPAPI(t *testing.T) *dpAPI {
	return &dpAPI{
		t:                   t,
		worker:              getWorkerActor(t),
		stateMinerInfoCalls: make(chan address.Address, 128),
		pushedMsgs:          make(chan *types.Message, 128),
	}
}

func (d *dpAPI) ChainHead(ctx context.Context) (*types.TipSet, error) {
	dummyCid, err := cid.Parse("bafkqaaa")
	require.NoError(d.t, err)
	return types.NewTipSet([]*types.BlockHeader{{
		Miner:                 tutils.NewActorAddr(d.t, "miner"),
		Height:                abi.ChainEpoch(10),
		ParentStateRoot:       dummyCid,
		Messages:              dummyCid,
		ParentMessageReceipts: dummyCid,
		BlockSig:              &crypto.Signature{Type: crypto.SigTypeBLS},
		BLSAggregate:          &crypto.Signature{Type: crypto.SigTypeBLS},
	}})
}

func (d *dpAPI) StateMinerInfo(ctx context.Context, address address.Address, key types.TipSetKey) (api.MinerInfo, error) {
	d.stateMinerInfoCalls <- address
	return api.MinerInfo{Worker: d.worker}, nil
}

func (d *dpAPI) MpoolPushMessage(ctx context.Context, msg *types.Message, spec *api.MessageSendSpec) (*types.SignedMessage, error) {
	d.pushedMsgs <- msg
	return &types.SignedMessage{Message: *msg}, nil
}

func (d *dpAPI) WalletBalance(ctx context.Context, a address.Address) (types.BigInt, error) {
	panic("don't call me")
}

func (d *dpAPI) WalletHas(ctx context.Context, a address.Address) (bool, error) {
	panic("don't call me")
}

func (d *dpAPI) StateAccountKey(ctx context.Context, a address.Address, key types.TipSetKey) (address.Address, error) {
	panic("don't call me")
}

func (d *dpAPI) StateLookupID(ctx context.Context, a address.Address, key types.TipSetKey) (address.Address, error) {
	panic("don't call me")
}

func (d *dpAPI) StateCall(ctx context.Context, message *types.Message, key types.TipSetKey) (*api.InvocResult, error) {
	var p markettypes.PublishStorageDealsParams
	if err := p.UnmarshalCBOR(bytes.NewReader(message.Params)); err != nil {
		return nil, xerrors.Errorf("unmarshal market params: %w", err)
	}

	exit := exitcode.Ok
	if p.Deals[0].Proposal.PieceSize == 1 {
		exit = exitcode.ErrIllegalState
	}
	return &api.InvocResult{MsgRct: &types.MessageReceipt{ExitCode: exit}}, nil
}

func getClientActor(t *testing.T) address.Address {
	return tutils.NewActorAddr(t, "client")
}

func getWorkerActor(t *testing.T) address.Address {
	return tutils.NewActorAddr(t, "worker")
}

func getProviderActor(t *testing.T) address.Address {
	return tutils.NewActorAddr(t, "provider")
}

var seq int

func generateCids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := blocks.NewBlock([]byte(fmt.Sprint(seq))).Cid()
		seq++
		cids = append(cids, c)
	}
	return cids
}
