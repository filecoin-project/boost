package storagemarket

import (
	"context"
	"testing"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/stretchr/testify/require"
)

func TestDealCompletionOnProcessResumption(t *testing.T) {
	ctx := context.Background()

	tcs := map[string]struct {
		dealBuilderF                  func(h *ProviderHarness) *testDeal
		waitForAndAssertBeforeResumeF func(t *testing.T, h *ProviderHarness, td *testDeal)
		stubAfterResumeF              func(tb *testDealBuilder) *testDeal
		unblockF                      func(td *testDeal)
	}{
		"resume after accepting": {
			dealBuilderF: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withBlockingHttpServer().build()
			},
			stubAfterResumeF: func(tb *testDealBuilder) *testDeal {
				return tb.withCommpNonBlocking().withPublishNonBlocking().withPublishConfirmNonBlocking().withAddPieceNonBlocking().withAnnounceNonBlocking().build()
			},
			unblockF: func(td *testDeal) {
				td.unblockTransfer()
			},
			waitForAndAssertBeforeResumeF: func(t *testing.T, h *ProviderHarness, td *testDeal) {
				td.waitForAndAssert(t, ctx, dealcheckpoints.Accepted)
				h.EventuallyAssertStorageFundState(t, ctx, td.params.Transfer.Size, h.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)
			},
		},
		"resume after finishing transfer": {
			dealBuilderF: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withCommpBlocking(true).withNormalHttpServer().build()
			},
			stubAfterResumeF: func(tb *testDealBuilder) *testDeal {
				return tb.withAllMinerCallsNonBlocking().build()
			},
			waitForAndAssertBeforeResumeF: func(t *testing.T, h *ProviderHarness, td *testDeal) {
				td.waitForAndAssert(t, ctx, dealcheckpoints.Accepted)
				h.EventuallyAssertStorageFundState(t, ctx, td.params.Transfer.Size, h.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)
			},
		},
		"resume after finishing commp": {
			dealBuilderF: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withCommpNonBlocking().withPublishBlocking().withNormalHttpServer().build()
			},
			stubAfterResumeF: func(tb *testDealBuilder) *testDeal {
				return tb.withPublishNonBlocking().withPublishConfirmNonBlocking().withAddPieceNonBlocking().withAnnounceNonBlocking().build()
			},
			waitForAndAssertBeforeResumeF: func(t *testing.T, h *ProviderHarness, td *testDeal) {
				td.waitForAndAssert(t, ctx, dealcheckpoints.Transferred)
				h.EventuallyAssertStorageFundState(t, ctx, td.params.Transfer.Size, h.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)
			},
		},
		"resume after publishing": {
			dealBuilderF: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withCommpNonBlocking().withPublishNonBlocking().withPublishConfirmBlocking().withNormalHttpServer().build()
			},
			stubAfterResumeF: func(tb *testDealBuilder) *testDeal {
				return tb.withPublishConfirmNonBlocking().withAddPieceNonBlocking().withAnnounceNonBlocking().build()
			},
			waitForAndAssertBeforeResumeF: func(t *testing.T, h *ProviderHarness, td *testDeal) {
				td.waitForAndAssert(t, ctx, dealcheckpoints.Published)
				h.EventuallyAssertStorageFundState(t, ctx, td.params.Transfer.Size, h.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)
			},
		},
		"resume after confirming publish": {
			dealBuilderF: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withCommpNonBlocking().withPublishNonBlocking().withPublishConfirmNonBlocking().withAddPieceBlocking().withNormalHttpServer().build()
			},
			stubAfterResumeF: func(tb *testDealBuilder) *testDeal {
				return tb.withAddPieceNonBlocking().withAnnounceNonBlocking().build()
			},
			waitForAndAssertBeforeResumeF: func(t *testing.T, h *ProviderHarness, td *testDeal) {
				td.waitForAndAssert(t, ctx, dealcheckpoints.PublishConfirmed)
				h.EventuallyAssertStorageFundState(t, ctx, td.params.Transfer.Size, abi.NewTokenAmount(0), abi.NewTokenAmount(0))
			},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			// setup the provider test harness
			harness := NewHarness(t)
			// start the provider test harness
			harness.Start(t, ctx)
			defer harness.Stop()

			// start executing the deal
			td := tc.dealBuilderF(harness)
			require.NoError(t, td.executeAndSubscribe())

			// wait for state after which to resume and assert funds and storage
			tc.waitForAndAssertBeforeResumeF(t, harness, td)

			// shutdown the existing provider and create a new provider
			harness.shutdownAndCreateNewProvider(t)

			// update the test deal state with the new provider
			tbuilder := td.updateWithRestartedProvider(harness)
			td = tc.stubAfterResumeF(tbuilder)

			// start the provider -> this will restart the deal
			err := harness.Provider.Start()
			require.NoError(t, err)
			dh := harness.Provider.getDealHandler(td.params.DealUUID)
			require.NotNil(t, dh)
			sub, err := dh.subscribeUpdates()
			require.NoError(t, err)
			td.sub = sub

			// update subscription and mock assertions as provider has restarted
			if tc.unblockF != nil {
				tc.unblockF(td)
			}

			td.waitForAndAssert(t, ctx, dealcheckpoints.AddedPiece)
			// assert funds and storage are no longer tagged
			harness.EventuallyAssertNoTagged(t, ctx)
		})
	}
}
