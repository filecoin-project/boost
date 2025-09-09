package storagemarket

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/filecoin-project/boost/db"
	bdclientutil "github.com/filecoin-project/boost/extern/boostd-data/clientutil"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket/dealfilter"
	"github.com/filecoin-project/boost/storagemarket/logs"
	mock_sealingpipeline "github.com/filecoin-project/boost/storagemarket/sealingpipeline/mock"
	"github.com/filecoin-project/boost/storagemarket/smtestutil"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/boost/transport"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/boost/transport/mocks"
	tspttypes "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v12/miner"
	"github.com/filecoin-project/go-state-types/builtin/v9/market"
	acrypto "github.com/filecoin-project/go-state-types/crypto"
	lapi "github.com/filecoin-project/lotus/api"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	"github.com/filecoin-project/lotus/build"
	test "github.com/filecoin-project/lotus/chain/events/state/mock"
	chaintypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/repo"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestSimpleDealHappy(t *testing.T) {
	runTest := func(t *testing.T, localCommp bool, carVersion CarVersion) {
		ctx := context.Background()

		// setup the provider test harness
		opts := []harnessOpt{}
		if localCommp {
			opts = append(opts, withLocalCommp())
		}
		harness := NewHarness(t, opts...)
		// start the provider test harness
		harness.Start(t, ctx)
		defer harness.Stop()

		// build the deal proposal with the blocking http test server and a completely blocking miner stub
		tdBuilder := harness.newDealBuilder(t, 1, withCarVersion(carVersion)).withBlockingHttpServer()
		if localCommp {
			// if commp is calculated locally, don't expect a remote call to commp
			// (expect calls to all other miner APIs)
			tdBuilder.withPublishBlocking().withPublishConfirmBlocking().withAddPieceBlocking().withAnnounceBlocking()
		} else {
			tdBuilder.withAllMinerCallsBlocking()
		}
		td := tdBuilder.build()

		// execute deal
		require.NoError(t, td.executeAndSubscribe())

		// wait for Accepted checkpoint
		td.waitForAndAssert(t, ctx, dealcheckpoints.Accepted)

		// unblock transfer & commp -> wait for Transferred checkpoint and assert deals db and storage and fund manager
		td.unblockTransfer()
		if !localCommp {
			td.unblockCommp()
		}
		td.waitForAndAssert(t, ctx, dealcheckpoints.Transferred)
		harness.AssertStorageAndFundManagerState(t, ctx, td.params.Transfer.Size, harness.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)

		// unblock publish -> wait for published checkpoint and assert
		td.unblockPublish()
		td.waitForAndAssert(t, ctx, dealcheckpoints.Published)
		harness.AssertStorageAndFundManagerState(t, ctx, td.params.Transfer.Size, harness.MinPublishFees, td.params.ClientDealProposal.Proposal.ProviderCollateral)

		// unblock publish confirmation -> wait for publish confirmed and assert
		td.unblockWaitForPublish()
		td.waitForAndAssert(t, ctx, dealcheckpoints.PublishConfirmed)
		harness.EventuallyAssertStorageFundState(t, ctx, td.params.Transfer.Size, abi.NewTokenAmount(0), abi.NewTokenAmount(0))

		// unblock adding piece -> wait for piece to be added and assert
		td.unblockAddPiece()
		td.waitForAndAssert(t, ctx, dealcheckpoints.AddedPiece)
		harness.EventuallyAssertNoTagged(t, ctx)

		// expect Proving event to be fired
		err := td.waitForSealingState(lapi.SectorState(sealing.Proving))
		require.NoError(t, err)

		// assert logs
		lgs, err := harness.Provider.logsDB.Logs(ctx, td.params.DealUUID)
		require.NoError(t, err)
		require.NotEmpty(t, lgs)
	}

	t.Run("with remote commp / car v1", func(t *testing.T) {
		runTest(t, false, CarVersion1)
	})
	t.Run("with remote commp / car v2", func(t *testing.T) {
		runTest(t, false, CarVersion2)
	})
	t.Run("with local commp / car v1", func(t *testing.T) {
		runTest(t, true, CarVersion1)
	})
	t.Run("with local commp / car v2", func(t *testing.T) {
		runTest(t, true, CarVersion2)
	})
}

func TestMultipleDealsConcurrent(t *testing.T) {
	t.Skip("TestMultipleDealsConcurrent is flaky, disabling for now")

	//logging.SetLogLevel("boost-provider", "debug")
	//logging.SetLogLevel("boost-storage-deal", "debug")
	nDeals := 10
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	tds := harness.executeNDealsConcurrentAndWaitFor(t, nDeals, func(i int) *testDeal {
		return harness.newDealBuilder(t, 1).withAllMinerCallsNonBlocking().withNormalHttpServer().build()
	}, func(_ int, td *testDeal) error {
		return td.waitForCheckpoint(dealcheckpoints.AddedPiece)
	})

	for i := 0; i < nDeals; i++ {
		td := tds[i]
		td.assertPieceAdded(t, ctx)
	}

	harness.EventuallyAssertNoTagged(t, ctx)
}

func TestDealsRejectedForFunds(t *testing.T) {
	t.Skip("flaky test")

	ctx := context.Background()
	// setup the provider test harness with configured publish fee per deal and a total wallet balance.
	harness := NewHarness(t, withMinPublishFees(abi.NewTokenAmount(100)), withPublishWalletBal(1000))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// 10 deals should get accepted and 5 deals should fail as we wont have enough funds to pay for the publishing costs.
	nDeals := 15
	var errg errgroup.Group

	var mu sync.Mutex
	var failedTds []*testDeal
	var successTds []*testDeal

	for i := 0; i < nDeals; i++ {
		td := harness.newDealBuilder(t, i).withNoOpMinerStub().withBlockingHttpServer().build()
		errg.Go(func() error {
			if err := td.executeAndSubscribe(); err != nil {
				// deal should be rejected only for lack of funds
				if !strings.Contains(err.Error(), "insufficient funds") {
					return errors.New("did not get expected error")
				}

				mu.Lock()
				failedTds = append(failedTds, td)
				mu.Unlock()
			} else {
				mu.Lock()
				successTds = append(successTds, td)
				mu.Unlock()
			}

			return nil
		})
	}
	require.NoError(t, errg.Wait())
	// ensure 10 deals got accepted and five deals failed
	require.Len(t, successTds, 10)
	require.Len(t, failedTds, 5)

	// cancel all transfers so all deals finish and db files can be deleted
	for i := range successTds {
		td := successTds[i]
		require.NoError(t, harness.Provider.CancelDealDataTransfer(td.params.DealUUID))
		td.assertEventuallyDealCleanedup(t, ctx)
	}

}

func TestDealRejectedForDuplicateProposal(t *testing.T) {
	ctx := context.Background()
	harness := NewHarness(t)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	t.Run("online", func(t *testing.T) {
		td := harness.newDealBuilder(t, 1).withNoOpMinerStub().withBlockingHttpServer().build()
		err := td.executeAndSubscribe()
		require.NoError(t, err)

		pi, err := td.ph.Provider.ExecuteDeal(context.Background(), td.params, "")
		require.NoError(t, err)
		require.False(t, pi.Accepted)
		require.Contains(t, pi.Reason, "deal proposal is identical")
	})

	t.Run("offline", func(t *testing.T) {
		td := harness.newDealBuilder(t, 1, withOfflineDeal()).withNoOpMinerStub().build()
		_, err := td.ph.Provider.ExecuteDeal(context.Background(), td.params, "")
		require.NoError(t, err)

		pi, err := td.ph.Provider.ExecuteDeal(context.Background(), td.params, "")
		require.NoError(t, err)
		require.False(t, pi.Accepted)
		require.Contains(t, pi.Reason, "deal proposal is identical")
	})
}

func TestDealRejectedForDuplicateUuid(t *testing.T) {
	ctx := context.Background()
	harness := NewHarness(t)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	t.Run("online", func(t *testing.T) {
		td := harness.newDealBuilder(t, 1).withNoOpMinerStub().withBlockingHttpServer().build()
		err := td.executeAndSubscribe()
		require.NoError(t, err)

		td2 := harness.newDealBuilder(t, 2).withNoOpMinerStub().withBlockingHttpServer().build()
		td2.params.DealUUID = td.params.DealUUID
		pi, err := td.ph.Provider.ExecuteDeal(context.Background(), td2.params, "")
		require.NoError(t, err)
		require.False(t, pi.Accepted)
		require.Contains(t, pi.Reason, "deal has the same uuid")
	})

	t.Run("offline", func(t *testing.T) {
		td := harness.newDealBuilder(t, 1, withOfflineDeal()).withNoOpMinerStub().build()
		_, err := td.ph.Provider.ExecuteDeal(context.Background(), td.params, "")
		require.NoError(t, err)

		td2 := harness.newDealBuilder(t, 2, withOfflineDeal()).withNoOpMinerStub().build()
		td2.params.DealUUID = td.params.DealUUID
		pi, err := td.ph.Provider.ExecuteDeal(context.Background(), td2.params, "")
		require.NoError(t, err)
		require.False(t, pi.Accepted)
		require.Contains(t, pi.Reason, "deal has the same uuid")
	})
}

func TestDealRejectedForInsufficientProviderFunds(t *testing.T) {
	ctx := context.Background()
	// setup the provider test harness with configured publish fee per deal
	// that is more than the total wallet balance.
	harness := NewHarness(t, withMinPublishFees(abi.NewTokenAmount(100)), withPublishWalletBal(50))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	td := harness.newDealBuilder(t, 1).withNoOpMinerStub().withBlockingHttpServer().build()
	pi, err := td.ph.Provider.ExecuteDeal(context.Background(), td.params, peer.ID(""))
	require.NoError(t, err)
	require.False(t, pi.Accepted)
	require.Contains(t, pi.Reason, "insufficient funds")
}

func TestDealRejectedForInsufficientProviderStorageSpace(t *testing.T) {
	ctx := context.Background()
	// setup the provider test harness with only enough storage
	// space for 1.5 deals
	fileSize := 2000
	harness := NewHarness(t, withMaxStagingDealsBytes(uint64(fileSize*3)/2))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// Make a deal
	td := harness.newDealBuilder(t, 1, withNormalFileSize(fileSize)).withNoOpMinerStub().withBlockingHttpServer().build()
	pi, err := td.ph.Provider.ExecuteDeal(context.Background(), td.params, peer.ID(""))
	require.NoError(t, err)
	require.True(t, pi.Accepted)

	// Make a second deal - this one should fail because there is not enough
	// space in the staging area for a second deal
	td2 := harness.newDealBuilder(t, 2, withNormalFileSize(fileSize)).withNoOpMinerStub().withBlockingHttpServer().build()
	pi, err = td2.ph.Provider.ExecuteDeal(context.Background(), td2.params, peer.ID(""))
	require.NoError(t, err)
	require.False(t, pi.Accepted)
	require.Contains(t, pi.Reason, "no space left")
}

func TestDealRejectedForInsufficientProviderStorageSpacePerHost(t *testing.T) {
	ctx := context.Background()
	// Set up the harness such that
	// - the download staging area is large enough for 10 deals
	// - the portion of the staging area reserved for each host is 15%
	//   (ie enough space for 1.5 deals)
	// Therefore it should be possible to make several deals where the data
	// for each deal is on a different host. But it should not be possible
	// to make 2 deals where the data is on the same host.
	fileSize := 2000
	harness := NewHarness(t,
		withMaxStagingDealsBytes(uint64(fileSize*10)),
		withMaxStagingDealsPercentPerHost(15))
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	hostA := "http://1.2.3.4:1234"
	hostB := "http://5.6.7.8:5678"
	downloadUrl := func(h string) string {
		return fmt.Sprintf(`{"URL":"%s/%d.car"}`, h, rand.Intn(2<<30))
	}

	// Make a deal with data url on host A
	hostADeal := harness.newDealBuilder(t, 1, withNormalFileSize(fileSize)).withNoOpMinerStub().withBlockingHttpServer().build()
	hostADeal.params.Transfer.Params = []byte(downloadUrl(hostA))
	pi, err := hostADeal.ph.Provider.ExecuteDeal(ctx, hostADeal.params, "")
	require.NoError(t, err)
	require.True(t, pi.Accepted)

	// Make a deal with data url on host B
	hostBDeal := harness.newDealBuilder(t, 2, withNormalFileSize(fileSize)).withNoOpMinerStub().withBlockingHttpServer().build()
	pi, err = hostBDeal.ph.Provider.ExecuteDeal(ctx, hostBDeal.params, "")
	hostBDeal.params.Transfer.Params = []byte(downloadUrl(hostB))
	require.NoError(t, err)
	require.True(t, pi.Accepted)

	// Make a second deal with data url on host A.
	// This one should fail because we've already made a deal with
	// data url on host A, and the download space per host is limited
	// to 1.5 x the deal size.
	hostADeal2 := harness.newDealBuilder(t, 3, withNormalFileSize(fileSize)).withNoOpMinerStub().withBlockingHttpServer().build()
	hostADeal2.params.Transfer.Params = []byte(downloadUrl(hostA))
	pi, err = hostADeal2.ph.Provider.ExecuteDeal(ctx, hostADeal2.params, "")
	require.NoError(t, err)
	require.False(t, pi.Accepted)
	require.Contains(t, pi.Reason, "no space left")
}

// Tests scenarios where a deal fails with a fatal error
func TestDealFailuresHandlingNonRecoverableErrors(t *testing.T) {
	ctx := context.Background()

	tcs := []struct {
		name        string
		expectedErr string
		opts        []harnessOpt
	}{{
		name:        "non-recoverable error when transport.Execute returns an error",
		expectedErr: "failed to start data transfer",
		opts: []harnessOpt{
			// Inject a Transport that immediately returns an error from Execute
			withTransportBuilder(func(ctrl *gomock.Controller) transport.Transport {
				tspt := mocks.NewMockTransport(ctrl)
				tspt.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("somerr"))
				return tspt
			}),
		},
	}, {
		name:        "non-recoverable error when transport fails",
		expectedErr: "data-transfer failed",
		opts: []harnessOpt{
			// Inject a Transport that returns success from Execute, but then
			// returns an error from the transfer progress channel
			withTransportBuilder(func(ctrl *gomock.Controller) transport.Transport {
				th := mocks.NewMockHandler(ctrl)
				evts := make(chan tspttypes.TransportEvent, 1)
				evts <- tspttypes.TransportEvent{
					NBytesReceived: 0,
					Error:          errors.New("data-transfer failed"),
				}
				close(evts)
				th.EXPECT().Sub().Return(evts)
				th.EXPECT().Close()
				tspt := mocks.NewMockTransport(ctrl)
				tspt.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any()).Return(th, nil)
				return tspt
			}),
		},
	}, {
		name:        "non-recoverable error when commp fails",
		expectedErr: "failed to verify CommP",
		opts: []harnessOpt{
			// Inject a Transport that returns success but doesn't download
			// anything. Running commp over the (empty) file will produce a
			// cid that does not match the deal proposal commp
			withTransportBuilder(func(ctrl *gomock.Controller) transport.Transport {
				th := mocks.NewMockHandler(ctrl)
				evts := make(chan tspttypes.TransportEvent)
				close(evts)
				th.EXPECT().Sub().Return(evts)
				th.EXPECT().Close()
				tspt := mocks.NewMockTransport(ctrl)
				tspt.EXPECT().Execute(gomock.Any(), gomock.Any(), gomock.Any()).Return(th, nil)
				return tspt
			}),
		},
	}}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// setup the provider test harness
			harness := NewHarness(t, tc.opts...)
			// start the provider test harness
			harness.Start(t, ctx)
			defer harness.Stop()

			// build the deal proposal
			td := harness.newDealBuilder(t, 1).build()

			// execute deal
			err := td.executeAndSubscribe()
			require.NoError(t, err)

			// assert cleanup of deal and db state
			td.assertEventuallyDealCleanedup(t, ctx)
			td.assertDealFailedNonRecoverable(t, ctx, tc.expectedErr)

			// assert storage and funds are untagged
			harness.EventuallyAssertNoTagged(t, ctx)

			// shutdown the existing provider and create a new provider
			harness.shutdownAndCreateNewProvider(t)

			// update the test deal state with the new provider
			td.updateWithRestartedProvider(harness)

			// start the provider
			err = harness.Provider.Start()
			require.NoError(t, err)

			// expect the deal not to have been restarted (because it failed
			// with a fatal error)
			dh := harness.Provider.getDealHandler(td.params.DealUUID)
			require.Nil(t, dh)
		})
	}
}

// Tests scenarios where there is an error that is resolved when the provider
// restarts
func TestDealAutoRestartAfterAutoRecoverableErrors(t *testing.T) {
	ctx := context.Background()

	tcs := []struct {
		name        string
		dbuilder    func(h *ProviderHarness) *testDeal
		expectedErr string
		onResume    func(b *testDealBuilder) *testDeal
	}{{
		name: "publish confirm fails",
		dbuilder: func(h *ProviderHarness) *testDeal {
			// Simulate publish confirm failure
			return h.newDealBuilder(t, 1).withCommpNonBlocking().withPublishNonBlocking().withPublishConfirmFailing(errors.New("pubconferr")).withNormalHttpServer().build()
		},
		expectedErr: "pubconferr",
		onResume: func(builder *testDealBuilder) *testDeal {
			// Simulate publish confirm success (and then success for all other calls to miner)
			return builder.withPublishConfirmNonBlocking().withAddPieceNonBlocking().withAnnounceNonBlocking().build()
		},
	}, {
		name: "add piece fails",
		dbuilder: func(h *ProviderHarness) *testDeal {
			// Simulate add piece failure
			return h.newDealBuilder(t, 1).withCommpNonBlocking().withPublishNonBlocking().withPublishConfirmNonBlocking().withAddPieceFailing(errors.New("addpieceerr")).withNormalHttpServer().build()
		},
		expectedErr: "addpieceerr",
		onResume: func(builder *testDealBuilder) *testDeal {
			// Simulate add piece success
			return builder.withAddPieceNonBlocking().withAnnounceNonBlocking().build()
		},
	}}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// setup the provider test harness
			harness := NewHarness(t)
			// start the provider test harness
			harness.Start(t, ctx)
			defer harness.Stop()

			// build the deal proposal
			td := tc.dbuilder(harness)

			// execute deal
			err := td.executeAndSubscribe()
			require.NoError(t, err)

			// expect recoverable error
			err = td.waitForError(tc.expectedErr, types.DealRetryAuto)
			require.NoError(t, err)

			// shutdown the existing provider and create a new provider
			harness.shutdownAndCreateNewProvider(t)

			// update the test deal state with the new provider
			tbuilder := td.updateWithRestartedProvider(harness)
			td = tc.onResume(tbuilder)

			// start the provider -> this will restart the deal
			err = harness.Provider.Start()
			require.NoError(t, err)
			dh := harness.Provider.getDealHandler(td.params.DealUUID)
			require.NotNil(t, dh)

			//Check for fast retrieval
			require.True(t, td.params.RemoveUnsealedCopy)

			sub, err := dh.subscribeUpdates()
			require.NoError(t, err)
			td.sub = sub
			td.waitForAndAssert(t, ctx, dealcheckpoints.AddedPiece)

			// assert funds and storage are no longer tagged
			harness.EventuallyAssertNoTagged(t, ctx)
		})
	}
}

func TestOfflineDealRestartAfterManualRecoverableErrors(t *testing.T) {
	ctx := context.Background()

	tcs := []struct {
		name        string
		dbuilder    func(h *ProviderHarness) *testDeal
		expectedErr string
		onResume    func(builder *testDealBuilder) *testDeal
	}{{
		name: "commp mismatch",
		dbuilder: func(h *ProviderHarness) *testDeal {
			// Simulate commp mismatch
			return h.newDealBuilder(t, 1, withOfflineDeal()).withCommpFailing(errors.New("mismatcherr")).build()
		},
		expectedErr: "mismatcherr",
		onResume: func(builder *testDealBuilder) *testDeal {
			return builder.withCommpNonBlocking().withPublishNonBlocking().withPublishConfirmNonBlocking().withAddPieceNonBlocking().withAnnounceNonBlocking().build()
		},
	}, {
		name: "deal exec panic",
		dbuilder: func(h *ProviderHarness) *testDeal {
			// Simulate panic
			return h.newDealBuilder(t, 1, withOfflineDeal()).withCommpFailing(errors.New("panic: commp panic")).build()
		},
		expectedErr: "caught panic in deal execution: commp panic",
		onResume: func(builder *testDealBuilder) *testDeal {
			return builder.withCommpNonBlocking().withPublishNonBlocking().withPublishConfirmNonBlocking().withAddPieceNonBlocking().withAnnounceNonBlocking().build()
		},
	}}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// setup the provider test harness
			harness := NewHarness(t)
			// start the provider test harness
			harness.Start(t, ctx)
			defer harness.Stop()

			// build the deal proposal
			td := tc.dbuilder(harness)

			_, err := td.ph.Provider.ExecuteDeal(context.Background(), td.params, "")
			require.NoError(t, err)

			// execute deal
			err = td.executeAndSubscribeImportOfflineDeal(false)
			require.NoError(t, err)

			// expect recoverable error with retry type Manual
			err = td.waitForError(tc.expectedErr, types.DealRetryManual)
			require.NoError(t, err)

			// shutdown the existing provider and create a new provider
			harness.shutdownAndCreateNewProvider(t)

			// update the test deal state with the new provider
			tbuilder := td.updateWithRestartedProvider(harness)
			td = tc.onResume(tbuilder)

			// start the provider
			err = harness.Provider.Start()
			require.NoError(t, err)

			// expect the deal not to have been automatically restarted
			// (because it was paused with retry set to "manual")
			require.False(t, harness.Provider.isRunning(td.params.DealUUID))

			// manually retry the deal
			err = harness.Provider.RetryPausedDeal(td.params.DealUUID)
			require.NoError(t, err)

			// expect the deal to complete successfully
			dh := harness.Provider.getDealHandler(td.params.DealUUID)
			require.NotNil(t, dh)
			sub, err := dh.subscribeUpdates()
			require.NoError(t, err)
			td.sub = sub
			td.waitForAndAssert(t, ctx, dealcheckpoints.AddedPiece)

			// assert funds and storage are no longer tagged
			harness.EventuallyAssertNoTagged(t, ctx)
		})
	}
}

// Tests scenarios where a deal is paused with an error that the user must
// resolve by retrying manually, and the user retries the deal
func TestDealRestartAfterManualRecoverableErrors(t *testing.T) {
	ctx := context.Background()

	tcs := []struct {
		name        string
		dbuilder    func(h *ProviderHarness) *testDeal
		expectedErr string
		onResume    func(builder *testDealBuilder) *testDeal
	}{{
		name: "publish fails",
		dbuilder: func(h *ProviderHarness) *testDeal {
			// Simulate publish deal failure
			return h.newDealBuilder(t, 1).withCommpNonBlocking().withPublishFailing(errors.New("puberr")).withNormalHttpServer().build()
		},
		expectedErr: "puberr",
		onResume: func(builder *testDealBuilder) *testDeal {
			return builder.withPublishNonBlocking().withPublishConfirmNonBlocking().withAddPieceNonBlocking().withAnnounceNonBlocking().build()
		},
	}}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// setup the provider test harness
			harness := NewHarness(t)
			// start the provider test harness
			harness.Start(t, ctx)
			defer harness.Stop()

			// build the deal proposal
			td := tc.dbuilder(harness)

			// execute deal
			err := td.executeAndSubscribe()
			require.NoError(t, err)

			// expect recoverable error with retry type Manual
			err = td.waitForError(tc.expectedErr, types.DealRetryManual)
			require.NoError(t, err)

			// shutdown the existing provider and create a new provider
			harness.shutdownAndCreateNewProvider(t)

			// update the test deal state with the new provider
			tbuilder := td.updateWithRestartedProvider(harness)
			td = tc.onResume(tbuilder)

			// start the provider
			err = harness.Provider.Start()
			require.NoError(t, err)

			// expect the deal not to have been automatically restarted
			// (because it was paused with retry set to "manual")
			require.False(t, harness.Provider.isRunning(td.params.DealUUID))

			// manually retry the deal
			err = harness.Provider.RetryPausedDeal(td.params.DealUUID)
			require.NoError(t, err)

			// expect the deal to complete successfully
			dh := harness.Provider.getDealHandler(td.params.DealUUID)
			require.NotNil(t, dh)
			sub, err := dh.subscribeUpdates()
			require.NoError(t, err)
			td.sub = sub
			td.waitForAndAssert(t, ctx, dealcheckpoints.AddedPiece)

			// assert funds and storage are no longer tagged
			harness.EventuallyAssertNoTagged(t, ctx)
		})
	}
}

func TestDealRestartFailExpiredDeal(t *testing.T) {
	ctx := context.Background()

	tcs := []struct {
		name      string
		isOffline bool
	}{{
		name:      "online",
		isOffline: false,
	}, {
		name:      "offline",
		isOffline: true,
	}}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// setup the provider test harness
			var chainHead *chaintypes.TipSet
			harness := NewHarness(t, withChainHeadFunction(func(ctx context.Context) (*chaintypes.TipSet, error) {
				return chainHead, nil
			}))

			chainHead, err := mockTipset(harness.MinerAddr, 5)
			require.NoError(t, err)

			// start the provider test harness
			harness.Start(t, ctx)
			defer harness.Stop()

			// build the deal proposal
			opts := []dealProposalOpt{}
			if tc.isOffline {
				opts = append(opts, withOfflineDeal())
			}
			td := harness.newDealBuilder(t, 1, opts...).withCommpBlocking(true).build()

			// execute deal
			err = td.executeAndSubscribe()
			require.NoError(t, err)

			err = td.waitForCheckpoint(dealcheckpoints.Accepted)
			require.NoError(t, err)

			// shutdown the existing provider and create a new provider
			harness.shutdownAndCreateNewProvider(t)

			// update the test deal state with the new provider
			_ = td.updateWithRestartedProvider(harness)

			// simulate a chain height that is greater that the epoch by which
			// the deal should have been sealed
			chainHead, err = mockTipset(harness.MinerAddr, td.params.ClientDealProposal.Proposal.StartEpoch+10)
			require.NoError(t, err)

			// start the provider
			err = harness.Provider.Start()
			require.NoError(t, err)

			// expect the deal to fail on startup because it has expired
			require.Eventually(t, func() bool {
				dl, err := harness.Provider.Deal(ctx, td.params.DealUUID)
				require.NoError(t, err)
				return strings.Contains(dl.Err, "expired")
			}, 5*time.Second, 100*time.Millisecond)
		})
	}
}

// Tests scenario that a contract deal fails fatally when PublishStorageDeal fails.
func TestContractDealFatalFailAfterPublishError(t *testing.T) {
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// generate random f4 client contract address
	f4addr, err := address.NewFromString("f410fnqocy7tkrlw4l5mdhgjlc4gsiafr7e76yopvo2y")
	require.NoError(t, err)

	// simulate publish deal failure
	td := harness.newDealBuilder(t, 1, withClientAddr(f4addr)).withCommpNonBlocking().withPublishFailing(errors.New("puberr")).withNormalHttpServer().build()

	// execute deal
	err = td.executeAndSubscribe()
	require.NoError(t, err)

	// expect fatal error (types.DealRetryFatal) as this is a contract deal
	// note that we return recoverable errors for regular client addresses
	err = td.waitForError("puberr", types.DealRetryFatal)
	require.NoError(t, err)
}

// Tests scenarios where a deal is paused with an error that the user must
// resolve by retrying manually, and the user fails the deal
func TestDealFailAfterManualRecoverableErrors(t *testing.T) {
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t)
	// start the provider test harness
	harness.Start(t, ctx)
	defer harness.Stop()

	// Simulate publish deal failure
	td := harness.newDealBuilder(t, 1).withCommpNonBlocking().withPublishFailing(errors.New("puberr")).withNormalHttpServer().build()

	// execute deal
	err := td.executeAndSubscribe()
	require.NoError(t, err)

	// expect recoverable error with retry type Manual
	err = td.waitForError("puberr", types.DealRetryManual)
	require.NoError(t, err)

	// wait for the deal execution thread to complete
	require.Eventually(t, func() bool {
		return !harness.Provider.isRunning(td.params.DealUUID)
	}, time.Second, 10*time.Millisecond)

	// manually fail the deal
	err = harness.Provider.FailPausedDeal(td.params.DealUUID)
	require.NoError(t, err)

	// assert cleanup of deal and db state
	td.assertEventuallyDealCleanedup(t, ctx)
	td.assertDealFailedNonRecoverable(t, ctx, "user manually terminated the deal")

	// assert storage and funds are untagged
	harness.EventuallyAssertNoTagged(t, ctx)
}

func TestDealAskValidation(t *testing.T) {
	ctx := context.Background()

	tcs := map[string]struct {
		ask         *legacytypes.StorageAsk
		dbuilder    func(h *ProviderHarness) *testDeal
		expectedErr string
	}{
		"fails if price below minimum for unverified deal": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(100000000000),
			},
			dbuilder: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withNoOpMinerStub().build()

			},
			expectedErr: "storage price per epoch less than asking price",
		},
		"fails if price below minimum for verified deal": {
			ask: &legacytypes.StorageAsk{
				Price:         abi.NewTokenAmount(0),
				VerifiedPrice: abi.NewTokenAmount(100000000000),
			},
			dbuilder: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withVerifiedDeal()).withNoOpMinerStub().build()

			},
			expectedErr: "storage price per epoch less than asking price",
		},
		"fails if piece size below minimum": {
			ask: &legacytypes.StorageAsk{
				Price:        abi.NewTokenAmount(0),
				MinPieceSize: abi.PaddedPieceSize(1000000000),
			},
			dbuilder: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withNormalFileSize(100)).withNoOpMinerStub().build()

			},
			expectedErr: "piece size less than minimum required size",
		},
		"fails if piece size above maximum": {
			ask: &legacytypes.StorageAsk{
				Price:        abi.NewTokenAmount(0),
				MaxPieceSize: abi.PaddedPieceSize(1),
			},
			dbuilder: func(h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withNormalFileSize(100)).withNoOpMinerStub().build()

			},
			expectedErr: "piece size more than maximum allowed size",
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			// setup the provider test harness
			harness := NewHarness(t, withStoredAsk(tc.ask.Price, tc.ask.VerifiedPrice, tc.ask.MinPieceSize, tc.ask.MaxPieceSize))
			// start the provider test harness
			harness.Start(t, ctx)
			defer harness.Stop()

			// build the deal proposal with the blocking http test server and a completely blocking miner stub
			td := tc.dbuilder(harness)

			// execute deal
			err := td.executeAndSubscribe()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

func TestDealVerification(t *testing.T) {
	ctx := context.Background()

	tcs := map[string]struct {
		ask         *legacytypes.StorageAsk
		dbuilder    func(t *testing.T, h *ProviderHarness) *testDeal
		expectedErr string
		expect      func(h *ProviderHarness)
		opts        []harnessOpt
	}{
		"fails if client does not have enough datacap for verified deal": {
			ask: &legacytypes.StorageAsk{
				VerifiedPrice: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withVerifiedDeal()).withNoOpMinerStub().build()
			},
			expect: func(h *ProviderHarness) {
				sp := abi.NewStoragePower(1)
				h.MockFullNode.EXPECT().StateVerifiedClientStatus(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sp,
					nil)
			},
			expectedErr: "verified deal DataCap 1 too small",
		},
		"fails if can't fetch datacap for verified deal": {
			ask: &legacytypes.StorageAsk{
				VerifiedPrice: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withVerifiedDeal()).withNoOpMinerStub().build()
			},
			expect: func(h *ProviderHarness) {
				h.MockFullNode.EXPECT().StateVerifiedClientStatus(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil,
					errors.New("some error"))
			},
			expectedErr: "getting verified datacap",
		},
		"fails if client does NOT have enough balance for deal": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withNoOpMinerStub().build()
			},
			opts:        []harnessOpt{withStateMarketBalance(abi.NewTokenAmount(10), abi.NewTokenAmount(10))},
			expectedErr: "funds in escrow 0 not enough",
		},
		"fails if client signature is not valid": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withNoOpMinerStub().build()
			},
			expect: func(h *ProviderHarness) {
				h.Provider.sigVerifier = &mockSignatureVerifier{false, nil}
			},
			expectedErr: "invalid signature",
		},
		"fails if client signature verification fails": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1).withNoOpMinerStub().build()
			},
			expect: func(h *ProviderHarness) {
				h.Provider.sigVerifier = &mockSignatureVerifier{true, errors.New("some error")}
			},
			expectedErr: "validating signature",
		},
		"fails if proposed provider collateral below minimum": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withProviderCollateral(abi.NewTokenAmount(0))).withNoOpMinerStub().build()
			},
			expectedErr: "proposed provider collateral 0 below minimum",
		},
		"fails if proposed provider collateral above maximum": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(_ *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withProviderCollateral(abi.NewTokenAmount(100))).withNoOpMinerStub().build()
			},
			expectedErr: "proposed provider collateral 100 above maximum",
		},

		"fails if provider address does not match": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				addr, err := address.NewIDAddress(1)
				require.NoError(t, err)
				return h.newDealBuilder(t, 1, withMinerAddr(addr)).withNoOpMinerStub().build()
			},
			expectedErr: "incorrect provider for deal",
		},
		"proposal piece cid has wrong prefix": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withPieceCid(testutil.GenerateCid())).withNoOpMinerStub().build()
			},
			expectedErr: "proposal PieceCID had wrong prefix",
		},
		"proposal piece cid undefined": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withUndefinedPieceCid()).withNoOpMinerStub().build()
			},
			expectedErr: "proposal PieceCID undefined",
		},
		"proposal end 9 before proposal start 10": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withEpochs(abi.ChainEpoch(10), abi.ChainEpoch(9))).withNoOpMinerStub().build()
			},
			expectedErr: "proposal end 9 before proposal start 10",
		},
		"deal start epoch has already elapsed": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withEpochs(abi.ChainEpoch(-1), abi.ChainEpoch(9))).withNoOpMinerStub().build()
			},
			expectedErr: "deal start epoch -1 has already elapsed",
		},
		"deal piece size invalid": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				return h.newDealBuilder(t, 1, withPieceSize(abi.PaddedPieceSize(1000))).withNoOpMinerStub().build()
			},
			expectedErr: "proposal piece size is invalid",
		},
		"deal end epoch too far out": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {
				start := miner.MaxSectorExpirationExtension - market.DealMinDuration - 1
				maxEndEpoch := miner.MaxSectorExpirationExtension + 100
				return h.newDealBuilder(t, 1, withEpochs(abi.ChainEpoch(start), abi.ChainEpoch(maxEndEpoch))).withNoOpMinerStub().build()
			},
			expectedErr: "invalid deal end epoch",
		},
		"deal duration greater than max duration": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {

				return h.newDealBuilder(t, 1, withEpochs(10, abi.ChainEpoch(1278*builtin.EpochsInDay)+11)).withNoOpMinerStub().build() // TODO: Use v12 value from package when Lotus has updated the API package to use market v12
			},
			expectedErr: "deal duration out of bounds",
		},
		"deal duration less than min duration": {
			ask: &legacytypes.StorageAsk{
				Price: abi.NewTokenAmount(0),
			},
			dbuilder: func(t *testing.T, h *ProviderHarness) *testDeal {

				return h.newDealBuilder(t, 1, withEpochs(10, 11)).withNoOpMinerStub().build()
			},
			expectedErr: "deal duration out of bounds",
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			// setup the provider test harness
			harness := NewHarness(t, tc.opts...)
			// start the provider test harness
			harness.Start(t, ctx)
			defer harness.Stop()

			// build the deal proposal with the blocking http test server and a completely blocking miner stub
			td := tc.dbuilder(t, harness)
			if tc.expect != nil {
				tc.expect(harness)
			}

			// execute deal
			err := td.executeAndSubscribe()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

func TestIPNIAnnounce(t *testing.T) {
	ctx := context.Background()

	runTest := func(announce bool) {
		// setup the provider test harness
		harness := NewHarness(t)
		harness.Start(t, ctx)
		defer harness.Stop()

		td := harness.newDealBuilder(t, 1).withAllMinerCallsNonBlocking().withNormalHttpServer().
			withDealParamAnnounce(announce).build()
		require.NoError(t, td.executeAndSubscribe())

		td.waitForAndAssert(t, ctx, dealcheckpoints.IndexedAndAnnounced)
	}

	t.Run("announce", func(t *testing.T) {
		runTest(true)
	})

	t.Run("don't announce", func(t *testing.T) {
		runTest(false)
	})
}

func TestDealFilter(t *testing.T) {
	ctx := context.Background()

	var dealFilterParams dealfilter.DealFilterParams
	df := func(ctx context.Context, dfp dealfilter.DealFilterParams) (bool, string, error) {
		dealFilterParams = dfp
		return true, "", nil
	}

	// setup the provider test harness
	harness := NewHarness(t, withDealFilter(df))
	harness.Start(t, ctx)
	defer harness.Stop()

	td := harness.newDealBuilder(t, 1).withAllMinerCallsNonBlocking().withNormalHttpServer().build()
	require.NoError(t, td.executeAndSubscribe())

	td.waitForAndAssert(t, ctx, dealcheckpoints.IndexedAndAnnounced)
	require.EqualValues(t, 1000, dealFilterParams.FundsState.Collateral.Balance.Uint64())
	require.EqualValues(t, 2000000, dealFilterParams.FundsState.Escrow.Available.Uint64())
	require.EqualValues(t, 10000000000, dealFilterParams.StorageState.TotalAvailable)
}

func TestFinalSealingState(t *testing.T) {
	ctx := context.Background()
	harness := NewHarness(t)

	harness.Start(t, ctx)
	defer harness.Stop()

	// The deal ID returned from Publish Storage Deals is hard-coded to 1 for
	// these tests.
	// Set the deal ID that is returned from the call to SectorsStatus
	// to be different so that there is a mismatch when checking the sealing
	// state.
	sectorsStatusDealId := abi.DealID(10)
	td := harness.newDealBuilder(t, 1, withSectorStatusDealId(sectorsStatusDealId)).withAllMinerCallsNonBlocking().withNormalHttpServer().build()
	require.NoError(t, td.executeAndSubscribe())

	err := td.waitForError("storage failed - deal not found in sector", types.DealRetryFatal)
	require.NoError(t, err)

}

func (h *ProviderHarness) executeNDealsConcurrentAndWaitFor(t *testing.T, nDeals int,
	buildDeal func(i int) *testDeal, waitF func(i int, td *testDeal) error) []*testDeal {
	tds := make([]*testDeal, 0, nDeals)
	var errG errgroup.Group
	for i := 0; i < nDeals; i++ {
		i := i
		// build the deal proposal
		td := buildDeal(i)
		tds = append(tds, td)

		errG.Go(func() error {
			err := td.executeAndSubscribe()
			if err != nil {
				return err
			}
			if err := waitF(i, td); err != nil {
				return err
			}
			return nil
		})
	}

	require.NoError(t, errG.Wait())

	return tds
}

func (h *ProviderHarness) AssertAccepted(t *testing.T, ctx context.Context, dp *types.DealParams) {
	h.AssertDealDBState(t, ctx, dp, abi.DealID(0), nil, dealcheckpoints.Accepted, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0), "")
}

func (h *ProviderHarness) AssertTransferred(t *testing.T, ctx context.Context, dp *types.DealParams) {
	h.AssertDealDBState(t, ctx, dp, abi.DealID(0), nil, dealcheckpoints.Transferred, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0), "")
}

func (h *ProviderHarness) AssertPublished(t *testing.T, ctx context.Context, dp *types.DealParams, so *smtestutil.StubbedMinerOutput) {
	h.AssertDealDBState(t, ctx, dp, abi.DealID(0), &so.PublishCid, dealcheckpoints.Published, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0), "")
}

func (h *ProviderHarness) AssertDealFailedTransferNonRecoverable(t *testing.T, ctx context.Context, dp *types.DealParams, errStr string) {
	h.AssertDealDBState(t, ctx, dp, abi.DealID(0), nil, dealcheckpoints.Complete, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0), errStr)
}

func (h *ProviderHarness) AssertPublishConfirmed(t *testing.T, ctx context.Context, dp *types.DealParams, so *smtestutil.StubbedMinerOutput) {
	h.AssertDealDBState(t, ctx, dp, so.DealID, &so.FinalPublishCid, dealcheckpoints.PublishConfirmed, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0), "")
}

func (h *ProviderHarness) AssertPieceAdded(t *testing.T, ctx context.Context, dp *types.DealParams, so *smtestutil.StubbedMinerOutput, carv2FilePath string) {
	h.AssertEventuallyDealCleanedup(t, ctx, dp)
	h.AssertDealDBState(t, ctx, dp, so.DealID, &so.FinalPublishCid, dealcheckpoints.IndexedAndAnnounced, so.SectorID, so.Offset, dp.ClientDealProposal.Proposal.PieceSize.Unpadded().Padded(), "")
	// Assert that the original file data we sent matches what was sent to the sealer
	h.AssertSealedContents(t, carv2FilePath, *so.SealedBytes)
	// assert that dagstore and piecestore have this deal
	_, err := h.DealsDB.ByID(ctx, dp.DealUUID)
	require.NoError(t, err)
	//rg, ok := h.DAGStore.GetRegistration(dbState.ClientDealProposal.Proposal.PieceCID)
	//require.True(t, ok)
	//require.True(t, rg.EagerInit)
	//require.Empty(t, rg.CarPath)
}

func (h *ProviderHarness) AssertDealIndexed(t *testing.T, ctx context.Context, dp *types.DealParams, so *smtestutil.StubbedMinerOutput) {
	h.AssertEventuallyDealCleanedup(t, ctx, dp)
	h.AssertDealDBState(t, ctx, dp, so.DealID, &so.FinalPublishCid, dealcheckpoints.IndexedAndAnnounced, so.SectorID, so.Offset, dp.ClientDealProposal.Proposal.PieceSize.Unpadded().Padded(), "")
}

func (h *ProviderHarness) EventuallyAssertNoTagged(t *testing.T, ctx context.Context) {
	h.EventuallyAssertStorageFundState(t, ctx, 0, abi.NewTokenAmount(0), abi.NewTokenAmount(0))
}

func (h *ProviderHarness) EventuallyAssertStorageFundState(t *testing.T, ctx context.Context, taggedStorage uint64, pub, collat abi.TokenAmount) {
	require.Eventually(t, func() bool {
		st, _ := h.StorageDB.TotalTagged(ctx)
		if st != taggedStorage {
			return false
		}
		ft, _ := h.FundsDB.TotalTagged(ctx)

		return ft.PubMsg.Uint64() == pub.Uint64() && ft.Collateral.Uint64() == collat.Uint64()
	}, 5*time.Second, 100*time.Millisecond)
}

func (h *ProviderHarness) AssertStorageAndFundManagerState(t *testing.T, ctx context.Context, taggedStorage uint64, pub, collat abi.TokenAmount) {
	h.AssertStorageManagerState(t, ctx, taggedStorage)
	h.AssertFundManagerState(t, ctx, pub, collat)
}

func (h *ProviderHarness) AssertStorageManagerState(t *testing.T, ctx context.Context, taggedStorage uint64) {
	st, err := h.StorageDB.TotalTagged(ctx)
	require.NoError(t, err)
	require.EqualValues(t, taggedStorage, st)
}

func (h *ProviderHarness) AssertFundManagerState(t *testing.T, ctx context.Context, pub, collat abi.TokenAmount) {
	ft, err := h.FundsDB.TotalTagged(ctx)
	require.NoError(t, err)
	require.EqualValues(t, pub, ft.PubMsg)
	require.EqualValues(t, collat, ft.Collateral)
}

func (h *ProviderHarness) AssertSealedContents(t *testing.T, carV2FilePath string, read []byte) {
	r, err := os.Open(carV2FilePath)
	require.NoError(t, err)
	defer func() {
		_ = r.Close()
	}()

	actual, err := io.ReadAll(r)
	require.NoError(t, err)

	// the read-bytes also contains extra zeros for the padding magic, so just match without the padding bytes.
	require.EqualValues(t, actual, read[:len(actual)])
}

func (h *ProviderHarness) AssertEventuallyDealCleanedup(t *testing.T, ctx context.Context, dp *types.DealParams) {
	dbState, err := h.DealsDB.ByID(ctx, dp.DealUUID)
	require.NoError(t, err)
	// assert that the deal has been cleanedup and there are no leaks
	require.Eventually(t, func() bool {
		// deal handler should be deleted
		dh := h.Provider.getDealHandler(dbState.DealUuid)
		if dh != nil {
			return false
		}

		// the deal inbound file should no longer exist if it is an online deal,
		// or if it is an offline deal with the delete after import flag set
		if dbState.CleanupData {
			_, statErr := os.Stat(dbState.InboundFilePath)
			return os.IsNotExist(statErr)
		}
		return true
	}, 5*time.Second, 200*time.Millisecond)
}

func (h *ProviderHarness) AssertDealDBState(t *testing.T, ctx context.Context, dp *types.DealParams, expectedDealID abi.DealID, publishCid *cid.Cid,
	checkpoint dealcheckpoints.Checkpoint, sector abi.SectorNumber, offset, length abi.PaddedPieceSize, errStr string) {
	dbState, err := h.DealsDB.ByID(ctx, dp.DealUUID)
	require.NoError(t, err)
	require.EqualValues(t, dp.DealUUID, dbState.DealUuid)
	require.EqualValues(t, dp.DealDataRoot, dbState.DealDataRoot)
	require.EqualValues(t, expectedDealID, dbState.ChainDealID)
	require.EqualValues(t, checkpoint, dbState.Checkpoint)
	require.EqualValues(t, sector, dbState.SectorID)
	require.EqualValues(t, offset, dbState.Offset)
	require.EqualValues(t, length, dbState.Length)
	require.EqualValues(t, dp.Transfer, dbState.Transfer)

	if len(errStr) == 0 {
		require.Empty(t, dbState.Err)
	} else {
		require.Contains(t, dbState.Err, errStr)
	}

	if publishCid == nil {
		require.Empty(t, dbState.PublishCID)
	} else {
		require.EqualValues(t, publishCid, dbState.PublishCID)
	}
}

type ProviderHarness struct {
	Host                         host.Host
	GoMockCtrl                   *gomock.Controller
	TempDir                      string
	MinerAddr                    address.Address
	ClientAddr                   address.Address
	MockFullNode                 *lotusmocks.MockFullNode
	MinerStub                    *smtestutil.MinerStub
	DealsDB                      *db.DealsDB
	FundsDB                      *db.FundsDB
	StorageDB                    *db.StorageDB
	PublishWallet                address.Address
	PledgeCollatWallet           address.Address
	MinPublishFees               abi.TokenAmount
	MaxStagingDealBytes          uint64
	MaxStagingDealPercentPerHost uint64
	MockSealingPipelineAPI       *mock_sealingpipeline.MockAPI
	Provider                     *Provider

	// http test servers
	NormalServer        *testutil.HttpTestServer
	BlockingServer      *testutil.BlockingHttpTestServer
	DisconnectingServer *httptest.Server

	Transport transport.Transport

	SqlDB *sql.DB
}

type ChainHeadFn func(ctx context.Context) (*chaintypes.TipSet, error)

type providerConfig struct {
	mockCtrl *gomock.Controller

	maxStagingDealBytes          uint64
	maxStagingDealPercentPerHost uint64
	minPublishFees               abi.TokenAmount
	disconnectAfterEvery         int64
	httpOpts                     []httptransport.Option
	transport                    transport.Transport

	lockedFunds      big.Int
	escrowFunds      big.Int
	publishWalletBal int64
	collatWalletBal  int64

	price         abi.TokenAmount
	verifiedPrice abi.TokenAmount
	minPieceSize  abi.PaddedPieceSize
	maxPieceSize  abi.PaddedPieceSize

	localCommp  bool
	dealFilter  dealfilter.StorageDealFilter
	chainHeadFn ChainHeadFn
}

type harnessOpt func(pc *providerConfig)

// withHttpTransportOpts configures the http transport config for the provider
func withHttpTransportOpts(opts []httptransport.Option) harnessOpt {
	return func(pc *providerConfig) {
		pc.httpOpts = opts
	}
}

// withHttpDisconnectServerAfter configures the disconnecting server of the harness to disconnect after sending `after` bytes.
// TODO: This should be per-deal rather than at the harness level
func withHttpDisconnectServerAfter(afterEvery int64) harnessOpt {
	return func(pc *providerConfig) {
		pc.disconnectAfterEvery = afterEvery
	}
}

func withMinPublishFees(fee abi.TokenAmount) harnessOpt {
	return func(pc *providerConfig) {
		pc.minPublishFees = fee
	}
}

func withPublishWalletBal(bal int64) harnessOpt {
	return func(pc *providerConfig) {
		pc.publishWalletBal = bal
	}
}

func withMaxStagingDealsBytes(max uint64) harnessOpt {
	return func(pc *providerConfig) {
		pc.maxStagingDealBytes = max
	}
}

func withMaxStagingDealsPercentPerHost(max uint64) harnessOpt {
	return func(pc *providerConfig) {
		pc.maxStagingDealPercentPerHost = max
	}
}

func withTransportBuilder(bldr func(controller *gomock.Controller) transport.Transport) harnessOpt {
	return func(pc *providerConfig) {
		pc.transport = bldr(pc.mockCtrl)
	}
}

func withLocalCommp() harnessOpt {
	return func(pc *providerConfig) {
		pc.localCommp = true
	}
}

func withStoredAsk(price, verifiedPrice abi.TokenAmount, minPieceSize, maxPieceSize abi.PaddedPieceSize) harnessOpt {
	return func(pc *providerConfig) {
		pc.price = price
		pc.verifiedPrice = verifiedPrice
		pc.minPieceSize = minPieceSize
		pc.maxPieceSize = maxPieceSize
	}
}

func withStateMarketBalance(locked, escrow abi.TokenAmount) harnessOpt {
	return func(pc *providerConfig) {
		pc.lockedFunds = locked
		pc.escrowFunds = escrow
	}
}

func withDealFilter(filter dealfilter.StorageDealFilter) harnessOpt {
	return func(pc *providerConfig) {
		pc.dealFilter = filter
	}
}

func withChainHeadFunction(fn ChainHeadFn) harnessOpt {
	return func(pc *providerConfig) {
		pc.chainHeadFn = fn
	}
}

func NewHarness(t *testing.T, opts ...harnessOpt) *ProviderHarness {
	ctrl := gomock.NewController(t)
	pc := &providerConfig{
		mockCtrl: ctrl,

		minPublishFees:       abi.NewTokenAmount(100),
		maxStagingDealBytes:  10000000000,
		disconnectAfterEvery: 1048600,
		lockedFunds:          big.NewInt(3000000),
		escrowFunds:          big.NewInt(5000000),
		publishWalletBal:     1000,
		collatWalletBal:      1000,

		price:         abi.NewTokenAmount(0),
		verifiedPrice: abi.NewTokenAmount(0),
		minPieceSize:  abi.PaddedPieceSize(0),
		maxPieceSize:  abi.PaddedPieceSize(10737418240), //10Gib default
	}

	sealingpipelineStatus := map[lapi.SectorState]int{
		"AddPiece":       0,
		"Packing":        0,
		"PreCommit1":     1,
		"PreCommit2":     0,
		"PreCommitWait":  0,
		"WaitSeed":       1,
		"Committing":     0,
		"CommitWait":     0,
		"FinalizeSector": 0,
	}

	for _, opt := range opts {
		opt(pc)
	}
	// Create a temporary directory for all the tests.
	dir := t.TempDir()

	// setup mocks
	fn := lotusmocks.NewMockFullNode(ctrl)
	minerStub := smtestutil.NewMinerStub(ctrl)
	sps := minerStub.MockAPI

	ver := lapi.APIVersion{
		Version:    "lotus-miner",
		APIVersion: lapi.MinerAPIVersion0,
		BlockDelay: build.BlockDelaySecs,
	}

	sps.EXPECT().Version(gomock.Any()).Return(ver, nil).AnyTimes()

	// setup client and miner addrs
	minerAddr, err := address.NewIDAddress(1011)
	require.NoError(t, err)
	cAddr, err := address.NewIDAddress(1014)
	require.NoError(t, err)

	// instantiate the http servers that will serve the files
	normalServer := testutil.HttpTestUnstartedFileServer(t, dir)
	blockingServer := testutil.NewBlockingHttpTestServer(t, dir)
	disconnServer := testutil.HttpTestDisconnectingServer(t, dir, pc.disconnectAfterEvery)

	// create a provider libp2p peer
	mn := mocknet.New()
	h, err := mn.GenPeer()
	require.NoError(t, err)

	// setup the databases
	f, err := os.CreateTemp(dir, "*.db")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	sqldb, err := db.SqlDB(f.Name())
	require.NoError(t, err)
	dealsDB := db.NewDealsDB(sqldb)
	fundsDB := db.NewFundsDB(sqldb)
	logsDB := db.NewLogsDB(sqldb)
	dl := logs.NewDealLogger(logsDB)

	// Create http transport
	tspt := pc.transport
	if tspt == nil {
		tspt = httptransport.New(h, dl, pc.httpOpts...)
	}

	// publish wallet
	pw, err := address.NewIDAddress(1)
	require.NoError(t, err)

	pcw, err := address.NewIDAddress(2)
	require.NoError(t, err)

	// create the harness with default values
	ph := &ProviderHarness{
		Host:                         h,
		GoMockCtrl:                   ctrl,
		TempDir:                      dir,
		MinerAddr:                    minerAddr,
		ClientAddr:                   cAddr,
		NormalServer:                 normalServer,
		BlockingServer:               blockingServer,
		DisconnectingServer:          disconnServer,
		Transport:                    tspt,
		MockSealingPipelineAPI:       sps,
		DealsDB:                      dealsDB,
		FundsDB:                      db.NewFundsDB(sqldb),
		StorageDB:                    db.NewStorageDB(sqldb),
		PublishWallet:                pw,
		PledgeCollatWallet:           pcw,
		MinerStub:                    minerStub,
		MinPublishFees:               pc.minPublishFees,
		MaxStagingDealBytes:          pc.maxStagingDealBytes,
		MaxStagingDealPercentPerHost: pc.maxStagingDealPercentPerHost,
		SqlDB:                        sqldb,
	}

	// fund manager
	fminitF := fundmanager.New(fundmanager.Config{
		Enabled:      true,
		PubMsgBalMin: ph.MinPublishFees,
		PubMsgWallet: pw,
		CollatWallet: pcw,
	})
	fm := fminitF(fn, fundsDB)

	// storage manager
	fsRepo, err := repo.NewFS(dir)
	require.NoError(t, err)
	lr, err := fsRepo.Lock(repo.StorageMiner)
	require.NoError(t, err)
	smInitF := storagemanager.New(storagemanager.Config{
		MaxStagingDealsBytes:          ph.MaxStagingDealBytes,
		MaxStagingDealsPercentPerHost: ph.MaxStagingDealPercentPerHost,
	})
	sm, err := smInitF(lr, sqldb)
	require.NoError(t, err)

	// Set a no-op deal filter unless a deal filter was specified as an option
	df := func(ctx context.Context, deal dealfilter.DealFilterParams) (bool, string, error) {
		return true, "", nil
	}
	if pc.dealFilter != nil {
		df = pc.dealFilter
	}

	askStore := &mockAskStore{}
	askStore.SetAsk(pc.price, pc.verifiedPrice, pc.minPieceSize, pc.maxPieceSize)

	pdctx, cancel := context.WithCancel(context.Background())
	pm := piecedirectory.NewPieceDirectory(bdclientutil.NewTestStore(pdctx), minerStub.MockPieceReader, 1)
	pm.Start(pdctx)
	t.Cleanup(cancel)

	ph.MockSealingPipelineAPI.EXPECT().Version(gomock.Any()).Return(ver, nil).AnyTimes()

	prvCfg := Config{
		MaxTransferDuration: time.Hour,
		RemoteCommp:         !pc.localCommp,
		TransferLimiter: TransferLimiterConfig{
			MaxConcurrent:    10,
			StallCheckPeriod: time.Millisecond,
			StallTimeout:     time.Hour,
		},
		SealingPipelineCacheTimeout: time.Second,
		StorageFilter:               "1",
	}
	commpThrottle := make(chan struct{}, 1)
	prov, err := NewProvider(prvCfg, sqldb, dealsDB, fm, sm, fn, minerStub, minerAddr, minerStub, minerStub, commpThrottle, sps, minerStub, df, sqldb,
		logsDB, pm, minerStub, askStore, &mockSignatureVerifier{true, nil}, dl, tspt)
	require.NoError(t, err)
	ph.Provider = prov

	chainHeadFn := pc.chainHeadFn
	if chainHeadFn == nil {
		// Creates chain tipset with height 5
		chainHead, err := test.MockTipset(minerAddr, 1)
		require.NoError(t, err)
		chainHeadFn = func(ctx context.Context) (*chaintypes.TipSet, error) {
			return chainHead, nil
		}
	}
	fn.EXPECT().ChainHead(gomock.Any()).DoAndReturn(chainHeadFn).AnyTimes()

	fn.EXPECT().StateDealProviderCollateralBounds(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(lapi.DealCollateralBounds{
		Min: abi.NewTokenAmount(1),
		Max: abi.NewTokenAmount(1),
	}, nil).AnyTimes()

	fn.EXPECT().StateMarketBalance(gomock.Any(), gomock.Any(), gomock.Any()).Return(lapi.MarketBalance{
		Locked: pc.lockedFunds,
		Escrow: pc.escrowFunds,
	}, nil).AnyTimes()

	fn.EXPECT().WalletBalance(gomock.Any(), ph.PledgeCollatWallet).Return(abi.NewTokenAmount(pc.publishWalletBal), nil).AnyTimes()
	fn.EXPECT().WalletBalance(gomock.Any(), ph.PublishWallet).Return(abi.NewTokenAmount(pc.publishWalletBal), nil).AnyTimes()

	ph.MockSealingPipelineAPI.EXPECT().WorkerJobs(gomock.Any()).Return(map[uuid.UUID][]storiface.WorkerJob{}, nil).AnyTimes()

	ph.MockSealingPipelineAPI.EXPECT().SectorsSummary(gomock.Any()).Return(sealingpipelineStatus, nil).AnyTimes()

	ph.MockFullNode = fn

	return ph
}

func (h *ProviderHarness) shutdownAndCreateNewProvider(t *testing.T, opts ...harnessOpt) {
	pc := &providerConfig{
		minPublishFees:       abi.NewTokenAmount(100),
		maxStagingDealBytes:  10000000000,
		disconnectAfterEvery: 1048600,
		lockedFunds:          big.NewInt(300),
		escrowFunds:          big.NewInt(500),
		publishWalletBal:     1000,
		collatWalletBal:      1000,
	}
	for _, opt := range opts {
		opt(pc)
	}
	// shutdown old provider
	h.Provider.Stop()
	h.MinerStub = smtestutil.NewMinerStub(h.GoMockCtrl)
	h.MockSealingPipelineAPI = h.MinerStub.MockAPI
	ver := lapi.APIVersion{
		Version:    "lotus-miner",
		APIVersion: lapi.MinerAPIVersion0,
		BlockDelay: build.BlockDelaySecs,
	}
	h.MockSealingPipelineAPI.EXPECT().Version(gomock.Any()).Return(ver, nil).AnyTimes()
	// no-op deal filter, as we are mostly testing the Provider and provider_loop here
	df := func(ctx context.Context, deal dealfilter.DealFilterParams) (bool, string, error) {
		return true, "", nil
	}

	// Recreate the piece directory because we need to pass it the recreated mock piece reader
	pdctx, cancel := context.WithCancel(context.Background())
	pm := piecedirectory.NewPieceDirectory(bdclientutil.NewTestStore(pdctx), h.MinerStub.MockPieceReader, 1)
	pm.Start(pdctx)
	t.Cleanup(cancel)

	// construct a new provider with pre-existing state
	commpThrottle := make(chan struct{}, 1)
	prov, err := NewProvider(h.Provider.config, h.Provider.db, h.Provider.dealsDB, h.Provider.fundManager,
		h.Provider.storageManager, h.Provider.fullnodeApi, h.MinerStub, h.MinerAddr, h.MinerStub, h.MinerStub, commpThrottle, h.MockSealingPipelineAPI, h.MinerStub,
		df, h.Provider.logsSqlDB, h.Provider.logsDB, pm, h.MinerStub, h.Provider.askGetter,
		h.Provider.sigVerifier, h.Provider.dealLogger, h.Provider.Transport)

	require.NoError(t, err)
	h.Provider = prov
}

func (h *ProviderHarness) Start(t *testing.T, ctx context.Context) {
	h.NormalServer.Start()
	h.BlockingServer.Start()
	h.DisconnectingServer.Start()
	err := h.Provider.Start()
	require.NoError(t, err)
}

func (h *ProviderHarness) Stop() {
	_ = h.SqlDB.Close()
	h.NormalServer.Close()
	h.BlockingServer.Close()
	h.DisconnectingServer.Close()
	h.GoMockCtrl.Finish()

}

type dealProposalConfig struct {
	normalFileSize      int
	offlineDeal         bool
	verifiedDeal        bool
	providerCollateral  abi.TokenAmount
	clientAddr          address.Address
	minerAddr           address.Address
	pieceCid            cid.Cid
	pieceSize           abi.PaddedPieceSize
	undefinedPieceCid   bool
	startEpoch          abi.ChainEpoch
	endEpoch            abi.ChainEpoch
	label               market.DealLabel
	carVersion          CarVersion
	sectorsStatusDealId abi.DealID
}

// dealProposalOpt allows configuration of the deal proposal
type dealProposalOpt func(dc *dealProposalConfig)

// withNormalFileSize configures the deal proposal to use a normal file of the given size.
// note: the carv2 file size will be larger than this
func withNormalFileSize(normalFileSize int) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.normalFileSize = normalFileSize
	}
}

type CarVersion int

const CarVersion1 = CarVersion(1)
const CarVersion2 = CarVersion(2)

// withCarVersion sets the CAR file version to be either v1 or v2
func withCarVersion(v CarVersion) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.carVersion = v
	}
}

func withOfflineDeal() dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.offlineDeal = true
	}
}

func withVerifiedDeal() dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.verifiedDeal = true
	}
}

func withProviderCollateral(amt abi.TokenAmount) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.providerCollateral = amt
	}
}

func withMinerAddr(addr address.Address) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.minerAddr = addr
	}
}

func withClientAddr(addr address.Address) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.clientAddr = addr
	}
}

func withPieceCid(c cid.Cid) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.pieceCid = c
	}
}

func withPieceSize(size abi.PaddedPieceSize) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.pieceSize = size
	}
}

func withUndefinedPieceCid() dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.undefinedPieceCid = true
	}
}

func withEpochs(start, end abi.ChainEpoch) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.startEpoch = start
		dc.endEpoch = end
	}
}

// Set the id of the deal that is returned from the call to SectorsStatus
func withSectorStatusDealId(id abi.DealID) dealProposalOpt {
	return func(dc *dealProposalConfig) {
		dc.sectorsStatusDealId = id
	}
}

func (ph *ProviderHarness) newDealBuilder(t *testing.T, seed int, opts ...dealProposalOpt) *testDealBuilder {
	tbuilder := &testDealBuilder{t: t, ph: ph}

	dc := &dealProposalConfig{
		normalFileSize:     2000000,
		verifiedDeal:       false,
		providerCollateral: abi.NewTokenAmount(1),
		minerAddr:          tbuilder.ph.MinerAddr,
		clientAddr:         tbuilder.ph.ClientAddr,
		pieceCid:           cid.Undef,
		undefinedPieceCid:  false,
		startEpoch:         50000,
		endEpoch:           800000,
		carVersion:         CarVersion2,
	}
	for _, opt := range opts {
		opt(dc)
	}

	// generate a CARv2 file using a random seed in the tempDir
	randomFilepath, err := testutil.CreateRandomFile(tbuilder.ph.TempDir, seed, dc.normalFileSize)
	require.NoError(tbuilder.t, err)
	rootCid, carFilePath, err := testutil.CreateDenseCARv2(tbuilder.ph.TempDir, randomFilepath)
	require.NoError(tbuilder.t, err)

	// if the file should be a version 1 car file
	if dc.carVersion == CarVersion1 {
		// Just get the data out of the car file and write it to disk
		// (a car v1 is just the data portion of the car v2)
		cv2r, err := carv2.OpenReader(carFilePath)
		require.NoError(tbuilder.t, err)
		r, err := cv2r.DataReader()
		require.NoError(tbuilder.t, err)
		carData, err := io.ReadAll(r)
		require.NoError(tbuilder.t, err)
		carv1File, err := os.CreateTemp(tbuilder.ph.TempDir, "v1.car")
		require.NoError(tbuilder.t, err)
		_, err = carv1File.Write(carData)
		require.NoError(tbuilder.t, err)
		err = carv1File.Close()
		require.NoError(tbuilder.t, err)
		carFilePath = carv1File.Name()
	}

	// generate CommP of the CARv2 file
	cidAndSize, err := GenerateCommPLocally(carFilePath)
	require.NoError(tbuilder.t, err)

	var pieceCid = cidAndSize.PieceCID
	if dc.pieceCid != cid.Undef {
		pieceCid = dc.pieceCid
	}
	if dc.undefinedPieceCid {
		pieceCid = cid.Undef
	}
	var pieceSize = cidAndSize.Size
	if dc.pieceSize != abi.PaddedPieceSize(0) {
		pieceSize = dc.pieceSize
	}

	// build the deal proposal
	proposal := market.DealProposal{
		PieceCID:             pieceCid,
		PieceSize:            pieceSize,
		VerifiedDeal:         dc.verifiedDeal,
		Client:               dc.clientAddr,
		Provider:             dc.minerAddr,
		Label:                dc.label,
		StartEpoch:           dc.startEpoch,
		EndEpoch:             dc.endEpoch,
		StoragePricePerEpoch: abi.NewTokenAmount(1),
		ProviderCollateral:   dc.providerCollateral,
		ClientCollateral:     abi.NewTokenAmount(1),
	}

	carv2Fileinfo, err := os.Stat(carFilePath)
	require.NoError(tbuilder.t, err)
	name := carv2Fileinfo.Name()

	req := tspttypes.HttpRequest{URL: "http://foo.bar"}
	xferParams, err := json.Marshal(req)
	require.NoError(t, err)

	// assemble the final deal params to send to the provider
	dealParams := &types.DealParams{
		DealUUID:  uuid.New(),
		IsOffline: dc.offlineDeal,
		ClientDealProposal: market.ClientDealProposal{
			Proposal: proposal,
			ClientSignature: acrypto.Signature{
				Type: acrypto.SigTypeBLS,
				Data: []byte("sig"),
			}, // We don't do signature verification in Boost SM testing.
		},
		DealDataRoot: rootCid,
		Transfer: types.Transfer{
			Type:   "http",
			Params: xferParams,
			Size:   uint64(carv2Fileinfo.Size()),
		},
		RemoveUnsealedCopy: true,
	}

	// Create a copy of the car file so that if the original car file gets
	// cleaned up after the deal is added to a sector, we still have a copy
	// we can use to compare with the contents of the unsealed file.
	carFileCopyPath := carFilePath + ".copy"
	err = copyFile(carFilePath, carFileCopyPath)
	require.NoError(tbuilder.t, err)
	td := &testDeal{
		ph:                tbuilder.ph,
		params:            dealParams,
		carv2FilePath:     carFilePath,
		carv2CopyFilePath: carFileCopyPath,
		carv2FileName:     name,
	}

	publishCid := testutil.GenerateCid()
	finalPublishCid := testutil.GenerateCid()

	dealId := abi.DealID(1)
	sectorsStatusDealId := dealId
	if dc.sectorsStatusDealId > abi.DealID(0) {
		dealId = dc.sectorsStatusDealId
	}

	sectorId := abi.SectorNumber(rand.Intn(100))
	offset := abi.PaddedPieceSize(rand.Intn(100))

	tbuilder.ms = tbuilder.ph.MinerStub.ForDeal(dealParams, publishCid, finalPublishCid, dealId, sectorsStatusDealId, sectorId, offset, carFileCopyPath)
	tbuilder.td = td
	return tbuilder
}

type minerStubCall struct {
	err      error
	blocking bool
	optional bool
}

type testDealBuilder struct {
	t  *testing.T
	td *testDeal
	ph *ProviderHarness

	ms               *smtestutil.MinerStubBuilder
	msNoOp           bool
	msCommp          *minerStubCall
	msPublish        *minerStubCall
	msPublishConfirm *minerStubCall
	msAddPiece       *minerStubCall
	msAnnounce       *minerStubCall
}

func (tbuilder *testDealBuilder) withPublishFailing(err error) *testDealBuilder {
	tbuilder.msPublish = &minerStubCall{err: err}
	return tbuilder
}

func (tbuilder *testDealBuilder) withPublishConfirmFailing(err error) *testDealBuilder {
	tbuilder.msPublishConfirm = &minerStubCall{err: err}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAddPieceFailing(err error) *testDealBuilder {
	tbuilder.msAddPiece = &minerStubCall{err: err}
	return tbuilder
}

func (tbuilder *testDealBuilder) withCommpFailing(err error) *testDealBuilder {
	tbuilder.msCommp = &minerStubCall{err: err}
	return tbuilder
}

func (tbuilder *testDealBuilder) withCommpBlocking(optional ...bool) *testDealBuilder {
	isOptional := false
	if len(optional) > 0 {
		isOptional = optional[0]
	}
	tbuilder.msCommp = &minerStubCall{blocking: true, optional: isOptional}
	return tbuilder
}

func (tbuilder *testDealBuilder) withCommpNonBlocking() *testDealBuilder {
	tbuilder.msCommp = &minerStubCall{blocking: false}
	return tbuilder
}

func (tbuilder *testDealBuilder) withPublishBlocking() *testDealBuilder {
	tbuilder.msPublish = &minerStubCall{blocking: true}
	return tbuilder
}

func (tbuilder *testDealBuilder) withPublishNonBlocking() *testDealBuilder {
	tbuilder.msPublish = &minerStubCall{blocking: false}
	return tbuilder
}

func (tbuilder *testDealBuilder) withPublishConfirmBlocking() *testDealBuilder {
	tbuilder.msPublishConfirm = &minerStubCall{blocking: true}
	return tbuilder
}

func (tbuilder *testDealBuilder) withPublishConfirmNonBlocking() *testDealBuilder {
	tbuilder.msPublishConfirm = &minerStubCall{blocking: false}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAddPieceBlocking() *testDealBuilder {
	tbuilder.msAddPiece = &minerStubCall{blocking: true}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAddPieceNonBlocking() *testDealBuilder {
	tbuilder.msAddPiece = &minerStubCall{blocking: false}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAnnounceBlocking() *testDealBuilder {
	tbuilder.msAnnounce = &minerStubCall{blocking: true}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAnnounceNonBlocking() *testDealBuilder {
	tbuilder.msAnnounce = &minerStubCall{blocking: true}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAllMinerCallsNonBlocking() *testDealBuilder {
	tbuilder.msCommp = &minerStubCall{blocking: false}
	tbuilder.msPublish = &minerStubCall{blocking: false}
	tbuilder.msPublishConfirm = &minerStubCall{blocking: false}
	tbuilder.msAddPiece = &minerStubCall{blocking: false}
	tbuilder.msAnnounce = &minerStubCall{blocking: false}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAllMinerCallsBlocking() *testDealBuilder {
	tbuilder.msCommp = &minerStubCall{blocking: true}
	tbuilder.msPublish = &minerStubCall{blocking: true}
	tbuilder.msPublishConfirm = &minerStubCall{blocking: true}
	tbuilder.msAddPiece = &minerStubCall{blocking: true}
	tbuilder.msAnnounce = &minerStubCall{blocking: true}

	return tbuilder
}

func (tbuilder *testDealBuilder) withBlockingHttpServer() *testDealBuilder {
	tbuilder.ph.BlockingServer.AddFile(tbuilder.td.carv2FileName)
	tbuilder.setTransferParams(tbuilder.td.ph.BlockingServer.URL)
	return tbuilder
}

func (tbuilder *testDealBuilder) withNoOpMinerStub() *testDealBuilder {
	tbuilder.msNoOp = true
	return tbuilder
}

func (tbuilder *testDealBuilder) withDisconnectingHttpServer() *testDealBuilder {
	tbuilder.setTransferParams(tbuilder.ph.DisconnectingServer.URL)
	return tbuilder
}

func (tbuilder *testDealBuilder) withNormalHttpServer() *testDealBuilder {
	tbuilder.setTransferParams(tbuilder.ph.NormalServer.URL)
	return tbuilder
}

func (tbuilder *testDealBuilder) setTransferParams(serverURL string) {
	transferParams := &tspttypes.HttpRequest{URL: serverURL + "/" + filepath.Base(tbuilder.td.carv2FilePath)}
	transferParamsJSON, err := json.Marshal(transferParams)
	if err != nil {
		panic(err)
	}
	tbuilder.td.params.Transfer.Params = transferParamsJSON
}

func (tbuilder *testDealBuilder) withDealParamAnnounce(announce bool) *testDealBuilder {
	tbuilder.td.params.SkipIPNIAnnounce = !announce
	return tbuilder
}

func (tbuilder *testDealBuilder) build() *testDeal {
	// if the miner stub is supposed to be a no-op, setup a no-op and don't build any other stub behaviour
	if tbuilder.msNoOp {
		tbuilder.ms.SetupNoOp()
	} else {
		tbuilder.buildCommp().buildPublish().buildPublishConfirm().buildAddPiece().buildAnnounce()
	}

	testDeal := tbuilder.td

	testDeal.stubOutput = tbuilder.ms.Output()
	testDeal.tBuilder = tbuilder
	return testDeal
}

func (tbuilder *testDealBuilder) buildCommp() *testDealBuilder {
	if tbuilder.msCommp != nil {
		if err := tbuilder.msCommp.err; err != nil {
			tbuilder.ms.SetupCommpFailure(err)
		} else {
			tbuilder.ms.SetupCommp(tbuilder.msCommp.blocking, tbuilder.msCommp.optional)
		}
	}

	return tbuilder
}

func (tbuilder *testDealBuilder) buildPublish() *testDealBuilder {
	if tbuilder.msPublish != nil {
		if err := tbuilder.msPublish.err; err != nil {
			tbuilder.ms.SetupPublishFailure(err)
		} else {
			tbuilder.ms.SetupPublish(tbuilder.msPublish.blocking)
		}
	}
	return tbuilder
}

func (tbuilder *testDealBuilder) buildPublishConfirm() *testDealBuilder {
	if tbuilder.msPublishConfirm != nil {
		if err := tbuilder.msPublishConfirm.err; err != nil {
			tbuilder.ms.SetupPublishConfirmFailure(err)
		} else {
			tbuilder.ms.SetupPublishConfirm(tbuilder.msPublishConfirm.blocking)
		}
	}

	return tbuilder
}

func (tbuilder *testDealBuilder) buildAddPiece() *testDealBuilder {
	if tbuilder.msAddPiece != nil {
		if err := tbuilder.msAddPiece.err; err != nil {
			tbuilder.ms.SetupAddPieceFailure(err)
		} else {
			tbuilder.ms.SetupAddPiece(tbuilder.msAddPiece.blocking)
		}
	}

	return tbuilder
}

func (tbuilder *testDealBuilder) buildAnnounce() *testDealBuilder {
	if tbuilder.msAnnounce != nil {
		tbuilder.ms.SetupAnnounce(tbuilder.msAnnounce.blocking, !tbuilder.td.params.SkipIPNIAnnounce)
	}
	return tbuilder
}

type testDeal struct {
	ph                *ProviderHarness
	params            *types.DealParams
	carv2FilePath     string
	carv2FileName     string
	carv2CopyFilePath string
	stubOutput        *smtestutil.StubbedMinerOutput
	sub               event.Subscription

	tBuilder *testDealBuilder
}

func (td *testDeal) executeAndSubscribeImportOfflineDeal(delAfterImport bool) error {
	pi, err := td.ph.Provider.ImportOfflineDealData(context.Background(), td.params.DealUUID, td.carv2FilePath, delAfterImport)
	if err != nil {
		return err
	}
	if !pi.Accepted {
		return errors.New("deal not accepted")
	}
	dh := td.ph.Provider.getDealHandler(td.params.DealUUID)
	sub, err := dh.subscribeUpdates()
	if err != nil {
		return err
	}
	td.sub = sub

	return nil
}

func (td *testDeal) executeAndSubscribe() error {
	dh, err := td.ph.Provider.mkAndInsertDealHandler(td.params.DealUUID)
	if err != nil {
		return err
	}
	sub, err := dh.subscribeUpdates()
	if err != nil {
		return err
	}
	td.sub = sub

	pi, err := td.ph.Provider.ExecuteDeal(context.Background(), td.params, "")
	if err != nil {
		return err
	}
	if !pi.Accepted {
		return fmt.Errorf("deal not accepted: %s", pi.Reason)
	}

	return nil
}

func (td *testDeal) waitForError(errContains string, retryType types.DealRetryType) error {
	if td.sub == nil {
		return errors.New("no subcription for deal")
	}

	for i := range td.sub.Out() {
		st := i.(types.ProviderDealState)
		if len(st.Err) != 0 {
			if !strings.Contains(st.Err, errContains) {
				return fmt.Errorf("actual error does not contain expected error, expected: %s, actual:%s", errContains, st.Err)
			}
			if st.Retry != retryType {
				return fmt.Errorf("retry type does not match expected type, expected: %s, actual:%s", retryType, st.Retry)
			}

			return nil
		}
	}

	return errors.New("did not get any error")
}

func (td *testDeal) waitForCheckpoint(cp dealcheckpoints.Checkpoint) error {
	if td.sub == nil {
		return errors.New("no subcription for deal")
	}

LOOP:
	for i := range td.sub.Out() {
		st := i.(types.ProviderDealState)
		if len(st.Err) != 0 {
			return errors.New(st.Err)
		}
		if st.Checkpoint == cp {
			break LOOP
		}
	}

	return nil
}

func (td *testDeal) waitForSealingState(secState lapi.SectorState) error {
	if td.sub == nil {
		return errors.New("no subcription for deal")
	}

	for i := range td.sub.Out() {
		st := i.(types.ProviderDealState)
		if len(st.Err) != 0 {
			return errors.New(st.Err)
		}
		si, err := td.ph.MockSealingPipelineAPI.SectorsStatus(context.Background(), st.SectorID, false)
		if err != nil {
			return err
		}
		if si.State == secState {
			return nil
		}
	}

	return fmt.Errorf("did not reach sealing state %s", secState)
}

func (td *testDeal) updateWithRestartedProvider(ph *ProviderHarness) *testDealBuilder {
	old := td.stubOutput

	td.ph = ph
	td.tBuilder.msCommp = nil
	td.tBuilder.msPublish = nil
	td.tBuilder.msAddPiece = nil
	td.tBuilder.msPublishConfirm = nil

	td.tBuilder.ph = ph
	td.tBuilder.td = td
	td.tBuilder.ms = ph.MinerStub.ForDeal(td.params, old.PublishCid, old.FinalPublishCid, old.DealID, old.SectorsStatusDealID, old.SectorID, old.Offset, old.CarFilePath)

	return td.tBuilder
}

func (td *testDeal) waitForAndAssert(t *testing.T, ctx context.Context, cp dealcheckpoints.Checkpoint) {
	require.NoError(t, td.waitForCheckpoint(cp))

	switch cp {
	case dealcheckpoints.Accepted:
		td.ph.AssertAccepted(t, ctx, td.params)
	case dealcheckpoints.Transferred:
		td.ph.AssertTransferred(t, ctx, td.params)
	case dealcheckpoints.Published:
		td.ph.AssertPublished(t, ctx, td.params, td.stubOutput)
	case dealcheckpoints.PublishConfirmed:
		td.ph.AssertPublishConfirmed(t, ctx, td.params, td.stubOutput)
	case dealcheckpoints.AddedPiece:
		td.ph.AssertPieceAdded(t, ctx, td.params, td.stubOutput, td.carv2CopyFilePath)
	case dealcheckpoints.IndexedAndAnnounced:
		td.ph.AssertDealIndexed(t, ctx, td.params, td.stubOutput)
	default:
		t.Fail()
	}
}

func (td *testDeal) unblockTransfer() {
	td.ph.BlockingServer.UnblockFile(td.carv2FileName)
}

func (td *testDeal) unblockCommp() {
	td.ph.MinerStub.UnblockCommp(td.params.DealUUID)
}

func (td *testDeal) unblockPublish() {
	td.ph.MinerStub.UnblockPublish(td.params.DealUUID)
}

func (td *testDeal) unblockWaitForPublish() {
	td.ph.MinerStub.UnblockWaitForPublish(td.params.DealUUID)
}

func (td *testDeal) unblockAddPiece() {
	td.ph.MinerStub.UnblockAddPiece(td.params.DealUUID)
}

func (td *testDeal) assertPieceAdded(t *testing.T, ctx context.Context) {
	td.ph.AssertPieceAdded(t, ctx, td.params, td.stubOutput, td.carv2CopyFilePath)
}

func (td *testDeal) assertDealFailedTransferNonRecoverable(t *testing.T, ctx context.Context, errStr string) {
	td.ph.AssertDealFailedTransferNonRecoverable(t, ctx, td.params, errStr)
}

func (td *testDeal) assertEventuallyDealCleanedup(t *testing.T, ctx context.Context) {
	td.ph.AssertEventuallyDealCleanedup(t, ctx, td.params)
}

func (td *testDeal) assertDealFailedNonRecoverable(t *testing.T, ctx context.Context, errContains string) {
	dbState, err := td.ph.DealsDB.ByID(ctx, td.params.DealUUID)
	require.NoError(t, err)

	require.NotEmpty(t, dbState.Err)
	require.Contains(t, dbState.Err, errContains)
	require.EqualValues(t, dealcheckpoints.Complete, dbState.Checkpoint)
	require.EqualValues(t, types.DealRetryFatal, dbState.Retry)
}

type mockAskStore struct {
	ask *legacytypes.StorageAsk
}

func (m *mockAskStore) SetAsk(price, verifiedPrice abi.TokenAmount, minPieceSize, maxPieceSize abi.PaddedPieceSize) {
	m.ask = &legacytypes.StorageAsk{
		Price:         price,
		VerifiedPrice: verifiedPrice,
		MinPieceSize:  minPieceSize,
		MaxPieceSize:  maxPieceSize,
	}
}

func (m *mockAskStore) GetAsk(miner address.Address) *legacytypes.SignedStorageAsk {
	return &legacytypes.SignedStorageAsk{
		Ask: m.ask,
	}
}

type mockSignatureVerifier struct {
	valid bool
	err   error
}

func (m *mockSignatureVerifier) VerifySignature(ctx context.Context, sig acrypto.Signature, addr address.Address, input []byte) (bool, error) {
	return m.valid, m.err
}

func copyFile(source string, dest string) error {
	input, err := os.ReadFile(source)
	if err != nil {
		return err
	}

	err = os.WriteFile(dest, input, 0644)
	if err != nil {
		return err
	}

	return nil
}

func mockTipset(minerAddr address.Address, height abi.ChainEpoch) (*chaintypes.TipSet, error) {
	dummyCid, _ := cid.Parse("bafkqaaa")
	return chaintypes.NewTipSet([]*chaintypes.BlockHeader{{
		Miner:                 minerAddr,
		Height:                height,
		ParentStateRoot:       dummyCid,
		Messages:              dummyCid,
		ParentMessageReceipts: dummyCid,
		BlockSig:              &acrypto.Signature{Type: acrypto.SigTypeBLS},
		BLSAggregate:          &acrypto.Signature{Type: acrypto.SigTypeBLS},
		Timestamp:             1,
	}})
}
