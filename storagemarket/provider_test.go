package storagemarket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket/smtestutil"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/boost/transport/httptransport"
	types2 "github.com/filecoin-project/boost/transport/types"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	acrypto "github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	"github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestSimpleDealHappy(t *testing.T) {
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t, ctx)
	defer harness.Stop()
	// start the provider test harness
	harness.Start(t, ctx)

	// build the deal proposal with the blocking http test server and a completely blocking miner stub
	td := harness.newDealBuilder(t, 1).withAllMinerCallsBlocking().withBlockingHttpServer().build()

	// execute deal
	require.NoError(t, td.execute())

	// wait for Accepted checkpoint
	td.waitForAndAssert(t, ctx, dealcheckpoints.Accepted)

	// unblock transfer -> wait for Transferred checkpoint and assert deals db and storage and fund manager
	td.unblockTransfer()
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
}

func TestMultipleDealsConcurrent(t *testing.T) {
	nDeals := 10
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t, ctx)
	defer harness.Stop()
	// start the provider test harness
	harness.Start(t, ctx)

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

func TestMultipleDealsConcurrentWithFundsAndStorage(t *testing.T) {
	nDeals := 10
	ctx := context.Background()

	// setup the provider test harness
	harness := NewHarness(t, ctx)
	defer harness.Stop()
	// start the provider test harness
	harness.Start(t, ctx)

	var errGrp errgroup.Group
	var tds []*testDeal
	totalStorage := uint64(0)
	totalCollat := abi.NewTokenAmount(0)
	totalPublish := abi.NewTokenAmount(0)
	// half the deals will finish, half will be blocked on the wait for publish call -> we will then assert that the funds and storage manager state is as expected
	for i := 0; i < nDeals; i++ {
		i := i
		var td *testDeal
		// for even numbered deals, we will never block
		if i%2 == 0 {
			// setup mock publish & add-piece expectations with non-blocking behaviours -> the associated tagged funds and storage will be released
			td = harness.newDealBuilder(t, i).withAllMinerCallsNonBlocking().withNormalHttpServer().build()
		} else {
			// for odd numbered deals, we will block on the publish-confirm step
			// setup mock publish & add-piece expectations with blocking wait-for-publish behaviours -> the associated tagged funds and storage will not be released
			td = harness.newDealBuilder(t, i).withPublishNonBlocking().withPublishConfirmBlocking().withAddPieceBlocking().withNormalHttpServer().build()
			totalStorage = totalStorage + td.params.Transfer.Size
			totalCollat = abi.NewTokenAmount(totalCollat.Add(totalCollat.Int, td.params.ClientDealProposal.Proposal.ProviderCollateral.Int).Int64())
			totalPublish = abi.NewTokenAmount(totalPublish.Add(totalPublish.Int, harness.MinPublishFees.Int).Int64())
		}

		tds = append(tds, td)

		errGrp.Go(func() error {
			err := td.execute()
			if err != nil {
				return err
			}
			var checkpoint dealcheckpoints.Checkpoint
			if i%2 == 0 {
				checkpoint = dealcheckpoints.AddedPiece
			} else {
				checkpoint = dealcheckpoints.Published
			}
			if err := td.waitForCheckpoint(checkpoint); err != nil {
				return err
			}

			return nil
		})
	}
	require.NoError(t, errGrp.Wait())

	for i := 0; i < nDeals; i++ {
		td := tds[i]
		if i%2 == 0 {
			td.assertPieceAdded(t, ctx)
		} else {
			td.assertDealPublished(t, ctx)
		}
	}

	harness.EventuallyAssertStorageFundState(t, ctx, totalStorage, totalPublish, totalCollat)

	// now confirm the publish for remaining deals and assert funds and storage
	for i := 0; i < nDeals; i++ {
		td := tds[i]
		if i%2 != 0 {
			td.unblockWaitForPublish()
			totalPublish = abi.NewTokenAmount(totalPublish.Sub(totalPublish.Int, harness.MinPublishFees.Int).Int64())
			totalCollat = abi.NewTokenAmount(totalCollat.Sub(totalCollat.Int, td.params.ClientDealProposal.Proposal.ProviderCollateral.Int).Int64())
		}
	}
	harness.EventuallyAssertStorageFundState(t, ctx, totalStorage, totalPublish, totalCollat)

	// now finish the remaining deals and assert funds and storage
	for i := 0; i < nDeals; i++ {
		td := tds[i]
		if i%2 != 0 {
			td.unblockAddPiece()
			totalStorage = totalStorage - td.params.Transfer.Size
		}
	}
	harness.EventuallyAssertNoTagged(t, ctx)
	// assert that piece has been added for the deals
	for i := 0; i < nDeals; i++ {
		if i%2 != 0 {
			td := tds[i]
			td.assertPieceAdded(t, ctx)
		}
	}
}

func TestDealFailuresHandlingNonRecoverableErrors(t *testing.T) {
	ctx := context.Background()
	// setup the provider test harness with a disconnecting server that disconnects after sending the given number of bytes
	harness := NewHarness(t, ctx, withHttpDisconnectServerAfter(1),
		withHttpTransportOpts([]httptransport.Option{httptransport.BackOffRetryOpt(1*time.Millisecond, 1*time.Millisecond, 2, 1)}))
	defer harness.Stop()
	// start the provider test harness
	harness.Start(t, ctx)

	// spin up four deals
	// deal 1 -> fails transfer, deal 2 -> fails publish, deal 3 -> fails publish confirm, deal 4 -> fails add piece
	publishErr := errors.New("publish failed")
	publishConfirmErr := errors.New("publish confirm error")
	addPieceErr := errors.New("add piece error")
	deals := []struct {
		dealBuilder func() *testDeal
		errContains string
	}{
		{
			dealBuilder: func() *testDeal {
				return harness.newDealBuilder(t, 1).withDisconnectingHttpServer().build()
			},
			errContains: "failed data transfer",
		},
		{
			dealBuilder: func() *testDeal {
				return harness.newDealBuilder(t, 1).withPublishFailing(publishErr).withNormalHttpServer().build()
			},
			errContains: publishErr.Error(),
		},
		{
			dealBuilder: func() *testDeal {
				return harness.newDealBuilder(t, 1).withPublishNonBlocking().withPublishConfirmFailing(publishConfirmErr).withNormalHttpServer().build()
			},
			errContains: publishConfirmErr.Error(),
		},
		{
			dealBuilder: func() *testDeal {
				return harness.newDealBuilder(t, 1).withPublishNonBlocking().
					withPublishConfirmNonBlocking().withAddPieceFailing(addPieceErr).withNormalHttpServer().build()
			},
			errContains: addPieceErr.Error(),
		},
	}

	tds := harness.executeNDealsConcurrentAndWaitFor(t, len(deals), func(i int) *testDeal {
		return deals[i].dealBuilder()
	}, func(i int, td *testDeal) error {
		return td.waitForError(deals[i].errContains)
	})

	// assert cleanup of deal and db state
	for i := range tds {
		td := tds[i]
		derr := deals[i].errContains
		td.assertEventuallyDealCleanedup(t, ctx)
		td.assertDealFailedNonRecoverable(t, ctx, derr)
	}

	// assert storage manager and funds
	harness.EventuallyAssertNoTagged(t, ctx)
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
			err := td.execute()
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
	h.AssertEventuallyDealCleanedup(t, ctx, dp.DealUUID)
	h.AssertDealDBState(t, ctx, dp, so.DealID, &so.FinalPublishCid, dealcheckpoints.AddedPiece, so.SectorID, so.Offset, dp.ClientDealProposal.Proposal.PieceSize.Unpadded().Padded(), "")
	// Assert that the original file data we sent matches what was sent to the sealer
	h.AssertSealedContents(t, carv2FilePath, *so.SealedBytes)
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
	cr, err := carv2.OpenReader(carV2FilePath)
	require.NoError(t, err)
	defer cr.Close()

	actual, err := ioutil.ReadAll(cr.DataReader())
	require.NoError(t, err)

	// the read-bytes also contains extra zeros for the padding magic, so just match without the padding bytes.
	require.EqualValues(t, actual, read[:len(actual)])
}

func (h *ProviderHarness) AssertEventuallyDealCleanedup(t *testing.T, ctx context.Context, dealUuid uuid.UUID) {
	dbState, err := h.DealsDB.ByID(ctx, dealUuid)
	require.NoError(t, err)
	// assert that the deal has been cleanedup and there are no leaks
	require.Eventually(t, func() bool {
		// deal handler should be deleted
		dh := h.Provider.getDealHandler(dbState.DealUuid)
		if dh != nil {
			return false
		}

		// the deal inbound file should no longer exist
		_, statErr := os.Stat(dbState.InboundFilePath)
		return statErr != nil
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
	GoMockCtrl          *gomock.Controller
	TempDir             string
	MinerAddr           address.Address
	ClientAddr          address.Address
	MockFullNode        *lotusmocks.MockFullNode
	MinerStub           *smtestutil.MinerStub
	DealsDB             *db.DealsDB
	FundsDB             *db.FundsDB
	StorageDB           *db.StorageDB
	PublishWallet       address.Address
	MinPublishFees      abi.TokenAmount
	MaxStagingDealBytes uint64

	Provider *Provider

	// http test servers
	NormalServer        *httptest.Server
	BlockingServer      *testutil.BlockingHttpTestServer
	DisconnectingServer *httptest.Server
}

type providerConfig struct {
	maxStagingDealBytes  uint64
	minPublishFees       abi.TokenAmount
	disconnectAfterEvery int64
	httpOpts             []httptransport.Option

	lockedFunds      big.Int
	escrowFunds      big.Int
	publishWalletBal int64
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

func NewHarness(t *testing.T, ctx context.Context, opts ...harnessOpt) *ProviderHarness {
	pc := &providerConfig{
		minPublishFees:       abi.NewTokenAmount(100),
		maxStagingDealBytes:  10000000000,
		disconnectAfterEvery: 1048600,
		lockedFunds:          big.NewInt(300),
		escrowFunds:          big.NewInt(500),
		publishWalletBal:     1000,
	}
	for _, opt := range opts {
		opt(pc)
	}
	// Create a temporary directory for all the tests.
	dir := t.TempDir()

	// setup mocks
	ctrl := gomock.NewController(t)
	fn := lotusmocks.NewMockFullNode(ctrl)
	bp := smtestutil.NewMinerStub(ctrl)

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
	mn := mocknet.New(ctx)
	h, err := mn.GenPeer()
	require.NoError(t, err)

	// setup the databases
	sqldb, err := db.CreateTmpDB(ctx)
	require.NoError(t, err)
	dealsDB := db.NewDealsDB(sqldb)

	// publish wallet
	pw, err := address.NewIDAddress(uint64(rand.Intn(100)))
	require.NoError(t, err)

	// create the harness with default values
	ph := &ProviderHarness{
		GoMockCtrl:          ctrl,
		TempDir:             dir,
		MinerAddr:           minerAddr,
		ClientAddr:          cAddr,
		NormalServer:        normalServer,
		BlockingServer:      blockingServer,
		DisconnectingServer: disconnServer,

		MockFullNode:        fn,
		DealsDB:             dealsDB,
		FundsDB:             db.NewFundsDB(sqldb),
		StorageDB:           db.NewStorageDB(sqldb),
		PublishWallet:       pw,
		MinerStub:           bp,
		MinPublishFees:      pc.minPublishFees,
		MaxStagingDealBytes: pc.maxStagingDealBytes,
	}

	// fund manager
	fminitF := fundmanager.New(fundmanager.Config{
		PubMsgBalMin: ph.MinPublishFees,
		PubMsgWallet: pw,
	})
	fm := fminitF(fn, sqldb)

	// storage manager
	fsRepo, err := repo.NewFS(dir)
	require.NoError(t, err)
	lr, err := fsRepo.Lock(repo.StorageMiner)
	require.NoError(t, err)
	smInitF := storagemanager.New(storagemanager.Config{
		MaxStagingDealsBytes: ph.MaxStagingDealBytes,
	})
	sm := smInitF(lr, sqldb)

	// deal filter
	df := func(ctx context.Context, deal types.DealParams) (bool, string, error) {
		return true, "", nil
	}

	prov, err := NewProvider("", h, sqldb, dealsDB, fm, sm, fn, bp, address.Undef, bp, nil, bp, df, pc.httpOpts...)
	require.NoError(t, err)
	prov.testMode = true
	ph.Provider = prov

	ph.MockFullNode.EXPECT().StateMarketBalance(gomock.Any(), gomock.Any(), gomock.Any()).Return(api.MarketBalance{
		Locked: pc.lockedFunds,
		Escrow: pc.escrowFunds,
	}, nil).AnyTimes()

	ph.MockFullNode.EXPECT().WalletBalance(gomock.Any(), ph.PublishWallet).Return(abi.NewTokenAmount(pc.publishWalletBal), nil).AnyTimes()

	return ph
}

func (h *ProviderHarness) Start(t *testing.T, ctx context.Context) {
	h.NormalServer.Start()
	h.BlockingServer.Start()
	h.DisconnectingServer.Start()
	require.NoError(t, h.Provider.Start(ctx))
}

func (h *ProviderHarness) Stop() {
	h.GoMockCtrl.Finish()
	h.NormalServer.Close()
	h.BlockingServer.Close()
	h.DisconnectingServer.Close()
}

type dealProposalConfig struct {
	normalFileSize int
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

func (ph *ProviderHarness) newDealBuilder(t *testing.T, seed int, opts ...dealProposalOpt) *testDealBuilder {
	tbuilder := &testDealBuilder{t: t, ph: ph}

	dc := &dealProposalConfig{
		normalFileSize: 2000000,
	}
	for _, opt := range opts {
		opt(dc)
	}

	// generate a CARv2 file using a random seed in the tempDir
	randomFilepath, err := testutil.CreateRandomFile(tbuilder.ph.TempDir, seed, dc.normalFileSize)
	require.NoError(tbuilder.t, err)
	rootCid, carV2FilePath, err := testutil.CreateDenseCARv2(tbuilder.ph.TempDir, randomFilepath)
	require.NoError(tbuilder.t, err)

	// generate CommP of the CARv2 file
	cidAndSize, err := GenerateCommP(carV2FilePath)
	require.NoError(tbuilder.t, err)

	// build the deal proposal
	proposal := market.DealProposal{
		PieceCID:             cidAndSize.PieceCID,
		PieceSize:            cidAndSize.PieceSize,
		VerifiedDeal:         false,
		Client:               tbuilder.ph.ClientAddr,
		Provider:             tbuilder.ph.MinerAddr,
		Label:                rootCid.String(),
		StartEpoch:           abi.ChainEpoch(rand.Intn(100000)),
		EndEpoch:             800000 + abi.ChainEpoch(rand.Intn(10000)),
		StoragePricePerEpoch: abi.NewTokenAmount(1),
		ProviderCollateral:   abi.NewTokenAmount(1),
		ClientCollateral:     abi.NewTokenAmount(1),
	}

	carv2Fileinfo, err := os.Stat(carV2FilePath)
	require.NoError(tbuilder.t, err)
	name := carv2Fileinfo.Name()

	// assemble the final deal params to send to the provider
	dealParams := &types.DealParams{
		DealUUID: uuid.New(),
		ClientDealProposal: market.ClientDealProposal{
			Proposal:        proposal,
			ClientSignature: acrypto.Signature{}, // We don't do signature verification in Boost SM testing.
		},
		DealDataRoot: rootCid,
		Transfer: types.Transfer{
			Type: "http",
			Size: uint64(carv2Fileinfo.Size()),
		},
	}

	td := &testDeal{
		ph:            tbuilder.ph,
		params:        dealParams,
		carv2FilePath: carV2FilePath,
		carv2FileName: name,
	}
	tbuilder.ms = tbuilder.ph.MinerStub.ForDeal(dealParams)
	tbuilder.td = td
	return tbuilder
}

type minerStubCall struct {
	err      error
	blocking bool
}

type testDealBuilder struct {
	t  *testing.T
	td *testDeal
	ph *ProviderHarness

	ms               *smtestutil.MinerStubBuilder
	msPublish        *minerStubCall
	msPublishConfirm *minerStubCall
	msAddPiece       *minerStubCall
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

func (tbuilder *testDealBuilder) withAllMinerCallsNonBlocking() *testDealBuilder {
	tbuilder.msPublish = &minerStubCall{blocking: false}
	tbuilder.msPublishConfirm = &minerStubCall{blocking: false}
	tbuilder.msAddPiece = &minerStubCall{blocking: false}
	return tbuilder
}

func (tbuilder *testDealBuilder) withAllMinerCallsBlocking() *testDealBuilder {
	tbuilder.msPublish = &minerStubCall{blocking: true}
	tbuilder.msPublishConfirm = &minerStubCall{blocking: true}
	tbuilder.msAddPiece = &minerStubCall{blocking: true}

	return tbuilder
}

func (tbuilder *testDealBuilder) withBlockingHttpServer() *testDealBuilder {
	tbuilder.ph.BlockingServer.AddFile(tbuilder.td.carv2FileName)
	tbuilder.setTransferParams(tbuilder.td.ph.BlockingServer.URL)
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
	transferParams := &types2.HttpRequest{URL: serverURL + "/" + filepath.Base(tbuilder.td.carv2FilePath)}
	transferParamsJSON, err := json.Marshal(transferParams)
	if err != nil {
		panic(err)
	}
	tbuilder.td.params.Transfer.Params = transferParamsJSON
}

func (tbuilder *testDealBuilder) build() *testDeal {
	if tbuilder.msPublish != nil {

		if err := tbuilder.msPublish.err; err != nil {
			tbuilder.ms.SetupPublishFailure(err)
		} else {
			tbuilder.ms.SetupPublish(tbuilder.msPublish.blocking)
		}
	}

	if tbuilder.msPublishConfirm != nil {
		if err := tbuilder.msPublishConfirm.err; err != nil {
			tbuilder.ms.SetupPublishConfirmFailure(err)
		} else {
			tbuilder.ms.SetupPublishConfirm(tbuilder.msPublishConfirm.blocking)
		}
	}

	if tbuilder.msAddPiece != nil {
		if err := tbuilder.msAddPiece.err; err != nil {
			tbuilder.ms.SetupAddPieceFailure(err)
		} else {
			tbuilder.ms.SetupAddPiece(tbuilder.msAddPiece.blocking)
		}
	}

	tbuilder.td.stubOutput = tbuilder.ms.Output()
	return tbuilder.td
}

type testDeal struct {
	ph            *ProviderHarness
	params        *types.DealParams
	carv2FilePath string
	carv2FileName string
	stubOutput    *smtestutil.StubbedMinerOutput
	sub           event.Subscription
}

func (td *testDeal) execute() error {
	pi, err := td.ph.Provider.ExecuteDeal(td.params, peer.ID(""))
	if err != nil {
		return err
	}
	if !pi.Accepted {
		return errors.New("deal not accepted")
	}

	sub, err := td.ph.Provider.SubscribeDealUpdates(td.params.DealUUID)
	if err != nil {
		return err
	}
	td.sub = sub
	return nil
}

func (td *testDeal) waitForError(errContains string) error {
	for i := range td.sub.Out() {
		st := i.(types.ProviderDealState)
		if len(st.Err) != 0 {
			if !strings.Contains(st.Err, errContains) {
				return fmt.Errorf("actual error does not contain expected error, expected: %s, actual:%s", errContains, st.Err)
			}

			return nil
		}
	}

	return errors.New("did not get any error")
}

func (td *testDeal) waitForCheckpoint(cp dealcheckpoints.Checkpoint) error {
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
		td.ph.AssertPieceAdded(t, ctx, td.params, td.stubOutput, td.carv2FilePath)
	}
}

func (td *testDeal) unblockTransfer() {
	td.ph.BlockingServer.UnblockFile(td.carv2FileName)
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
	td.ph.AssertPieceAdded(t, ctx, td.params, td.stubOutput, td.carv2FilePath)
}

func (td *testDeal) assertDealPublished(t *testing.T, ctx context.Context) {
	td.ph.AssertPublished(t, ctx, td.params, td.stubOutput)
}

func (td *testDeal) assertDealFailedTransferNonRecoverable(t *testing.T, ctx context.Context, errStr string) {
	td.ph.AssertDealFailedTransferNonRecoverable(t, ctx, td.params, errStr)
}

func (td *testDeal) assertEventuallyDealCleanedup(t *testing.T, ctx context.Context) {
	td.ph.AssertEventuallyDealCleanedup(t, ctx, td.params.DealUUID)
}

func (td *testDeal) assertDealFailedNonRecoverable(t *testing.T, ctx context.Context, errContains string) {
	dbState, err := td.ph.DealsDB.ByID(ctx, td.params.DealUUID)
	require.NoError(t, err)

	require.NotEmpty(t, dbState.Err)
	require.Contains(t, dbState.Err, errContains)
	require.EqualValues(t, dealcheckpoints.Complete, dbState.Checkpoint)
}
