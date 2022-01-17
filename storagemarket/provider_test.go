package storagemarket

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/filecoin-project/boost/storagemarket/smtestutil"

	"github.com/libp2p/go-libp2p-core/event"

	carv2 "github.com/ipld/go-car/v2"

	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/lotus/api"

	types2 "github.com/filecoin-project/boost/transport/types"

	acrypto "github.com/filecoin-project/go-state-types/crypto"

	"github.com/filecoin-project/boost/testutil"
	"github.com/google/uuid"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-actors/actors/builtin/market"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"

	"testing"

	"github.com/filecoin-project/lotus/node/repo"
	"github.com/golang/mock/gomock"
	"github.com/libp2p/go-libp2p-core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

func TestSimpleDealHappy(t *testing.T) {
	ctx := context.Background()
	testMode = true
	defer func() {
		testMode = false
	}()

	// setup the provider test harness
	// TODO Do not hardcode these values
	minPublish := abi.NewTokenAmount(100)
	harness := NewHarness(t, ctx, 10000000000, minPublish)
	defer harness.Stop()
	// start the provider test harness
	harness.Start(t, ctx)

	// build the deal proposal
	dp, carV2FilePath := harness.mkDealProposal(t, harness.NormalServer.URL, 1)

	// setup mock publish & add-piece expectations
	so := harness.MinerStub.ForDeal(dp).SetupAllBlocking().Output()
	harness.setupWalletBalances(t, big.NewInt(300), big.NewInt(500), 1000)

	// execute deal
	sub, err := harness.executeDeal(dp)
	require.NoError(t, err)

	// wait for Transferred checkpoint and assert deals db and storage and fund manager
	require.NoError(t, harness.waitForCheckpoint(sub, dealcheckpoints.Transferred))
	harness.AssertTransferred(t, ctx, dp)
	harness.AssertStorageAndFundManagerState(t, ctx, dp.Transfer.Size, harness.MinPublishFees, dp.ClientDealProposal.Proposal.ProviderCollateral)

	// unblock publish -> wait for published checkpoint and assert
	harness.MinerStub.UnblockPublish(dp.DealUUID)
	require.NoError(t, harness.waitForCheckpoint(sub, dealcheckpoints.Published))
	harness.AssertPublished(t, ctx, dp, so)
	harness.AssertStorageAndFundManagerState(t, ctx, dp.Transfer.Size, harness.MinPublishFees, dp.ClientDealProposal.Proposal.ProviderCollateral)

	// unblock publish confirmation -> wait for publish confirmed and assert
	harness.MinerStub.UnblockWaitForPublish(dp.DealUUID)
	require.NoError(t, harness.waitForCheckpoint(sub, dealcheckpoints.PublishConfirmed))
	harness.AssertPublishConfirmed(t, ctx, dp, so)
	harness.EventuallyAssertStorageFundState(t, ctx, dp.Transfer.Size, abi.NewTokenAmount(0), abi.NewTokenAmount(0))

	// unblock adding piece -> wait for piece to be added and assert
	harness.MinerStub.UnblockAddPiece(dp.DealUUID)
	require.NoError(t, harness.waitForCheckpoint(sub, dealcheckpoints.AddedPiece))
	harness.AssertPieceAdded(t, ctx, dp, so, carV2FilePath)
	harness.EventuallyAssertNoTagged(t, ctx)
}

func TestMultipleDealsConcurrent(t *testing.T) {
	nDeals := 10
	ctx := context.Background()
	testMode = true
	defer func() {
		testMode = false
	}()

	// setup the provider test harness
	// TODO Do not hardcode these values
	minPublish := abi.NewTokenAmount(100)
	harness := NewHarness(t, ctx, 10000000000, minPublish)
	defer harness.Stop()
	// start the provider test harness
	harness.Start(t, ctx)

	// setup wallet balances
	harness.setupWalletBalances(t, big.NewInt(300), big.NewInt(500), 1000)

	var errGrp errgroup.Group
	var testDeals []*testDealInfo
	for i := 0; i < nDeals; i++ {
		i := i
		dp, carV2FilePath := harness.mkDealProposal(t, harness.NormalServer.URL, i)
		// setup mock publish & add-piece expectations
		so := harness.MinerStub.ForDeal(dp).SetupAllNonBlocking().Output()

		testDeals = append(testDeals, &testDealInfo{dp, so, carV2FilePath})

		errGrp.Go(func() error {
			sub, err := harness.executeDeal(dp)
			if err != nil {
				return err
			}
			if err := harness.waitForCheckpoint(sub, dealcheckpoints.AddedPiece); err != nil {
				return err
			}
			return nil
		})
	}

	require.NoError(t, errGrp.Wait())
	for i := 0; i < nDeals; i++ {
		td := testDeals[i]
		harness.AssertPieceAdded(t, ctx, td.dp, td.so, td.carV2FilePath)
	}

	harness.EventuallyAssertNoTagged(t, ctx)
}

func TestMultipleDealsConcurrentWithFundsAndStorage(t *testing.T) {
	nDeals := 10
	ctx := context.Background()
	testMode = true
	defer func() {
		testMode = false
	}()

	// setup the provider test harness
	// TODO Do not hardcode these values
	minPublish := abi.NewTokenAmount(100)
	harness := NewHarness(t, ctx, 10000000000, minPublish)
	defer harness.Stop()
	// start the provider test harness
	harness.Start(t, ctx)

	// setup wallet balances
	harness.setupWalletBalances(t, big.NewInt(300), big.NewInt(500), 1000)

	var errGrp errgroup.Group
	var tInfos []*testDealInfo
	totalStorage := uint64(0)
	totalCollat := abi.NewTokenAmount(0)
	totalPublish := abi.NewTokenAmount(0)
	// half the deals will finish, half will be blocked on the wait for publish call -> we will then assert that the funds and storage manager state is as expected
	for i := 0; i < nDeals; i++ {
		i := i
		dp, carV2FilePath := harness.mkDealProposal(t, harness.NormalServer.URL, i)
		var so *smtestutil.StubbedMinerOutput
		// for even numbered deals, we will never block
		if i%2 == 0 {
			// setup mock publish & add-piece expectations with non-blocking behaviours -> the associated tagged funds and storage will be released
			so = harness.MinerStub.ForDeal(dp).SetupAllNonBlocking().Output()
		} else {
			// for odd numbered deals, we will block on the publish-confirm step
			// setup mock publish & add-piece expectations with blocking wait-for-publish behaviours -> the associated tagged funds and storage will not be released
			so = harness.MinerStub.ForDeal(dp).SetupPublish(false).SetupPublishConfirm(true).SetupAddPiece(true).Output()
			totalStorage = totalStorage + dp.Transfer.Size
			totalCollat = abi.NewTokenAmount(totalCollat.Add(totalCollat.Int, dp.ClientDealProposal.Proposal.ProviderCollateral.Int).Int64())
			totalPublish = abi.NewTokenAmount(totalPublish.Add(totalPublish.Int, harness.MinPublishFees.Int).Int64())
		}

		tInfos = append(tInfos, &testDealInfo{dp, so, carV2FilePath})

		errGrp.Go(func() error {
			sub, err := harness.executeDeal(dp)
			if err != nil {
				return err
			}
			if i%2 == 0 {
				if err := harness.waitForCheckpoint(sub, dealcheckpoints.AddedPiece); err != nil {
					return err
				}
			} else {
				if err := harness.waitForCheckpoint(sub, dealcheckpoints.Published); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, errGrp.Wait())

	for i := 0; i < nDeals; i++ {
		ti := tInfos[i]
		if i%2 == 0 {
			harness.AssertPieceAdded(t, ctx, ti.dp, ti.so, ti.carV2FilePath)
		} else {
			harness.AssertPublished(t, ctx, ti.dp, ti.so)
		}
	}

	harness.EventuallyAssertStorageFundState(t, ctx, totalStorage, totalPublish, totalCollat)

	// now confirm the publish for remaining deals and assert funds and storage
	for i := 0; i < nDeals; i++ {
		ti := tInfos[i]
		if i%2 != 0 {
			harness.MinerStub.UnblockWaitForPublish(ti.dp.DealUUID)
			totalPublish = abi.NewTokenAmount(totalPublish.Sub(totalPublish.Int, harness.MinPublishFees.Int).Int64())
			totalCollat = abi.NewTokenAmount(totalCollat.Sub(totalCollat.Int, ti.dp.ClientDealProposal.Proposal.ProviderCollateral.Int).Int64())
		}
	}
	harness.EventuallyAssertStorageFundState(t, ctx, totalStorage, totalPublish, totalCollat)

	// now finish the remaining deals and assert funds and storage
	for i := 0; i < nDeals; i++ {
		ti := tInfos[i]
		if i%2 != 0 {
			harness.MinerStub.UnblockAddPiece(ti.dp.DealUUID)
			totalStorage = totalStorage - ti.dp.Transfer.Size
		}
	}
	harness.EventuallyAssertNoTagged(t, ctx)
	// assert that piece has been added for the deals
	for i := 0; i < nDeals; i++ {
		if i%2 != 0 {
			ti := tInfos[i]
			harness.AssertPieceAdded(t, ctx, ti.dp, ti.so, ti.carV2FilePath)
		}
	}
}

func (h *ProviderHarness) executeDeal(dp *types.DealParams) (event.Subscription, error) {
	pi, err := h.Provider.ExecuteDeal(dp, peer.ID(""))
	if err != nil {
		return nil, err
	}
	if !pi.Accepted {
		return nil, errors.New("deal not accepted")
	}

	sub, err := h.Provider.SubscribeDealUpdates(dp.DealUUID)
	if err != nil {
		return nil, err
	}
	return sub, nil
}

func (h *ProviderHarness) waitForCheckpoint(sub event.Subscription, cp dealcheckpoints.Checkpoint) error {
LOOP:
	for i := range sub.Out() {
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

func (h *ProviderHarness) AssertTransferred(t *testing.T, ctx context.Context, dp *types.DealParams) {
	h.AssertDealDBState(t, ctx, dp, abi.DealID(0), nil, dealcheckpoints.Transferred, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0))
}

func (h *ProviderHarness) AssertPublished(t *testing.T, ctx context.Context, dp *types.DealParams, so *smtestutil.StubbedMinerOutput) {
	h.AssertDealDBState(t, ctx, dp, abi.DealID(0), &so.PublishCid, dealcheckpoints.Published, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0))
}

func (h *ProviderHarness) AssertPublishConfirmed(t *testing.T, ctx context.Context, dp *types.DealParams, so *smtestutil.StubbedMinerOutput) {
	h.AssertDealDBState(t, ctx, dp, so.DealID, &so.FinalPublishCid, dealcheckpoints.PublishConfirmed, abi.SectorNumber(0), abi.PaddedPieceSize(0), abi.PaddedPieceSize(0))
}

func (h *ProviderHarness) AssertPieceAdded(t *testing.T, ctx context.Context, dp *types.DealParams, so *smtestutil.StubbedMinerOutput, carv2FilePath string) {
	h.AssertEventuallyDealCleanedup(t, ctx, dp.DealUUID)
	h.AssertDealDBState(t, ctx, dp, so.DealID, &so.FinalPublishCid, dealcheckpoints.AddedPiece, so.SectorID, so.Offset, dp.ClientDealProposal.Proposal.PieceSize.Unpadded().Padded())
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
	checkpoint dealcheckpoints.Checkpoint, sector abi.SectorNumber, offset, length abi.PaddedPieceSize) {
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

	if publishCid == nil {
		require.Empty(t, dbState.PublishCID)
	} else {
		require.EqualValues(t, publishCid, dbState.PublishCID)
	}
}

type ProviderHarness struct {
	GokMockCtrl    *gomock.Controller
	TempDir        string
	MinerAddr      address.Address
	ClientAddr     address.Address
	MockFullNode   *lotusmocks.MockFullNode
	MinerStub      *smtestutil.MinerStub
	DealsDB        *db.DealsDB
	FundsDB        *db.FundsDB
	StorageDB      *db.StorageDB
	PublishWallet  address.Address
	MinPublishFees abi.TokenAmount

	Provider *Provider

	// http test servers
	NormalServer *httptest.Server
}

func NewHarness(t *testing.T, ctx context.Context, maxStagingDealsBytes uint64, minPublishBal abi.TokenAmount) *ProviderHarness {
	// tempDir
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
	normalServer, err := testutil.HttpTestUnstartedFileServer(t, dir)
	require.NoError(t, err)

	// create a provider libp2p peer
	mn := mocknet.New(ctx)
	h, err := mn.GenPeer()
	require.NoError(t, err)

	// setup the databases
	sqldb, err := db.CreateTmpDB(ctx)
	require.NoError(t, err)
	dealsDB := db.NewDealsDB(sqldb)

	// fund manager
	pw, err := address.NewIDAddress(uint64(rand.Intn(100)))
	require.NoError(t, err)
	fminitF := fundmanager.New(fundmanager.Config{
		PubMsgBalMin: minPublishBal,
		PubMsgWallet: pw,
	})
	fm := fminitF(fn, sqldb)

	// storage manager
	smInitF := storagemanager.New(storagemanager.Config{
		MaxStagingDealsBytes: maxStagingDealsBytes,
	})
	fsRepo, err := repo.NewFS(dir)
	require.NoError(t, err)
	lr, err := fsRepo.Lock(repo.StorageMiner)
	require.NoError(t, err)
	sm := smInitF(lr, sqldb)
	prov, err := NewProvider("", h, sqldb, dealsDB, fm, sm, fn, bp, address.Undef, bp, nil, bp)
	require.NoError(t, err)

	return &ProviderHarness{
		GokMockCtrl:    ctrl,
		Provider:       prov,
		TempDir:        dir,
		MinerAddr:      minerAddr,
		ClientAddr:     cAddr,
		NormalServer:   normalServer,
		MockFullNode:   fn,
		DealsDB:        dealsDB,
		FundsDB:        db.NewFundsDB(sqldb),
		StorageDB:      db.NewStorageDB(sqldb),
		PublishWallet:  pw,
		MinerStub:      bp,
		MinPublishFees: minPublishBal,
	}
}

func (h *ProviderHarness) Start(t *testing.T, ctx context.Context) {
	h.NormalServer.Start()
	require.NoError(t, h.Provider.Start(ctx))
}

func (h *ProviderHarness) Stop() {
	h.GokMockCtrl.Finish()
	h.NormalServer.Close()
}

func (h *ProviderHarness) mkDealProposal(t *testing.T, serverURL string, seed int) (dp *types.DealParams, carV2FilePath string) {
	// generate a CARv2 file using a random seed in the tempDir
	randomFilepath, err := testutil.CreateRandomFile(h.TempDir, seed, 2000000)
	require.NoError(t, err)
	rootCid, carV2FilePath, err := testutil.CreateDenseCARv2(h.TempDir, randomFilepath)
	require.NoError(t, err)

	// generate CommP of the CARv2 file
	cidAndSize, err := GenerateCommP(carV2FilePath)
	require.NoError(t, err)

	// build the deal proposal
	proposal := market.DealProposal{
		PieceCID:             cidAndSize.PieceCID,
		PieceSize:            cidAndSize.PieceSize,
		VerifiedDeal:         false,
		Client:               h.ClientAddr,
		Provider:             h.MinerAddr,
		Label:                rootCid.String(),
		StartEpoch:           abi.ChainEpoch(rand.Intn(100000)),
		EndEpoch:             800000 + abi.ChainEpoch(rand.Intn(10000)),
		StoragePricePerEpoch: abi.NewTokenAmount(1),
		ProviderCollateral:   abi.NewTokenAmount(1),
		ClientCollateral:     abi.NewTokenAmount(1),
	}

	// build the transfer params to send in the deal proposal
	transferParams := &types2.HttpRequest{URL: serverURL + "/" + filepath.Base(carV2FilePath)}
	transferParamsJSON, err := json.Marshal(transferParams)
	require.NoError(t, err)

	carv2Fileinfo, err := os.Stat(carV2FilePath)
	require.NoError(t, err)

	// assemble the final deal params to send to the provider
	dealParams := types.DealParams{
		DealUUID: uuid.New(),
		ClientDealProposal: market.ClientDealProposal{
			Proposal:        proposal,
			ClientSignature: acrypto.Signature{}, // We don't do signature verification in Boost SM testing.
		},
		DealDataRoot: rootCid,
		Transfer: types.Transfer{
			Type:   "http",
			Params: transferParamsJSON,
			Size:   uint64(carv2Fileinfo.Size()),
		},
	}
	return &dealParams, carV2FilePath
}

func (h *ProviderHarness) setupWalletBalances(t *testing.T, locked big.Int, escrow big.Int, publishWalletBal int64) {
	h.MockFullNode.EXPECT().StateMarketBalance(gomock.Any(), gomock.Any(), gomock.Any()).Return(api.MarketBalance{
		Locked: locked,
		Escrow: escrow,
	}, nil).AnyTimes()

	h.MockFullNode.EXPECT().WalletBalance(gomock.Any(), h.PublishWallet).Return(abi.NewTokenAmount(publishWalletBal), nil).AnyTimes()
}

type testDealInfo struct {
	dp            *types.DealParams
	so            *smtestutil.StubbedMinerOutput
	carV2FilePath string
}
