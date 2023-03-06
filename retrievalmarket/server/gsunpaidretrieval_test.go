package server

import (
	"bytes"
	"context"
	"errors"
	boosttu "github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	dtimpl "github.com/filecoin-project/go-data-transfer/impl"
	"github.com/filecoin-project/go-data-transfer/testutil"
	dtgstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	retrievalimpl "github.com/filecoin-project/go-fil-markets/retrievalmarket/impl"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/testnodes"
	rmnet "github.com/filecoin-project/go-fil-markets/retrievalmarket/network"
	tut "github.com/filecoin-project/go-fil-markets/shared_testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-graphsync"
	graphsyncimpl "github.com/ipfs/go-graphsync/impl"
	"github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	logging "github.com/ipfs/go-log/v2"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
	"time"
)

var tlog = logging.Logger("testgs")

type testCase struct {
	name          string
	reqMissingCid bool
	watch         func(client retrievalmarket.RetrievalClient, gsupr *GraphsyncUnpaidRetrieval)
	expectErr     bool
	expectCancel  bool
}

var providerCancelled = errors.New("provider cancelled")
var clientCancelled = errors.New("client cancelled")

func TestGS(t *testing.T) {
	//_ = logging.SetLogLevel("testgs", "debug")
	_ = logging.SetLogLevel("testgs", "info")
	//_ = logging.SetLogLevel("dt-impl", "debug")

	testCases := []testCase{{
		name: "happy path",
	}, {
		name:          "request missing payload cid",
		reqMissingCid: true,
		expectErr:     true,
	}, {
		name: "cancel request after sending 2 blocks",
		watch: func(client retrievalmarket.RetrievalClient, gsupr *GraphsyncUnpaidRetrieval) {
			count := 0
			gsupr.outgoingBlockHook = func(state *retrievalState) {
				count++
				if count == 2 {
					tlog.Debug("cancelling client deal")
					err := client.CancelDeal(state.mkts.ID)
					require.NoError(t, err)
				}
				if count == 10 {
					tlog.Warn("sending last block but client cancel hasn't arrived yet")
				}
			}
		},
		expectCancel: true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runRequestTest(t, tc)
		})
	}
}

func runRequestTest(t *testing.T, tc testCase) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a CAR file and set up mocks
	testData := tut.NewLibp2pTestData(ctx, t)
	carRootCid, carData := createCarV1(t)
	sectorID := abi.SectorNumber(1)
	offset := abi.PaddedPieceSize(0)
	pieceInfo := piecestore.PieceInfo{
		PieceCID: tut.GenerateCids(1)[0],
		Deals: []piecestore.DealInfo{
			{
				DealID:   abi.DealID(1),
				SectorID: sectorID,
				Offset:   offset,
				Length:   abi.UnpaddedPieceSize(len(carData)).Padded(),
			},
		},
	}

	pieceStore := tut.NewTestPieceStore()
	sectorAccessor := testnodes.NewTestSectorAccessor()
	sectorAccessor.ExpectUnseal(sectorID, offset.Unpadded(), abi.UnpaddedPieceSize(len(carData)), carData)
	dagstoreWrapper := tut.NewMockDagStoreWrapper(pieceStore, sectorAccessor)
	vdeps := ValidationDeps{
		DagStore:       dagstoreWrapper,
		PieceStore:     pieceStore,
		SectorAccessor: sectorAccessor,
	}

	expectedPiece := pieceInfo.PieceCID
	pieceStore.ExpectPiece(expectedPiece, pieceInfo)
	pieceStore.ExpectCID(carRootCid, piecestore.CIDInfo{
		PieceBlockLocations: []piecestore.PieceBlockLocation{
			{
				PieceCID: expectedPiece,
			},
		},
	})
	dagstoreWrapper.AddBlockToPieceIndex(carRootCid, expectedPiece)

	// Create a blockstore over the CAR file blocks
	carDataBuff := bytes.NewReader(carData)
	carDataBs, err := blockstore.NewReadOnly(carDataBuff, nil, carv2.ZeroLengthSectionAsEOF(true), blockstore.UseWholeCIDs(true))
	require.NoError(t, err)

	// Wrap graphsync with the graphsync unpaid retrieval interceptor
	linkSystem2 := storeutil.LinkSystemForBlockstore(carDataBs)
	gs2 := graphsyncimpl.New(ctx, network.NewFromLibp2pHost(testData.Host2), linkSystem2)
	gsupr, err := NewGraphsyncUnpaidRetrieval(testData.Host2.ID(), gs2, testData.DTNet2, vdeps)
	require.NoError(t, err)

	// Create the retrieval provider with the graphsync unpaid retrieval interceptor
	paymentAddress := address.TestAddress2
	provider := createRetrievalProvider(ctx, t, testData, pieceStore, sectorAccessor, dagstoreWrapper, gsupr, paymentAddress)

	gsupr.SubscribeToDataTransferEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		tlog.Debugf("prov dt: %s %s / %s", datatransfer.Events[event.Code], event.Message, datatransfer.Statuses[channelState.Status()])
	})
	gsupr.Start(ctx)
	tut.StartAndWaitForReady(ctx, t, provider)

	// Create a retrieval client
	retrievalPeer := retrievalmarket.RetrievalPeer{
		Address: paymentAddress,
		ID:      testData.Host2.ID(),
	}
	retrievalClientNode := testnodes.NewTestRetrievalClientNode(testnodes.TestRetrievalClientNodeParams{})
	retrievalClientNode.ExpectKnownAddresses(retrievalPeer, nil)
	client := createRetrievalClient(ctx, t, testData, retrievalClientNode)
	tut.StartAndWaitForReady(ctx, t, client)

	if tc.watch != nil {
		tc.watch(client, gsupr)
	}

	// Watch for provider completion
	providerResChan := make(chan error)
	gsupr.SubscribeToMarketsEvents(func(event retrievalmarket.ProviderEvent, state retrievalmarket.ProviderDealState) {
		tlog.Debugf("prov mkt: %s %s %s", retrievalmarket.ProviderEvents[event], state.Status.String(), state.Message)
		switch event {
		case retrievalmarket.ProviderEventComplete:
			providerResChan <- nil
		case retrievalmarket.ProviderEventCancelComplete:
			providerResChan <- providerCancelled
		case retrievalmarket.ProviderEventDataTransferError:
			providerResChan <- errors.New(state.Message)
		}
	})

	// Watch for client completion
	clientResChan := make(chan error)
	client.SubscribeToEvents(func(event retrievalmarket.ClientEvent, state retrievalmarket.ClientDealState) {
		tlog.Debugf("clnt mkt: %s %s %s", event.String(), state.Status.String(), state.Message)
		switch event {
		case retrievalmarket.ClientEventComplete:
			clientResChan <- nil
		case retrievalmarket.ClientEventCancelComplete:
			clientResChan <- clientCancelled
		case retrievalmarket.ClientEventDataTransferError:
			clientResChan <- errors.New(state.Message)
		}
	})

	// Retrieve the data
	tlog.Infof("Retrieve cid %s from peer %s", carRootCid, retrievalPeer.ID)
	sel := selectorparse.CommonSelector_ExploreAllRecursively
	params, err := retrievalmarket.NewParamsV1(abi.NewTokenAmount(0), 0, 0, sel, nil, abi.NewTokenAmount(0))
	require.NoError(t, err)
	if tc.reqMissingCid {
		carRootCid, err = cid.Parse("bafkqaaa")
		require.NoError(t, err)
	}
	_, err = client.Retrieve(ctx, 1, carRootCid, params, abi.NewTokenAmount(0), retrievalPeer, address.TestAddress, address.TestAddress2)
	require.NoError(t, err)

	// Wait for provider completion
	err = waitFor(ctx, t, providerResChan)
	if tc.expectErr || tc.expectCancel {
		require.Error(t, err)
		if tc.expectCancel {
			require.EqualError(t, err, providerCancelled.Error())
		}
	} else {
		require.NoError(t, err)
	}

	// Wait for client completion
	err = waitFor(ctx, t, clientResChan)
	if tc.expectErr || tc.expectCancel {
		require.Error(t, err)
		if tc.expectCancel {
			require.EqualError(t, err, clientCancelled.Error())
		}
	} else {
		require.NoError(t, err)
	}
}

func createRetrievalProvider(ctx context.Context, t *testing.T, testData *tut.Libp2pTestData, pieceStore *tut.TestPieceStore, sectorAccessor *testnodes.TestSectorAccessor, dagstoreWrapper *tut.MockDagStoreWrapper, gs graphsync.GraphExchange, paymentAddress address.Address) retrievalmarket.RetrievalProvider {
	nw2 := rmnet.NewFromLibp2pHost(testData.Host2, rmnet.RetryParameters(0, 0, 0, 0))
	dtTransport2 := dtgstransport.NewTransport(testData.Host2.ID(), gs)
	dt2, err := dtimpl.NewDataTransfer(testData.DTStore2, testData.DTNet2, dtTransport2)
	require.NoError(t, err)
	testutil.StartAndWaitForReady(ctx, t, dt2)
	providerDs := namespace.Wrap(testData.Ds2, datastore.NewKey("/retrievals/provider"))
	priceFunc := func(ctx context.Context, dealPricingParams retrievalmarket.PricingInput) (retrievalmarket.Ask, error) {
		return retrievalmarket.Ask{
			UnsealPrice:  abi.NewTokenAmount(0),
			PricePerByte: abi.NewTokenAmount(0),
		}, nil
	}
	providerNode := testnodes.NewTestRetrievalProviderNode()
	provider, err := retrievalimpl.NewProvider(
		paymentAddress, providerNode, sectorAccessor, nw2, pieceStore, dagstoreWrapper, dt2, providerDs,
		priceFunc)
	require.NoError(t, err)
	return provider
}

func createRetrievalClient(ctx context.Context, t *testing.T, testData *tut.Libp2pTestData, retrievalClientNode *testnodes.TestRetrievalClientNode) retrievalmarket.RetrievalClient {
	nw1 := rmnet.NewFromLibp2pHost(testData.Host1, rmnet.RetryParameters(0, 0, 0, 0))
	gs1 := graphsyncimpl.New(ctx, network.NewFromLibp2pHost(testData.Host1), testData.LinkSystem1)
	dtTransport1 := dtgstransport.NewTransport(testData.Host1.ID(), gs1)
	dt1, err := dtimpl.NewDataTransfer(testData.DTStore1, testData.DTNet1, dtTransport1)
	require.NoError(t, err)
	testutil.StartAndWaitForReady(ctx, t, dt1)
	require.NoError(t, err)
	clientDs := namespace.Wrap(testData.Ds1, datastore.NewKey("/retrievals/client"))
	ba := tut.NewTestRetrievalBlockstoreAccessor()
	client, err := retrievalimpl.NewClient(nw1, dt1, retrievalClientNode, &tut.TestPeerResolver{}, clientDs, ba)
	require.NoError(t, err)
	return client
}

func createCarV1(t *testing.T) (cid.Cid, []byte) {
	rf, err := boosttu.CreateRandomFile(t.TempDir(), int(time.Now().Unix()), 2*1024*1024)
	require.NoError(t, err)

	// carv1
	caropts := []carv2.Option{
		blockstore.WriteAsCarV1(true),
	}

	root, cn, err := boosttu.CreateDenseCARWith(t.TempDir(), rf, 128*1024, 1024, caropts)
	require.NoError(t, err)

	file, err := os.Open(cn)
	require.NoError(t, err)
	defer file.Close()

	reader, err := carv2.NewReader(file)
	require.NoError(t, err)

	v1Reader, err := reader.DataReader()
	require.NoError(t, err)

	bz, err := io.ReadAll(v1Reader)
	require.NoError(t, err)

	return root, bz
}

func waitFor(ctx context.Context, t *testing.T, resChan chan error) error {
	var err error
	select {
	case <-ctx.Done():
		require.Fail(t, "test timed out")
	case err = <-resChan:
	}
	return err
}
