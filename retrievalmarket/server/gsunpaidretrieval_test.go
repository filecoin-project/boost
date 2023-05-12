package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/boost-gfm/piecestore"
	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	retrievalimpl "github.com/filecoin-project/boost-gfm/retrievalmarket/impl"
	"github.com/filecoin-project/boost-gfm/retrievalmarket/impl/askstore"
	"github.com/filecoin-project/boost-gfm/retrievalmarket/impl/testnodes"
	rmnet "github.com/filecoin-project/boost-gfm/retrievalmarket/network"
	tut "github.com/filecoin-project/boost-gfm/shared_testutil"
	graphsync "github.com/filecoin-project/boost-graphsync"
	graphsyncimpl "github.com/filecoin-project/boost-graphsync/impl"
	"github.com/filecoin-project/boost-graphsync/network"
	"github.com/filecoin-project/boost-graphsync/storeutil"
	boosttu "github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	dtimpl "github.com/filecoin-project/go-data-transfer/impl"
	"github.com/filecoin-project/go-data-transfer/testutil"
	dtgstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	logging "github.com/ipfs/go-log/v2"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
)

var tlog = logging.Logger("testgs")

type testCase struct {
	name                      string
	reqPayloadCid             cid.Cid
	watch                     func(client retrievalmarket.RetrievalClient, gsupr *GraphsyncUnpaidRetrieval)
	ask                       *retrievalmarket.Ask
	noUnsealedCopy            bool
	expectErr                 bool
	expectClientCancelEvent   bool
	expectProviderCancelEvent bool
	expectRejection           string
}

var providerCancelled = errors.New("provider cancelled")
var clientCancelled = errors.New("client cancelled")
var clientRejected = errors.New("client received reject response")

func TestGS(t *testing.T) {
	// _ = logging.SetLogLevel("testgs", "debug")
	_ = logging.SetLogLevel("testgs", "info")
	//_ = logging.SetLogLevel("dt-impl", "debug")

	missingCid, err := cid.Parse("bafkqaaa")
	require.NoError(t, err)

	testCases := []testCase{{
		name: "happy path",
	}, {
		name:          "request missing payload cid",
		reqPayloadCid: missingCid,
		expectErr:     true,
	}, {
		name:            "request for piece with no unsealed sectors",
		noUnsealedCopy:  true,
		expectErr:       true,
		expectRejection: "no unsealed piece",
	}, {
		name: "request for non-zero price per byte",
		ask: &retrievalmarket.Ask{
			UnsealPrice:  abi.NewTokenAmount(0),
			PricePerByte: abi.NewTokenAmount(1),
		},
		expectErr:       true,
		expectRejection: "ask price is non-zero",
	}, {
		// Note: we disregard the unseal price because we only serve deals
		// with an unsealed piece, so the unseal price is irrelevant.
		// Therefore the retrieval should succeed for non-zero unseal price.
		name: "request for non-zero unseal price",
		ask: &retrievalmarket.Ask{
			UnsealPrice:  abi.NewTokenAmount(1),
			PricePerByte: abi.NewTokenAmount(0),
		},
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
		expectClientCancelEvent:   true,
		expectProviderCancelEvent: true,
	}, {
		name: "provider cancel request after sending 2 blocks",
		watch: func(client retrievalmarket.RetrievalClient, gsupr *GraphsyncUnpaidRetrieval) {
			count := 0
			gsupr.outgoingBlockHook = func(state *retrievalState) {
				count++
				if count == 2 {
					tlog.Debug("provider cancelling client deal")
					err := gsupr.CancelTransfer(context.TODO(), state.cs.transferID, &state.cs.recipient)
					require.NoError(t, err)
				}
				if count == 10 {
					tlog.Warn("sending last block but client cancel hasn't arrived yet")
				}
			}
		},
		expectErr:               true,
		expectClientCancelEvent: true,
	}, {
		name: "provider cancel request after sending 2 blocks without peer id",
		watch: func(client retrievalmarket.RetrievalClient, gsupr *GraphsyncUnpaidRetrieval) {
			count := 0
			gsupr.outgoingBlockHook = func(state *retrievalState) {
				count++
				if count == 2 {
					tlog.Debug("provider cancelling client deal")
					err := gsupr.CancelTransfer(context.TODO(), state.cs.transferID, nil)
					require.NoError(t, err)
				}
				if count == 10 {
					tlog.Warn("sending last block but client cancel hasn't arrived yet")
				}
			}
		},
		expectErr:               true,
		expectClientCancelEvent: true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			runRequestTest(t, tc)
		})
	}
}

func runRequestTest(t *testing.T, tc testCase) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

	askStore, err := askstore.NewAskStore(namespace.Wrap(testData.Ds1, datastore.NewKey("retrieval-ask")), datastore.NewKey("latest"))
	require.NoError(t, err)
	ask := &retrievalmarket.Ask{UnsealPrice: abi.NewTokenAmount(0), PricePerByte: abi.NewTokenAmount(0)}
	if tc.ask != nil {
		ask = tc.ask
	}
	err = askStore.SetAsk(ask)
	require.NoError(t, err)

	sectorAccessor := testnodes.NewTestSectorAccessor()
	if !tc.noUnsealedCopy {
		sectorAccessor.MarkUnsealed(ctx, sectorID, offset.Unpadded(), abi.UnpaddedPieceSize(len(carData)))
	}

	pieceStore := tut.NewTestPieceStore()
	sectorAccessor.ExpectUnseal(sectorID, offset.Unpadded(), abi.UnpaddedPieceSize(len(carData)), carData)
	dagstoreWrapper := tut.NewMockDagStoreWrapper(pieceStore, sectorAccessor)
	vdeps := ValidationDeps{
		DagStore:       dagstoreWrapper,
		PieceStore:     pieceStore,
		SectorAccessor: sectorAccessor,
		AskStore:       askStore,
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
	gsupr, err := NewGraphsyncUnpaidRetrieval(testData.Host2.ID(), gs2, testData.DTNet2, vdeps, nil)
	require.NoError(t, err)

	// Create the retrieval provider with the graphsync unpaid retrieval interceptor
	paymentAddress := address.TestAddress2
	provider := createRetrievalProvider(ctx, t, testData, pieceStore, sectorAccessor, dagstoreWrapper, gsupr, paymentAddress)

	gsupr.SubscribeToDataTransferEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		tlog.Debugf("prov dt: %s %s / %s", datatransfer.Events[event.Code], event.Message, datatransfer.Statuses[channelState.Status()])
	})
	err = gsupr.Start(ctx)
	require.NoError(t, err)
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
		case retrievalmarket.ClientEventDealRejected:
			clientResChan <- fmt.Errorf("%s :%w", state.Message, clientRejected)
		case retrievalmarket.ClientEventDataTransferError:
			clientResChan <- errors.New(state.Message)
		}
	})

	// Retrieve the data
	tlog.Infof("Retrieve cid %s from peer %s", carRootCid, retrievalPeer.ID)
	// Use an explore-all but add unixfs-preload to make sure we have UnixFS
	// ADL support wired up.
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreInterpretAs("unixfs-preload", ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	)).Node()
	params, err := retrievalmarket.NewParamsV1(abi.NewTokenAmount(0), 0, 0, sel, nil, abi.NewTokenAmount(0))
	require.NoError(t, err)
	if tc.reqPayloadCid != cid.Undef {
		carRootCid = tc.reqPayloadCid
	}
	_, err = client.Retrieve(ctx, 1, carRootCid, params, abi.NewTokenAmount(0), retrievalPeer, address.TestAddress, address.TestAddress2)
	require.NoError(t, err)

	// Wait for provider completion
	err = waitFor(ctx, t, providerResChan)
	if tc.expectErr || tc.expectProviderCancelEvent {
		require.Error(t, err)
		if tc.expectProviderCancelEvent {
			require.EqualError(t, err, providerCancelled.Error())
		}
	} else {
		require.NoError(t, err)
	}

	// Wait for client completion
	err = waitFor(ctx, t, clientResChan)
	if tc.expectErr || tc.expectClientCancelEvent {
		require.Error(t, err)
		if tc.expectClientCancelEvent {
			require.EqualError(t, err, clientCancelled.Error())
		} else if tc.expectRejection != "" {
			require.ErrorContains(t, err, tc.expectRejection)
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

	dt1.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		tlog.Debugf("client dt: %s %s / %s", datatransfer.Events[event.Code], event.Message, datatransfer.Statuses[channelState.Status()])
	})

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
