package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/boost-gfm/retrievalmarket"
	retrievalimpl "github.com/filecoin-project/boost-gfm/retrievalmarket/impl"
	"github.com/filecoin-project/boost-gfm/retrievalmarket/impl/askstore"
	"github.com/filecoin-project/boost-gfm/retrievalmarket/impl/testnodes"
	rmnet "github.com/filecoin-project/boost-gfm/retrievalmarket/network"
	tut "github.com/filecoin-project/boost-gfm/shared_testutil"
	graphsyncimpl "github.com/filecoin-project/boost-graphsync/impl"
	"github.com/filecoin-project/boost-graphsync/network"
	"github.com/filecoin-project/boost-graphsync/storeutil"
	bdclientutil "github.com/filecoin-project/boost/extern/boostd-data/clientutil"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/go-address"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	dtimpl "github.com/filecoin-project/go-data-transfer/impl"
	"github.com/filecoin-project/go-data-transfer/testutil"
	dtgstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2"
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
	//_ = logging.SetLogLevel("testgs", "debug")
	_ = logging.SetLogLevel("testgs", "info")
	//_ = logging.SetLogLevel("dt-impl", "debug")

	missingCid := cid.MustParse("baguqeeraaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

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

	carRootCid, carFilePath := piecedirectory.CreateCarFile(t)
	carFile, err := os.Open(carFilePath)
	require.NoError(t, err)
	defer carFile.Close()

	// Create a random CAR file
	carReader, err := car.OpenReader(carFilePath)
	require.NoError(t, err)
	defer carReader.Close()
	carv1Reader, err := carReader.DataReader()
	require.NoError(t, err)

	// Any calls to get a reader over data should return a reader over the random CAR file
	pr := piecedirectory.CreateMockPieceReader(t, carv1Reader)

	carv1Bytes, err := io.ReadAll(carv1Reader)
	require.NoError(t, err)
	carSize := len(carv1Bytes)

	maddr := address.TestAddress
	pieceCid := tut.GenerateCids(1)[0]
	sectorID := abi.SectorNumber(1)
	offset := abi.PaddedPieceSize(0)
	dealInfo := model.DealInfo{
		DealUuid:    uuid.New().String(),
		ChainDealID: abi.DealID(1),
		MinerAddr:   maddr,
		SectorID:    sectorID,
		PieceOffset: offset,
		PieceLength: abi.UnpaddedPieceSize(carSize).Padded(),
	}

	askStore, err := askstore.NewAskStore(namespace.Wrap(testData.Ds1, datastore.NewKey("retrieval-ask")), datastore.NewKey("latest"))
	require.NoError(t, err)
	ask := &retrievalmarket.Ask{UnsealPrice: abi.NewTokenAmount(0), PricePerByte: abi.NewTokenAmount(0)}
	if tc.ask != nil {
		ask = tc.ask
	}
	err = askStore.SetAsk(ask)
	require.NoError(t, err)

	cl := bdclientutil.NewTestStore(ctx)
	defer cl.Close(ctx)

	pd := piecedirectory.NewPieceDirectory(cl, pr, 1)
	pd.Start(ctx)
	err = pd.AddDealForPiece(ctx, pieceCid, dealInfo)
	require.NoError(t, err)

	vdeps := ValidationDeps{
		PieceDirectory: pd,
		SectorAccessor: &mockSectorAccessor{
			unsealed: !tc.noUnsealedCopy,
		},
		AskStore: askStore,
	}

	// Create a blockstore over the CAR file blocks
	carDataBs, err := pd.GetBlockstore(ctx, pieceCid)
	require.NoError(t, err)

	// Wrap graphsync with the graphsync unpaid retrieval interceptor
	linkSystem2 := storeutil.LinkSystemForBlockstore(carDataBs)
	gs2 := graphsyncimpl.New(ctx, network.NewFromLibp2pHost(testData.Host2), linkSystem2)
	gsupr, err := NewGraphsyncUnpaidRetrieval(testData.Host2.ID(), gs2, testData.DTNet2, vdeps, nil)
	require.NoError(t, err)

	// Create a Graphsync transport and call SetEventHandler, which registers
	// listeners for all the Graphsync hooks.
	gsTransport := dtgstransport.NewTransport(testData.Host2.ID(), gsupr)
	err = gsTransport.SetEventHandler(nil)
	require.NoError(t, err)

	// Create the retrieval provider with the graphsync unpaid retrieval interceptor
	paymentAddress := address.TestAddress2

	gsupr.SubscribeToDataTransferEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		tlog.Debugf("prov dt: %s %s / %s", datatransfer.Events[event.Code], event.Message, datatransfer.Statuses[channelState.Status()])
	})
	err = gsupr.Start(ctx)
	require.NoError(t, err)

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

	// final verification -- the server has no active graphsync requests
	stats := gsupr.GraphExchange.Stats()
	require.Equal(t, stats.IncomingRequests.Active, uint64(0))
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

func waitFor(ctx context.Context, t *testing.T, resChan chan error) error {
	var err error
	select {
	case <-ctx.Done():
		require.Fail(t, "test timed out")
	case err = <-resChan:
	}
	return err
}

type mockSectorAccessor struct {
	unsealed bool
}

func (m *mockSectorAccessor) IsUnsealed(ctx context.Context, minerAddr address.Address, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error) {
	return m.unsealed, nil
}
