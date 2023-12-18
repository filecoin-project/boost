package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	graphsyncimpl "github.com/filecoin-project/boost-graphsync/impl"
	gsnet "github.com/filecoin-project/boost-graphsync/network"
	"github.com/filecoin-project/boost-graphsync/storeutil"
	clinode "github.com/filecoin-project/boost/cli/node"
	"github.com/filecoin-project/boost/datatransfer"
	dtgstransport "github.com/filecoin-project/boost/datatransfer/transport/graphsync"
	"github.com/filecoin-project/boost/markets/utils"
	"github.com/filecoin-project/boost/piecedirectory"
	gsclient "github.com/filecoin-project/boost/retrievalmarket/client"
	"github.com/filecoin-project/boost/retrievalmarket/testutil"
	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	bdclientutil "github.com/filecoin-project/boostd-data/clientutil"
	"github.com/filecoin-project/boostd-data/model"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	test "github.com/filecoin-project/lotus/chain/events/state/mock"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	blocksutil "github.com/ipfs/go-ipfs-blocksutil"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
	"golang.org/x/term"
)

var tlog = logging.Logger("testgs")

type testCase struct {
	name                      string
	reqPayloadCid             cid.Cid
	watch                     func(client *gsclient.Client, gsupr *GraphsyncUnpaidRetrieval)
	ask                       *legacyretrievaltypes.Ask
	noUnsealedCopy            bool
	expectErr                 bool
	expectClientCancelEvent   bool
	expectProviderCancelEvent bool
	expectRejection           string
}

var providerCancelled = errors.New("provider cancelled")

//var clientCancelled = errors.New("client cancelled")
//var clientRejected = errors.New("client received reject response")

func TestGS(t *testing.T) {
	t.Skip("refactor tests to use boost client")
	//_ = logging.SetLogLevel("testgs", "debug")
	_ = logging.SetLogLevel("testgs", "info")
	_ = logging.SetLogLevel("dt-impl", "debug")

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
		ask: &legacyretrievaltypes.Ask{
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
		ask: &legacyretrievaltypes.Ask{
			UnsealPrice:  abi.NewTokenAmount(1),
			PricePerByte: abi.NewTokenAmount(0),
		},
	}, {
		name: "provider cancel request after sending 2 blocks",
		watch: func(client *gsclient.Client, gsupr *GraphsyncUnpaidRetrieval) {
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
		watch: func(client *gsclient.Client, gsupr *GraphsyncUnpaidRetrieval) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create a CAR file and set up mocks
	testData := testutil.NewLibp2pTestData(ctx, t)

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
	pieceCid := GenerateCids(1)[0]
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

	cl := bdclientutil.NewTestStore(ctx)
	defer cl.Close(ctx)

	pd := piecedirectory.NewPieceDirectory(cl, pr, 1)
	pd.Start(ctx)
	err = pd.AddDealForPiece(ctx, pieceCid, dealInfo)
	require.NoError(t, err)

	sa := &mockSectorAccessor{
		unsealed: !tc.noUnsealedCopy,
	}
	vdeps := ValidationDeps{
		PieceDirectory: pd,
		SectorAccessor: sa,
		AskStore:       NewRetrievalAskGetter(),
	}

	// Create a blockstore over the CAR file blocks
	carDataBs, err := pd.GetBlockstore(ctx, pieceCid)
	require.NoError(t, err)

	// Wrap graphsync with the graphsync unpaid retrieval interceptor
	linkSystem2 := storeutil.LinkSystemForBlockstore(carDataBs)
	gs2 := graphsyncimpl.New(ctx, gsnet.NewFromLibp2pHost(testData.Host2), linkSystem2)
	gsupr, err := NewGraphsyncUnpaidRetrieval(testData.Host2.ID(), gs2, testData.DTNet2, vdeps)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	fn := lotusmocks.NewMockFullNode(ctrl)
	peerID := testData.Host2.ID()
	var maddrs []abi.Multiaddrs
	for _, mma := range testData.Host2.Addrs() {
		maddrs = append(maddrs, mma.Bytes())
	}
	minfo := api.MinerInfo{
		PeerId:     &peerID,
		Multiaddrs: maddrs,
		Worker:     address.TestAddress2,
	}
	fn.EXPECT().StateMinerInfo(gomock.Any(), gomock.Any(), gomock.Any()).Return(minfo, nil).AnyTimes()
	chainHead, err := test.MockTipset(maddr, 1)
	require.NoError(t, err)
	fn.EXPECT().ChainHead(gomock.Any()).Return(chainHead, nil).AnyTimes()

	queryHandler := NewQueryAskHandler(testData.Host2, maddr, pd, sa, NewRetrievalAskGetter(), fn)
	queryHandler.Start()
	defer queryHandler.Stop()

	// Create a Graphsync transport and call SetEventHandler, which registers
	// listeners for all the Graphsync hooks.
	gsTransport := dtgstransport.NewTransport(testData.Host2.ID(), gsupr)
	err = gsTransport.SetEventHandler(nil)
	require.NoError(t, err)

	gsupr.SubscribeToDataTransferEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		tlog.Debugf("prov dt: %s %s / %s", datatransfer.Events[event.Code], event.Message, datatransfer.Statuses[channelState.Status()])
	})
	err = gsupr.Start(ctx)
	require.NoError(t, err)

	client := newTestClient(t, testData, fn)

	if tc.watch != nil {
		tc.watch(client, gsupr)
	}

	// Watch for provider completion
	providerResChan := make(chan error)
	gsupr.SubscribeToMarketsEvents(func(event legacyretrievaltypes.ProviderEvent, state legacyretrievaltypes.ProviderDealState) {
		tlog.Debugf("prov mkt: %s %s %s", legacyretrievaltypes.ProviderEvents[event], state.Status.String(), state.Message)
		switch event {
		case legacyretrievaltypes.ProviderEventComplete:
			providerResChan <- nil
		case legacyretrievaltypes.ProviderEventCancelComplete:
			providerResChan <- providerCancelled
		case legacyretrievaltypes.ProviderEventDataTransferError:
			providerResChan <- errors.New(state.Message)
		}
	})

	// Retrieve the data
	tlog.Infof("Retrieve cid %s from peer %s", carRootCid, client.ClientAddr.String())
	// Use an explore-all but add unixfs-preload to make sure we have UnixFS
	// ADL support wired up.
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	sel := ssb.ExploreInterpretAs("unixfs-preload", ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	)).Node()

	query, err := client.RetrievalQuery(ctx, maddr, pieceCid)
	require.NoError(t, err)

	proposal, err := gsclient.RetrievalProposalForAsk(query, carRootCid, sel)
	require.NoError(t, err)

	// Retrieve the data
	_, err = client.RetrieveContentWithProgressCallback(
		ctx,
		maddr,
		proposal,
		func(bytesReceived_ uint64) {
			printProgress(bytesReceived_)
		},
	)
	require.NoError(t, err)

	dservOffline := merkledag.NewDAGService(blockservice.New(testData.Bs1, offline.Exchange(testData.Bs1)))

	// if we used a selector - need to find the sub-root the user actually wanted to retrieve
	if sel != nil {
		var subRootFound bool
		err = utils.TraverseDag(
			ctx,
			dservOffline,
			carRootCid,
			sel,
			func(p traversal.Progress, n ipld.Node, r traversal.VisitReason) error {
				if r == traversal.VisitReason_SelectionMatch {

					require.Equal(t, p.LastBlock.Path.String(), p.Path.String())

					cidLnk, castOK := p.LastBlock.Link.(cidlink.Link)
					require.True(t, castOK)

					carRootCid = cidLnk.Cid
					subRootFound = true
				}
				return nil
			},
		)
		require.NoError(t, err)

		require.True(t, subRootFound)
	}

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

	//final verification -- the server has no active graphsync requests
	stats := gsupr.GraphExchange.Stats()
	require.Equal(t, stats.IncomingRequests.Active, uint64(0))
}

func newTestClient(t *testing.T, testData *testutil.Libp2pTestData, full api.FullNode) *gsclient.Client {
	clientPath, err := os.MkdirTemp(t.TempDir(), "client")
	require.NoError(t, err)

	clientNode, err := clinode.Setup(clientPath)
	require.NoError(t, err)
	clientNode.Host = testData.Host1
	//err = clientNode.Wallet.SetDefault(address.TestAddress2)
	//require.NoError(t, err)
	clientDs := namespace.Wrap(testData.Ds1, datastore.NewKey("/retrievals/client"))
	addr, err := clientNode.Wallet.GetDefault()
	require.NoError(t, err)

	// Create the retrieval client
	fc, err := gsclient.NewClient(clientNode.Host, full, clientNode.Wallet, addr, testData.Bs1, clientDs, clientPath)
	require.NoError(t, err)
	return fc
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

func printProgress(bytesReceived uint64) {
	str := fmt.Sprintf("%v (%v)", bytesReceived, humanize.IBytes(bytesReceived))

	termWidth, _, err := term.GetSize(int(os.Stdin.Fd()))
	strLen := len(str)
	if err == nil {

		if strLen < termWidth {
			// If the string is shorter than the terminal width, pad right side
			// with spaces to remove old text
			str = strings.Join([]string{str, strings.Repeat(" ", termWidth-strLen)}, "")
		} else if strLen > termWidth {
			// If the string doesn't fit in the terminal, cut it down to a size
			// that fits
			str = str[:termWidth]
		}
	}

	fmt.Fprintf(os.Stderr, "%s\r", str)
}

// GenerateCids produces n content identifiers.
func GenerateCids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	for i := 0; i < n; i++ {
		c := blockGenerator.Next().Cid()
		cids = append(cids, c)
	}
	return cids
}

var blockGenerator = blocksutil.NewBlockGenerator()
