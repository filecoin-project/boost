package retrievalmarket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/filecoin-project/boost/retrievalmarket/types"
	"github.com/filecoin-project/boost/testutil"
	"github.com/filecoin-project/dagstore/mount"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	retrievalimpl "github.com/filecoin-project/go-fil-markets/retrievalmarket/impl"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/askstore"
	"github.com/filecoin-project/go-fil-markets/shared"
	tut "github.com/filecoin-project/go-fil-markets/shared_testutil"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin/v8/market"
	acrypto "github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api"
	lotusmocks "github.com/filecoin-project/lotus/api/mocks"
	"github.com/filecoin-project/lotus/api/v1api"
	ctypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	dss "github.com/ipfs/go-datastore/sync"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDynamicPricing(t *testing.T) {
	expectedAddress := address.TestAddress2
	expectedMinerWallet := address.TestAddress
	expectedConfig := Config{
		HTTPRetrievalURL: "",
	}
	payloadCID := testutil.GenerateCids(1)[0]
	peer1 := peer.ID("peer1")
	peer2 := peer.ID("peer2")

	// differential price per byte
	expectedppbUnVerified := abi.NewTokenAmount(4321)
	expectedppbVerified := abi.NewTokenAmount(2)

	// differential sealing/unsealing price
	expectedUnsealPrice := abi.NewTokenAmount(100)
	expectedUnsealDiscount := abi.NewTokenAmount(1)

	// differential payment interval
	expectedpiPeer1 := uint64(4567)
	expectedpiPeer2 := uint64(20)

	expectedPaymentIntervalIncrease := uint64(100)

	// multiple pieces have the same payload
	expectedPieceCID1 := testutil.GenerateCids(1)[0]
	expectedPieceCID2 := testutil.GenerateCids(1)[0]

	// sizes
	piece1SizePadded := uint64(1234)
	piece1Size := uint64(abi.PaddedPieceSize(piece1SizePadded).Unpadded())

	piece2SizePadded := uint64(2234)
	piece2Size := uint64(abi.PaddedPieceSize(piece2SizePadded).Unpadded())

	piece1 := piecestore.PieceInfo{
		PieceCID: expectedPieceCID1,
		Deals: []piecestore.DealInfo{
			{
				DealID: abi.DealID(1),
				Length: abi.PaddedPieceSize(piece1SizePadded),
			},
			{
				DealID: abi.DealID(11),
				Length: abi.PaddedPieceSize(piece1SizePadded),
			},
		},
	}

	piece2 := piecestore.PieceInfo{
		PieceCID: expectedPieceCID2,
		Deals: []piecestore.DealInfo{
			{
				DealID: abi.DealID(2),
				Length: abi.PaddedPieceSize(piece2SizePadded),
			},
			{
				DealID: abi.DealID(22),
				Length: abi.PaddedPieceSize(piece2SizePadded),
			},
			{
				DealID: abi.DealID(222),
				Length: abi.PaddedPieceSize(piece2SizePadded),
			},
		},
	}

	dPriceFunc := func(ctx context.Context, dealPricingParams retrievalmarket.PricingInput) (retrievalmarket.Ask, error) {
		ask := retrievalmarket.Ask{}

		if dealPricingParams.VerifiedDeal {
			ask.PricePerByte = expectedppbVerified
		} else {
			ask.PricePerByte = expectedppbUnVerified
		}

		if dealPricingParams.Unsealed {
			ask.UnsealPrice = expectedUnsealDiscount
		} else {
			ask.UnsealPrice = expectedUnsealPrice
		}

		fmt.Println("\n client is", dealPricingParams.Client.String())
		if dealPricingParams.Client == peer2 {
			ask.PaymentInterval = expectedpiPeer2
		} else {
			ask.PaymentInterval = expectedpiPeer1
		}
		ask.PaymentIntervalIncrease = expectedPaymentIntervalIncrease

		return ask, nil
	}

	buildProvider := func(
		t *testing.T,
		sa dagstore.SectorAccessor,
		fn v1api.FullNode,
		pieceStore piecestore.PieceStore,
		dagStore *tut.MockDagStoreWrapper,
		pFnc RetrievalPricingFunc,
		askStore retrievalmarket.AskStore,
	) *Provider {
		c, err := NewProvider(expectedConfig, expectedAddress, fn, sa, pieceStore, dagStore, askStore, &mockSignatureVerifier{true, nil}, pFnc)
		require.NoError(t, err)
		return c
	}

	mockStorageDealResults := func(n *lotusmocks.MockFullNode, pieceInfo piecestore.PieceInfo, verified bool) {
		for _, deal := range pieceInfo.Deals {
			n.EXPECT().StateMarketStorageDeal(gomock.Any(), deal.DealID, ctypes.EmptyTSK).AnyTimes().Return(&api.MarketDeal{
				Proposal: market.DealProposal{
					PieceCID:     pieceInfo.PieceCID,
					PieceSize:    deal.Length,
					VerifiedDeal: verified,
				},
			}, nil)
		}
	}

	tcs := map[string]struct {
		query              types.Query
		peerID             peer.ID
		nodeFunc           func(n *lotusmocks.MockFullNode)
		expFunc            func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper)
		sectorAccessorFunc func(sa *TestSectorAccessor)
		setAsk             func(ask *retrievalmarket.Ask)
		pricingFnc         RetrievalPricingFunc

		expectedPricePerByte            abi.TokenAmount
		expectedPaymentInterval         uint64
		expectedPaymentIntervalIncrease uint64
		expectedUnsealPrice             abi.TokenAmount
		expectedSize                    uint64
	}{
		// Retrieval request for a payloadCid without a pieceCid
		"pieceCid no-op: quote correct price for sealed, unverified, peer1": {
			query:  types.Query{PayloadCID: &payloadCID},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, false)
				mockStorageDealResults(n, piece2, false)
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				pieceStore.ExpectPiece(expectedPieceCID2, piece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			pricingFnc: dPriceFunc,

			expectedPricePerByte:            expectedppbUnVerified,
			expectedPaymentInterval:         expectedpiPeer1,
			expectedUnsealPrice:             expectedUnsealPrice,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece1Size,
		},

		"pieceCid no-op: quote correct price for sealed, unverified, peer2": {
			query:  types.Query{PayloadCID: &payloadCID},
			peerID: peer2,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, false)
				mockStorageDealResults(n, piece2, false)
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				pieceStore.ExpectPiece(expectedPieceCID2, piece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			pricingFnc: dPriceFunc,

			expectedPricePerByte:            expectedppbUnVerified,
			expectedPaymentInterval:         expectedpiPeer2,
			expectedUnsealPrice:             expectedUnsealPrice,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece1Size,
		},

		"pieceCid no-op: quote correct price for sealed, verified, peer1": {
			query:  types.Query{PayloadCID: &payloadCID},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, true)
				mockStorageDealResults(n, piece2, true)
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				pieceStore.ExpectPiece(expectedPieceCID2, piece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			pricingFnc: dPriceFunc,

			expectedPricePerByte:            expectedppbVerified,
			expectedPaymentInterval:         expectedpiPeer1,
			expectedUnsealPrice:             expectedUnsealPrice,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece1Size,
		},

		"pieceCid no-op: quote correct price for unsealed, unverified, peer1": {
			query:  types.Query{PayloadCID: &payloadCID},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, false)
				mockStorageDealResults(n, piece2, false)
			},
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := piece2.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				//pieceStore.ExpectCID(payloadCID, expectedCIDInfo)
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				pieceStore.ExpectPiece(expectedPieceCID2, piece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			pricingFnc: dPriceFunc,

			expectedPricePerByte:            expectedppbUnVerified,
			expectedPaymentInterval:         expectedpiPeer1,
			expectedUnsealPrice:             expectedUnsealDiscount,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece2Size,
		},

		"pieceCid no-op: quote correct price for unsealed, verified, peer1": {
			query:  types.Query{PayloadCID: &payloadCID},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, true)
				mockStorageDealResults(n, piece2, true)
			},
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := piece2.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				pieceStore.ExpectPiece(expectedPieceCID2, piece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			pricingFnc: dPriceFunc,

			expectedPricePerByte:            expectedppbVerified,
			expectedPaymentInterval:         expectedpiPeer1,
			expectedUnsealPrice:             expectedUnsealDiscount,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece2Size,
		},

		"pieceCid no-op: quote correct price for unsealed, verified, peer1 using default pricing policy if data transfer fee set to zero for verified deals": {
			query:  types.Query{PayloadCID: &payloadCID},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, true)
				mockStorageDealResults(n, piece2, true)
			},
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := piece2.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				pieceStore.ExpectPiece(expectedPieceCID2, piece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},

			setAsk: func(ask *retrievalmarket.Ask) {
				ask.PaymentInterval = expectedpiPeer1
				ask.PaymentIntervalIncrease = expectedPaymentIntervalIncrease
			},

			pricingFnc: retrievalimpl.DefaultPricingFunc(true),

			expectedPricePerByte:            big.Zero(),
			expectedUnsealPrice:             big.Zero(),
			expectedPaymentInterval:         expectedpiPeer1,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece2Size,
		},

		"pieceCid no-op: quote correct price for unsealed, verified, peer1 using default pricing policy if data transfer fee not set to zero for verified deals": {
			query:  types.Query{PayloadCID: &payloadCID},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, true)
				mockStorageDealResults(n, piece2, true)
			},
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := piece2.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				pieceStore.ExpectPiece(expectedPieceCID2, piece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},

			setAsk: func(ask *retrievalmarket.Ask) {
				ask.PricePerByte = expectedppbVerified
				ask.PaymentInterval = expectedpiPeer1
				ask.PaymentIntervalIncrease = expectedPaymentIntervalIncrease
			},

			pricingFnc: retrievalimpl.DefaultPricingFunc(false),

			expectedPricePerByte:            expectedppbVerified,
			expectedUnsealPrice:             big.Zero(),
			expectedPaymentInterval:         expectedpiPeer1,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece2Size,
		},

		"pieceCid no-op: quote correct price for sealed, verified, peer1 using default pricing policy": {
			query:  types.Query{PayloadCID: &payloadCID},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, true)
				mockStorageDealResults(n, piece2, true)
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				pieceStore.ExpectPiece(expectedPieceCID2, piece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			setAsk: func(ask *retrievalmarket.Ask) {
				ask.PricePerByte = expectedppbVerified
				ask.PaymentInterval = expectedpiPeer1
				ask.PaymentIntervalIncrease = expectedPaymentIntervalIncrease
				ask.UnsealPrice = expectedUnsealPrice
			},
			pricingFnc: retrievalimpl.DefaultPricingFunc(false),

			expectedPricePerByte:            expectedppbVerified,
			expectedPaymentInterval:         expectedpiPeer1,
			expectedUnsealPrice:             expectedUnsealPrice,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece1Size,
		},

		// Retrieval requests for a payloadCid inside a specific piece Cid
		"specific sealed piece Cid, first piece Cid matches: quote correct price for sealed, unverified, peer1": {
			query: types.Query{
				PayloadCID: &payloadCID,
				PieceCID:   &expectedPieceCID1,
			},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, false)
			},
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := piece2.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
			},
			pricingFnc: dPriceFunc,

			expectedPricePerByte:            expectedppbUnVerified,
			expectedPaymentInterval:         expectedpiPeer1,
			expectedUnsealPrice:             expectedUnsealPrice,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece1Size,
		},

		"specific sealed piece Cid, second piece Cid matches: quote correct price for sealed, unverified, peer1": {
			query: types.Query{
				PayloadCID: &payloadCID,
				PieceCID:   &expectedPieceCID2,
			},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece2, false)
			},
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := piece1.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				pieceStore.ExpectPiece(expectedPieceCID1, piece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			pricingFnc: dPriceFunc,

			expectedPricePerByte:            expectedppbUnVerified,
			expectedPaymentInterval:         expectedpiPeer1,
			expectedUnsealPrice:             expectedUnsealPrice,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece2Size,
		},

		"specific sealed piece Cid, first piece Cid matches: quote correct price for sealed, verified, peer1": {
			query: types.Query{
				PayloadCID: &payloadCID,
				PieceCID:   &expectedPieceCID1,
			},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, true)
			},
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := piece2.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
			},
			pricingFnc: dPriceFunc,

			expectedPricePerByte:            expectedppbVerified,
			expectedPaymentInterval:         expectedpiPeer1,
			expectedUnsealPrice:             expectedUnsealPrice,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece1Size,
		},

		"specific sealed piece Cid, first piece Cid matches: quote correct price for unsealed, verified, peer1": {
			query: types.Query{
				PayloadCID: &payloadCID,
				PieceCID:   &expectedPieceCID1,
			},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, true)
			},
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := piece1.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
			},
			pricingFnc: dPriceFunc,

			expectedPricePerByte:            expectedppbVerified,
			expectedPaymentInterval:         expectedpiPeer1,
			expectedUnsealPrice:             expectedUnsealDiscount,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece1Size,
		},

		"specific sealed piece Cid, second piece Cid matches: quote correct price for unsealed, verified, peer2": {
			query: types.Query{
				PayloadCID: &payloadCID,
				PieceCID:   &expectedPieceCID2,
			},
			peerID: peer2,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece2, true)
			},
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := piece2.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				pieceStore.ExpectPiece(expectedPieceCID2, piece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			pricingFnc: dPriceFunc,

			expectedPricePerByte:            expectedppbVerified,
			expectedPaymentInterval:         expectedpiPeer2,
			expectedUnsealPrice:             expectedUnsealDiscount,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece2Size,
		},

		"pieceCid no-op: quote correct price for sealed, unverified, peer1 based on a pre-existing ask": {
			query:  types.Query{PayloadCID: &payloadCID},
			peerID: peer1,
			nodeFunc: func(n *lotusmocks.MockFullNode) {
				mockStorageDealResults(n, piece1, false)
				mockStorageDealResults(n, piece2, false)
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID1, piece1)
				pieceStore.ExpectPiece(expectedPieceCID2, piece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID1)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			setAsk: func(ask *retrievalmarket.Ask) {
				ask.PricePerByte = expectedppbUnVerified
				ask.UnsealPrice = expectedUnsealPrice
			},
			pricingFnc: func(ctx context.Context, dealPricingParams retrievalmarket.PricingInput) (retrievalmarket.Ask, error) {
				ask, _ := dPriceFunc(ctx, dealPricingParams)
				ppb := big.Add(ask.PricePerByte, dealPricingParams.CurrentAsk.PricePerByte)
				unseal := big.Add(ask.UnsealPrice, dealPricingParams.CurrentAsk.UnsealPrice)
				ask.PricePerByte = ppb
				ask.UnsealPrice = unseal
				return ask, nil
			},

			expectedPricePerByte:            big.Mul(expectedppbUnVerified, big.NewInt(2)),
			expectedPaymentInterval:         expectedpiPeer1,
			expectedUnsealPrice:             big.Mul(expectedUnsealPrice, big.NewInt(2)),
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedSize:                    piece1Size,
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			fn := lotusmocks.NewMockFullNode(ctrl)
			fn.EXPECT().StateMinerInfo(gomock.Any(), expectedAddress, ctypes.EmptyTSK).AnyTimes().Return(api.MinerInfo{Worker: expectedMinerWallet}, nil)

			sectorAccessor := NewTestSectorAccessor()

			pieceStore := tut.NewTestPieceStore()
			dagStore := tut.NewMockDagStoreWrapper(pieceStore, sectorAccessor)
			tc.nodeFunc(fn)
			if tc.sectorAccessorFunc != nil {
				tc.sectorAccessorFunc(sectorAccessor)
			}
			tc.expFunc(t, pieceStore, dagStore)

			ds := dss.MutexWrap(datastore.NewMapDatastore())

			askStore, err := askstore.NewAskStore(namespace.Wrap(ds, datastore.NewKey("retrieval-ask")), datastore.NewKey("latest"))
			require.NoError(t, err)
			if tc.setAsk != nil {
				ask := askStore.GetAsk()
				tc.setAsk(ask)
				err = askStore.SetAsk(ask)
				require.NoError(t, err)
			}
			p := buildProvider(t, sectorAccessor, fn, pieceStore, dagStore, tc.pricingFnc, askStore)
			actualResp := p.ExecuteQuery(&types.SignedQuery{
				Query: tc.query,
			}, tc.peerID)
			pieceStore.VerifyExpectations(t)
			ctrl.Finish()

			require.Equal(t, types.QueryResponseAvailable, actualResp.Status)
			require.Empty(t, actualResp.Error)
			require.Nil(t, actualResp.Protocols.HTTPFilecoinV1)
			graphsyncResponse := actualResp.Protocols.GraphsyncFilecoinV1
			require.NotNil(t, graphsyncResponse)
			require.Equal(t, expectedMinerWallet, graphsyncResponse.PaymentAddress)
			require.Equal(t, tc.expectedPricePerByte, graphsyncResponse.MinPricePerByte)
			require.Equal(t, tc.expectedUnsealPrice, graphsyncResponse.UnsealPrice)
			require.Equal(t, tc.expectedPaymentInterval, graphsyncResponse.MaxPaymentInterval)
			require.Equal(t, tc.expectedPaymentIntervalIncrease, graphsyncResponse.MaxPaymentIntervalIncrease)
			require.Equal(t, tc.expectedSize, graphsyncResponse.Size)
		})
	}
}

func TestHandleQueryStream(t *testing.T) {

	payloadCID := tut.GenerateCids(1)[0]
	expectedPeer := peer.ID("somepeer")
	paddedSize := uint64(1234)
	expectedSize := uint64(abi.PaddedPieceSize(paddedSize).Unpadded())

	paddedSize2 := uint64(2234)
	expectedSize2 := uint64(abi.PaddedPieceSize(paddedSize2).Unpadded())

	expectedPieceCID := tut.GenerateCids(1)[0]
	expectedPieceCID2 := tut.GenerateCids(1)[0]

	expectedPiece := piecestore.PieceInfo{
		PieceCID: expectedPieceCID,
		Deals: []piecestore.DealInfo{
			{
				DealID: 0,
				Length: abi.PaddedPieceSize(paddedSize),
			},
		},
	}

	expectedPiece2 := piecestore.PieceInfo{
		PieceCID: expectedPieceCID2,
		Deals: []piecestore.DealInfo{
			{
				DealID: 1,
				Length: abi.PaddedPieceSize(paddedSize2),
			},
		},
	}

	expectedAddress := address.TestAddress2
	expectedMinerWallet := address.TestAddress
	expectedPricePerByte := abi.NewTokenAmount(4321)
	expectedPaymentInterval := uint64(4567)
	expectedPaymentIntervalIncrease := uint64(100)
	expectedUnsealPrice := abi.NewTokenAmount(100)

	// differential pricing
	expectedUnsealDiscount := abi.NewTokenAmount(1)

	receiveQueryOnProvider := func(
		t *testing.T,
		fn v1api.FullNode,
		sa *TestSectorAccessor,
		query *types.SignedQuery,
		config Config,
		pieceStore piecestore.PieceStore,
		dagStore *tut.MockDagStoreWrapper,
		signatureVerifier SignatureVerifier,
	) *types.QueryResponse {
		ds := dss.MutexWrap(datastore.NewMapDatastore())
		askStore, err := askstore.NewAskStore(namespace.Wrap(ds, datastore.NewKey("retrieval-ask")), datastore.NewKey("latest"))
		require.NoError(t, err)

		priceFunc := func(ctx context.Context, dealPricingParams retrievalmarket.PricingInput) (retrievalmarket.Ask, error) {
			ask := retrievalmarket.Ask{}
			ask.PricePerByte = expectedPricePerByte
			ask.PaymentInterval = expectedPaymentInterval
			ask.PaymentIntervalIncrease = expectedPaymentIntervalIncrease

			if dealPricingParams.Unsealed {
				ask.UnsealPrice = expectedUnsealDiscount
			} else {
				ask.UnsealPrice = expectedUnsealPrice
			}
			return ask, nil
		}

		c, err := NewProvider(config, expectedAddress, fn, sa, pieceStore, dagStore, askStore, signatureVerifier, priceFunc)
		require.NoError(t, err)

		return c.ExecuteQuery(query, expectedPeer)
	}

	testCases := []struct {
		name                            string
		httpRetrievalURL                string
		validSignature                  bool
		signatureError                  error
		query                           types.SignedQuery
		expResp                         types.QueryResponse
		expFunc                         func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper)
		sectorAccessorFunc              func(sa *TestSectorAccessor)
		expectedPricePerByte            abi.TokenAmount
		expectedPaymentInterval         uint64
		expectedPaymentIntervalIncrease uint64
		expectedUnsealPrice             abi.TokenAmount
	}{
		{name: "When PieceCID is not provided and PayloadCID is found, no http",
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID, expectedPiece)
				pieceStore.ExpectPiece(expectedPieceCID2, expectedPiece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			query: types.SignedQuery{Query: types.Query{PayloadCID: &payloadCID}},
			expResp: types.QueryResponse{
				Status: types.QueryResponseAvailable,
				Protocols: types.QueryProtocols{
					GraphsyncFilecoinV1: &types.GraphsyncFilecoinV1Response{
						Size: expectedSize,
					},
				},
			},
			expectedPricePerByte:            expectedPricePerByte,
			expectedPaymentInterval:         expectedPaymentInterval,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedUnsealPrice:             expectedUnsealPrice,
		},
		{name: "When PieceCID is not provided and PayloadCID is found, http",
			httpRetrievalURL: "https://www.yahoo.com",
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID, expectedPiece)
				pieceStore.ExpectPiece(expectedPieceCID2, expectedPiece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			query: types.SignedQuery{Query: types.Query{PayloadCID: &payloadCID}},
			expResp: types.QueryResponse{
				Status: types.QueryResponseAvailable,
				Protocols: types.QueryProtocols{
					GraphsyncFilecoinV1: &types.GraphsyncFilecoinV1Response{
						Size: expectedSize,
					},
					HTTPFilecoinV1: &types.HTTPFilecoinV1Response{
						URL:  fmt.Sprintf("https://www.yahoo.com/payload/%s.car", payloadCID),
						Size: expectedSize,
					},
				},
			},
			expectedPricePerByte:            expectedPricePerByte,
			expectedPaymentInterval:         expectedPaymentInterval,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedUnsealPrice:             expectedUnsealPrice,
		},

		{name: "When PieceCID is not provided, prefer a piece for which an unsealed sector already exists and price it accordingly, no http",
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := expectedPiece2.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID, expectedPiece)
				pieceStore.ExpectPiece(expectedPieceCID2, expectedPiece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			query: types.SignedQuery{Query: types.Query{PayloadCID: &payloadCID}},
			expResp: types.QueryResponse{
				Status: types.QueryResponseAvailable,
				Protocols: types.QueryProtocols{
					GraphsyncFilecoinV1: &types.GraphsyncFilecoinV1Response{
						Size: expectedSize2,
					},
				},
			},
			expectedPricePerByte:            expectedPricePerByte,
			expectedPaymentInterval:         expectedPaymentInterval,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedUnsealPrice:             expectedUnsealDiscount,
		},

		{name: "When PieceCID is not provided, prefer a piece for which an unsealed sector already exists and price it accordingly, http",
			httpRetrievalURL: "https://www.yahoo.com",
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := expectedPiece2.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID, expectedPiece)
				pieceStore.ExpectPiece(expectedPieceCID2, expectedPiece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			query: types.SignedQuery{Query: types.Query{PayloadCID: &payloadCID}},
			expResp: types.QueryResponse{
				Status: types.QueryResponseAvailable,
				Protocols: types.QueryProtocols{
					GraphsyncFilecoinV1: &types.GraphsyncFilecoinV1Response{
						Size: expectedSize2,
					},
					HTTPFilecoinV1: &types.HTTPFilecoinV1Response{
						URL:  fmt.Sprintf("https://www.yahoo.com/payload/%s.car", payloadCID),
						Size: expectedSize2,
					},
				},
			},
			expectedPricePerByte:            expectedPricePerByte,
			expectedPaymentInterval:         expectedPaymentInterval,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedUnsealPrice:             expectedUnsealDiscount,
		},

		{name: "When PieceCID is provided and both PieceCID and PayloadCID are found, no http",
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				loadPieceCIDS(t, pieceStore, expectedPieceCID)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID)
			},
			query: types.SignedQuery{
				Query: types.Query{
					PayloadCID: &payloadCID,
					PieceCID:   &expectedPieceCID,
				},
			},
			expResp: types.QueryResponse{
				Status: types.QueryResponseAvailable,
				Protocols: types.QueryProtocols{
					GraphsyncFilecoinV1: &types.GraphsyncFilecoinV1Response{
						Size: expectedSize,
					},
				},
			},
			expectedPricePerByte:            expectedPricePerByte,
			expectedPaymentInterval:         expectedPaymentInterval,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedUnsealPrice:             expectedUnsealPrice,
		},
		{name: "When PieceCID is provided and both PieceCID and PayloadCID are found, http",
			httpRetrievalURL: "https://www.yahoo.com",
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				loadPieceCIDS(t, pieceStore, expectedPieceCID)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID)
			},
			query: types.SignedQuery{
				Query: types.Query{
					PayloadCID: &payloadCID,
					PieceCID:   &expectedPieceCID,
				},
			},
			expResp: types.QueryResponse{
				Status: types.QueryResponseAvailable,
				Protocols: types.QueryProtocols{
					GraphsyncFilecoinV1: &types.GraphsyncFilecoinV1Response{
						Size: expectedSize,
					},
					HTTPFilecoinV1: &types.HTTPFilecoinV1Response{
						URL:  fmt.Sprintf("https://www.yahoo.com/payload/%s.car", payloadCID),
						Size: expectedSize,
					},
				},
			},
			expectedPricePerByte:            expectedPricePerByte,
			expectedPaymentInterval:         expectedPaymentInterval,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedUnsealPrice:             expectedUnsealPrice,
		},
		{name: "When PieceCID is provided and piece is missing",
			expFunc: func(t *testing.T, ps *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				loadPieceCIDS(t, ps, cid.Undef)
				ps.ExpectMissingPiece(expectedPieceCID)
				ps.ExpectMissingPiece(expectedPieceCID2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			query: types.SignedQuery{
				Query: types.Query{
					PayloadCID: &payloadCID,
					PieceCID:   &expectedPieceCID,
				},
			},
			expResp: types.QueryResponse{
				Status: types.QueryResponseUnavailable,
				Error:  "piece info for cid not found (deal has not been added to a piece yet)",
			},
			expectedPricePerByte:            big.Zero(),
			expectedPaymentInterval:         0,
			expectedPaymentIntervalIncrease: 0,
			expectedUnsealPrice:             big.Zero(),
		},
		{name: "When ONLY PieceCID is provided, no http",
			query: types.SignedQuery{
				Query: types.Query{
					PieceCID: &expectedPieceCID,
				},
			},
			expResp: types.QueryResponse{
				Status: types.QueryResponseError,
				Error:  "piece only retrieval not supported",
			},
		},
		{name: "When ONLY PieceCID is provided, unsealed, http",
			httpRetrievalURL: "https://www.yahoo.com",
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID, expectedPiece)
			},
			sectorAccessorFunc: func(sa *TestSectorAccessor) {
				p := expectedPiece.Deals[0]
				sa.MarkUnsealed(context.TODO(), p.SectorID, p.Offset.Unpadded(), p.Length.Unpadded())
			},
			query: types.SignedQuery{
				Query: types.Query{
					PieceCID: &expectedPieceCID,
				},
			},
			expResp: types.QueryResponse{
				Status: types.QueryResponseAvailable,
				Protocols: types.QueryProtocols{
					HTTPFilecoinV1: &types.HTTPFilecoinV1Response{
						URL:  fmt.Sprintf("https://www.yahoo.com/piece/%s", expectedPieceCID),
						Size: expectedSize,
					},
				},
			},
		},
		{name: "When ONLY PieceCID is provided, sealed, http",
			httpRetrievalURL: "https://www.yahoo.com",
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID, expectedPiece)
			},
			query: types.SignedQuery{
				Query: types.Query{
					PieceCID: &expectedPieceCID,
				},
			},
			expResp: types.QueryResponse{
				Status: types.QueryResponseError,
				Error:  "piece not avialable in unsealed sector",
			},
		},
		{name: "When payload CID not found",
			query: types.SignedQuery{
				Query: types.Query{
					PayloadCID: &payloadCID,
					PieceCID:   &expectedPieceCID,
				},
			},
			expResp: types.QueryResponse{
				Status: types.QueryResponseUnavailable,
				Error:  "piece info for cid not found (deal has not been added to a piece yet)",
			},
		},
		{name: "error reading piece",
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ReturnErrorFromGetPieceInfo(errors.New("something went wrong"))
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID)
			},
			query: types.SignedQuery{Query: types.Query{PayloadCID: &payloadCID}},
			expResp: types.QueryResponse{
				Status: types.QueryResponseError,
				Error:  "failed to fetch piece to retrieve from: could not locate piece: something went wrong",
			},
		},
		{name: "error reading piece, pieceCID only specified",
			httpRetrievalURL: "https://www.yahoo.com",

			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ReturnErrorFromGetPieceInfo(errors.New("something went wrong"))
			},
			query: types.SignedQuery{Query: types.Query{PieceCID: &expectedPieceCID}},
			expResp: types.QueryResponse{
				Status: types.QueryResponseError,
				Error:  "failed to fetch piece to retrieve from: something went wrong",
			},
		},
		{
			name: "address provided with valid signature",
			expFunc: func(t *testing.T, pieceStore *tut.TestPieceStore, dagStore *tut.MockDagStoreWrapper) {
				pieceStore.ExpectPiece(expectedPieceCID, expectedPiece)
				pieceStore.ExpectPiece(expectedPieceCID2, expectedPiece2)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID)
				dagStore.AddBlockToPieceIndex(payloadCID, expectedPieceCID2)
			},
			validSignature: true,
			query: types.SignedQuery{
				Query: types.Query{
					PayloadCID: &payloadCID,
				},
				ClientAddress: &address.TestAddress,
				ClientSignature: &acrypto.Signature{
					Type: acrypto.SigTypeSecp256k1,
					Data: []byte{1, 2, 3, 4},
				},
			},
			expResp: types.QueryResponse{
				Status: types.QueryResponseAvailable,
				Protocols: types.QueryProtocols{
					GraphsyncFilecoinV1: &types.GraphsyncFilecoinV1Response{
						Size: expectedSize,
					},
				},
			},
			expectedPricePerByte:            expectedPricePerByte,
			expectedPaymentInterval:         expectedPaymentInterval,
			expectedPaymentIntervalIncrease: expectedPaymentIntervalIncrease,
			expectedUnsealPrice:             expectedUnsealPrice,
		},
		{
			name: "address provided, invalid signature",
			query: types.SignedQuery{
				Query: types.Query{
					PayloadCID: &payloadCID,
				},
				ClientAddress: &address.TestAddress,
				ClientSignature: &acrypto.Signature{
					Type: acrypto.SigTypeSecp256k1,
					Data: []byte{1, 2, 3, 4},
				},
			},
			expResp: types.QueryResponse{
				Status: types.QueryResponseError,
				Error:  "signature invalid",
			},
		},
		{
			name:           "address provided, signature validation error",
			signatureError: errors.New("something went wrong"),
			query: types.SignedQuery{
				Query: types.Query{
					PayloadCID: &payloadCID,
				},
				ClientAddress: &address.TestAddress,
				ClientSignature: &acrypto.Signature{
					Type: acrypto.SigTypeSecp256k1,
					Data: []byte{1, 2, 3, 4},
				},
			},
			expResp: types.QueryResponse{
				Status: types.QueryResponseError,
				Error:  "error while attempting to verify signature: something went wrong",
			},
		},
		{name: "When neither PieceCID nor PayloadCID is provided",
			query: types.SignedQuery{Query: types.Query{}},
			expResp: types.QueryResponse{
				Status: types.QueryResponseError,
				Error:  "either piece CID or payload CID must be present",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			fn := lotusmocks.NewMockFullNode(ctrl)
			fn.EXPECT().StateMinerInfo(gomock.Any(), expectedAddress, ctypes.EmptyTSK).AnyTimes().Return(api.MinerInfo{Worker: expectedMinerWallet}, nil)
			fn.EXPECT().StateMarketStorageDeal(gomock.Any(), abi.DealID(0), ctypes.EmptyTSK).AnyTimes().Return(&api.MarketDeal{
				Proposal: market.DealProposal{
					PieceSize: abi.PaddedPieceSize(paddedSize),
					PieceCID:  expectedPieceCID,
				},
			}, nil)
			fn.EXPECT().StateMarketStorageDeal(gomock.Any(), abi.DealID(1), ctypes.EmptyTSK).AnyTimes().Return(&api.MarketDeal{
				Proposal: market.DealProposal{
					PieceSize: abi.PaddedPieceSize(paddedSize2),
					PieceCID:  expectedPieceCID2,
				},
			}, nil)
			sa := NewTestSectorAccessor()
			pieceStore := tut.NewTestPieceStore()
			dagStore := tut.NewMockDagStoreWrapper(pieceStore, sa)
			if tc.sectorAccessorFunc != nil {
				tc.sectorAccessorFunc(sa)
			}

			if tc.expFunc != nil {
				tc.expFunc(t, pieceStore, dagStore)
			}

			signatureVerifier := &mockSignatureVerifier{tc.validSignature, tc.signatureError}
			actualResp := receiveQueryOnProvider(t, fn, sa, &tc.query, Config{HTTPRetrievalURL: tc.httpRetrievalURL}, pieceStore, dagStore, signatureVerifier)

			pieceStore.VerifyExpectations(t)
			ctrl.Finish()

			if tc.expResp.Protocols.GraphsyncFilecoinV1 != nil {
				graphsyncResponse := tc.expResp.Protocols.GraphsyncFilecoinV1
				graphsyncResponse.PaymentAddress = expectedMinerWallet
				graphsyncResponse.MinPricePerByte = tc.expectedPricePerByte
				graphsyncResponse.MaxPaymentInterval = tc.expectedPaymentInterval
				graphsyncResponse.MaxPaymentIntervalIncrease = tc.expectedPaymentIntervalIncrease
				graphsyncResponse.UnsealPrice = tc.expectedUnsealPrice
			}
			assert.Equal(t, &tc.expResp, actualResp)
		})
	}

}

type sectorKey struct {
	sectorID abi.SectorNumber
	offset   abi.UnpaddedPieceSize
	length   abi.UnpaddedPieceSize
}

// TestSectorAccessor is a mock implementation of the SectorAccessor
type TestSectorAccessor struct {
	unsealed map[sectorKey]struct{}
}

var _ retrievalmarket.SectorAccessor = &TestSectorAccessor{}

// NewTestSectorAccessor instantiates a new TestSectorAccessor
func NewTestSectorAccessor() *TestSectorAccessor {
	return &TestSectorAccessor{
		unsealed: make(map[sectorKey]struct{}),
	}
}

func (trpn *TestSectorAccessor) IsUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (bool, error) {
	_, ok := trpn.unsealed[sectorKey{sectorID, offset, length}]
	return ok, nil
}

func (trpn *TestSectorAccessor) MarkUnsealed(ctx context.Context, sectorID abi.SectorNumber, offset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) {
	trpn.unsealed[sectorKey{sectorID, offset, length}] = struct{}{}
}

// UnsealSector simulates unsealing a sector by returning a stubbed response
// or erroring
func (trpn *TestSectorAccessor) UnsealSector(ctx context.Context, sectorID abi.SectorNumber, offset, length abi.UnpaddedPieceSize) (io.ReadCloser, error) {
	return nil, errors.New("could not unseal")
}

func (trpn *TestSectorAccessor) UnsealSectorAt(ctx context.Context, sectorID abi.SectorNumber, pieceOffset abi.UnpaddedPieceSize, length abi.UnpaddedPieceSize) (mount.Reader, error) {
	return nil, errors.New("could not unseal")
}

// loadPieceCIDS sets expectations to receive expectedPieceCID and 3 other random PieceCIDs to
// disinguish the case of a PayloadCID is found but the PieceCID is not
func loadPieceCIDS(t *testing.T, pieceStore *tut.TestPieceStore, expectedPieceCID cid.Cid) {

	otherPieceCIDs := tut.GenerateCids(3)
	expectedSize := uint64(1234)

	blockLocs := make([]piecestore.PieceBlockLocation, 4)
	expectedPieceInfo := piecestore.PieceInfo{
		PieceCID: expectedPieceCID,
		Deals: []piecestore.DealInfo{
			{
				Length: abi.PaddedPieceSize(expectedSize),
			},
		},
	}

	blockLocs[0] = piecestore.PieceBlockLocation{PieceCID: expectedPieceCID}
	for i, pieceCID := range otherPieceCIDs {
		blockLocs[i+1] = piecestore.PieceBlockLocation{PieceCID: pieceCID}
		pi := expectedPieceInfo
		pi.PieceCID = pieceCID
	}
	if expectedPieceCID != cid.Undef {
		pieceStore.ExpectPiece(expectedPieceCID, expectedPieceInfo)
	}
}

type mockSignatureVerifier struct {
	valid bool
	err   error
}

func (m *mockSignatureVerifier) VerifySignature(ctx context.Context, sig acrypto.Signature, addr address.Address, input []byte, encodedTs shared.TipSetToken) (bool, error) {
	return m.valid, m.err
}
