package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/boost/safe"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
)

// The time limit to read a message from the client when the client opens a stream
const providerReadDeadline = 10 * time.Second

// The time limit to write a response to the client
const providerWriteDeadline = 10 * time.Second

// The time limit to process a query (not including reading / writing to stream)
const processQueryTimeout = 30 * time.Second

type QueryAskHandler struct {
	minerAddress address.Address
	pd           *piecedirectory.PieceDirectory
	sa           SectorAccessor
	askStore     RetrievalAskGetter
	full         v1api.FullNode
	host         host.Host
}

func NewQueryAskHandler(host host.Host, maddr address.Address, pd *piecedirectory.PieceDirectory, sa SectorAccessor, askStore RetrievalAskGetter, full v1api.FullNode) *QueryAskHandler {
	return &QueryAskHandler{
		host:         host,
		minerAddress: maddr,
		pd:           pd,
		sa:           sa,
		askStore:     askStore,
		full:         full,
	}
}

func (qa *QueryAskHandler) Start() {
	qa.host.SetStreamHandler(legacyretrievaltypes.QueryProtocolID, safe.Handle(qa.HandleQueryStream))
}

func (qa *QueryAskHandler) Stop() {
	qa.host.RemoveStreamHandler(legacyretrievaltypes.QueryProtocolID)
}

func (qa *QueryAskHandler) HandleQueryStream(stream network.Stream) {
	defer func() {
		_ = stream.Close()
	}()

	// Set a deadline on reading from the stream so it doesn't hang
	_ = stream.SetReadDeadline(time.Now().Add(providerReadDeadline))

	var query legacyretrievaltypes.Query
	err := query.UnmarshalCBOR(stream)
	_ = stream.SetReadDeadline(time.Time{}) // Clear read deadline so conn doesn't get closed
	if err != nil {
		log.Infow("Retrieval query: reading query", "peer", stream.Conn().RemotePeer(), "error", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.TODO(), processQueryTimeout)
	defer cancel()
	answer, err := qa.getQueryResponse(ctx, query)
	if err != nil {
		status := legacyretrievaltypes.QueryResponseError
		if errors.Is(err, legacyretrievaltypes.ErrNotFound) {
			status = legacyretrievaltypes.QueryResponseUnavailable
		}
		answer = &legacyretrievaltypes.QueryResponse{
			Status:          status,
			PieceCIDFound:   legacyretrievaltypes.QueryItemUnavailable,
			PaymentAddress:  qa.minerAddress,
			MinPricePerByte: big.Zero(),
			UnsealPrice:     big.Zero(),
			Message:         err.Error(),
		}
	}

	// Set a deadline on writing to the stream so it doesn't hang
	_ = stream.SetWriteDeadline(time.Now().Add(providerWriteDeadline))
	defer func() {
		_ = stream.SetWriteDeadline(time.Time{})
	}()

	if err := cborutil.WriteCborRPC(stream, answer); err != nil {
		log.Infow("Retrieval query: writing query response", "peer", stream.Conn().RemotePeer(), "error", err)
	}
}

func (qa *QueryAskHandler) getQueryResponse(ctx context.Context, query legacyretrievaltypes.Query) (*legacyretrievaltypes.QueryResponse, error) {
	// Fetch the payment address the client should send the payment to
	head, err := qa.full.ChainHead(ctx)
	if err != nil {
		return nil, fmt.Errorf("get chain head: %w", err)
	}

	minerInfo, err := qa.full.StateMinerInfo(ctx, qa.minerAddress, head.Key())
	if err != nil {
		return nil, fmt.Errorf("get miner info: %w", err)
	}

	// Fetch the piece from which the payload will be retrieved.
	// If user has specified the Piece in the request, we use that.
	// Otherwise, we prefer a Piece which can retrieved from an unsealed sector.
	pieceCID := cid.Undef
	if query.PieceCID != nil {
		pieceCID = *query.PieceCID
	}

	pieces, piecesErr := GetAllPieceInfoForPayload(ctx, qa.pd, query.PayloadCID)
	// Error may be non-nil, but we may have successfuly found >0 pieces, so defer error handling till
	// we have no other option.

	pieceInfo, _ := GetBestPieceInfoMatch(ctx, qa.sa, pieces, pieceCID)
	if !pieceInfo.PieceCID.Defined() {
		if piecesErr != nil && !errors.Is(piecesErr, legacyretrievaltypes.ErrNotFound) {
			return nil, fmt.Errorf("fetching piece to retrieve from: %w", piecesErr)
		}
		return nil, fmt.Errorf("getting pieces for payload cid %s: %w", query.PayloadCID, legacyretrievaltypes.ErrNotFound)
	}

	if len(pieceInfo.Deals) == 0 {
		return nil, fmt.Errorf("fetching storage deals for piece %s: piece has 0 deals", pieceInfo.PieceCID)
	}

	storageDeals := GetStorageDealsForPiece(query.PieceCID != nil, pieces, pieceInfo)
	if len(storageDeals) == 0 {
		if piecesErr == nil {
			piecesErr = fmt.Errorf("no deals found")
		}
		return nil, fmt.Errorf("fetching storage deals containing payload cid %s: %w", query.PayloadCID, piecesErr)
	}

	currAsk := qa.askStore.GetAsk()
	if currAsk == nil {
		return nil, errors.New("no ask configured in ask-store")
	}

	return &legacyretrievaltypes.QueryResponse{
		PaymentAddress:             minerInfo.Worker,
		Status:                     legacyretrievaltypes.QueryResponseAvailable,
		Size:                       uint64(pieceInfo.Deals[0].PieceLength.Unpadded()),
		PieceCIDFound:              legacyretrievaltypes.QueryItemAvailable,
		MinPricePerByte:            currAsk.PricePerByte,
		MaxPaymentInterval:         currAsk.PaymentInterval,
		MaxPaymentIntervalIncrease: currAsk.PaymentIntervalIncrease,
		UnsealPrice:                currAsk.UnsealPrice,
	}, nil
}
