package retrievalmarket

import (
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/boost/retrievalmarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/stores"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/lotus/api/v1api"
	ctypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/markets/dagstore"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

var log = logging.Logger("boost-provider")

// Config is config vars that are used by the provider
type Config struct {
	// HTTP Retrievals URL -- if blank HTTP retrieval is considered "off"
	HTTPRetrievalURL string
}

// RetrievalPricingFunc is a custom function that sets retrieval pricing
type RetrievalPricingFunc func(ctx context.Context, dealPricingParams retrievalmarket.PricingInput) (retrievalmarket.Ask, error)

// Provider is the boost implementation of the retrieval provider, which currently
// only implements the QueryAsk v2 protocol
type Provider struct {
	ctx                  context.Context
	cancel               context.CancelFunc
	Address              address.Address
	config               Config
	fullnodeAPI          v1api.FullNode
	dagStore             stores.DAGStoreWrapper
	pieceStore           piecestore.PieceStore
	sa                   dagstore.SectorAccessor
	askStore             retrievalmarket.AskStore
	retrievalPricingFunc RetrievalPricingFunc
}

// NewProvider returns a new retrieval Provider
func NewProvider(config Config,
	address address.Address,
	fullnodeAPI v1api.FullNode,
	sa dagstore.SectorAccessor,
	pieceStore piecestore.PieceStore,
	dagStore stores.DAGStoreWrapper,
	askStore retrievalmarket.AskStore,
	retrievalPricingFunc RetrievalPricingFunc,
) (*Provider, error) {

	if retrievalPricingFunc == nil {
		return nil, errors.New("retrievalPricingFunc is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Provider{
		config:               config,
		Address:              address,
		sa:                   sa,
		pieceStore:           pieceStore,
		retrievalPricingFunc: retrievalPricingFunc,
		dagStore:             dagStore,
		askStore:             askStore,
		fullnodeAPI:          fullnodeAPI,
		ctx:                  ctx,
		cancel:               cancel,
	}, nil
}

// stop shuts down the retreival provider
func (p *Provider) Stop() {
	p.cancel()
}

// ExecuteQuery generates a query response for the queryAsk v2 protocol
func (p *Provider) ExecuteQuery(q *types.Query, remote peer.ID) *types.QueryResponse {

	answer := types.QueryResponse{
		Status: types.QueryResponseUnavailable,
	}

	if q.PayloadCID != nil {

		// payload present, process as payload query

		// fetch the piece from which the payload will be retrieved.

		// if user has specified the Piece in the request, we use that.
		// Otherwise, we prefer a Piece which can retrieved from an unsealed sector.
		pieceCID := cid.Undef
		if q.PieceCID != nil {
			pieceCID = *q.PieceCID
		}
		pieceInfo, isUnsealed, err := p.getPieceInfoFromCid(p.ctx, *q.PayloadCID, pieceCID)
		// did we find a piece with this payload CID?
		if err != nil {
			log.Errorf("Retrieval query: getPieceInfoFromCid: %s", err)
			if !errors.Is(err, retrievalmarket.ErrNotFound) {
				answer.Status = types.QueryResponseError
				answer.Error = fmt.Sprintf("failed to fetch piece to retrieve from: %s", err)
			} else {
				answer.Error = "piece info for cid not found (deal has not been added to a piece yet)"
			}
			return &answer
		}

		// graphsync is always available for payloads, so assemble response -- we'll need
		graphsyncFilecoinV1Response, err := p.graphsyncQueryResponse(q, remote, pieceInfo, isUnsealed)
		if err != nil {
			answer.Status = types.QueryResponseError
			answer.Error = err.Error()
			return &answer
		}

		answer.Status = types.QueryResponseAvailable
		answer.Protocols.GraphsyncFilecoinV1 = &graphsyncFilecoinV1Response

		// add http payload url if we are supporting HTTP retrievals
		if p.config.HTTPRetrievalURL != "" {
			answer.Protocols.HTTPFilecoinV1 = &types.HTTPFilecoinV1Response{
				URL:  p.config.HTTPRetrievalURL + "/payload/" + q.PayloadCID.String() + ".car",
				Size: uint64(pieceInfo.Deals[0].Length.Unpadded()),
			}
		}

		return &answer
	}

	if q.PieceCID == nil {
		// payload cid & piece cid are both nil, error
		answer.Status = types.QueryResponseError
		answer.Error = "either piece CID or payload CID must be present"
		return &answer
	}

	// piece cid only is present, process as piece retrieval, meaning only HTTP is available

	// if http retrieval is not available, there is no piece retrieval supported
	if p.config.HTTPRetrievalURL == "" {
		answer.Status = types.QueryResponseError
		answer.Error = "piece only retrieval not supported"
		return &answer
	}

	// lookup information on this piece, determine if there is an unsealed sector
	pieceInfo, err := p.pieceStore.GetPieceInfo(*q.PieceCID)
	if err != nil {
		if !errors.Is(err, retrievalmarket.ErrNotFound) {
			answer.Status = types.QueryResponseError
			answer.Error = fmt.Sprintf("failed to fetch piece to retrieve from: %s", err)
		} else {
			answer.Error = "piece info for cid not found (deal has not been added to a piece yet)"
		}
		return &answer
	}

	// we have the piece, but do we have it unsealed?
	if !p.pieceInUnsealedSector(p.ctx, pieceInfo) {
		answer.Status = types.QueryResponseError
		answer.Error = fmt.Sprintf("piece not avialable in unsealed sector")
		return &answer
	}

	// ok we have an unsealed sector with the piece and HTTP retrieval is on, we're good to go
	answer.Status = types.QueryResponseAvailable
	answer.Protocols.HTTPFilecoinV1 = &types.HTTPFilecoinV1Response{
		URL:  p.config.HTTPRetrievalURL + "/piece/" + q.PieceCID.String(),
		Size: uint64(pieceInfo.Deals[0].Length.Unpadded()),
	}
	return &answer

}

// graphsyncQueryResponse returns graphsync response params for a payload retireval
func (p *Provider) graphsyncQueryResponse(q *types.Query, remote peer.ID, pieceInfo piecestore.PieceInfo, isUnsealed bool) (types.GraphsyncFilecoinV1Response, error) {
	graphsyncFilecoinV1Response := &types.GraphsyncFilecoinV1Response{
		MinPricePerByte: big.Zero(),
		UnsealPrice:     big.Zero(),
	}
	// fetch the payment address the client should send the payment to.
	minerInfo, err := p.fullnodeAPI.StateMinerInfo(p.ctx, p.Address, ctypes.EmptyTSK)
	if err != nil {
		log.Errorf("Retrieval query: Lookup Payment Address: %s", err)
		return types.GraphsyncFilecoinV1Response{}, fmt.Errorf("failed to look up payment address: %w", err)
	}
	graphsyncFilecoinV1Response.PaymentAddress = minerInfo.Worker

	// set the size
	graphsyncFilecoinV1Response.Size = uint64(pieceInfo.Deals[0].Length.Unpadded()) // TODO: verify on intermediate

	// look up associated storage deals
	storageDeals, err := p.storageDealsForPiece(q.PieceCID != nil, *q.PayloadCID, pieceInfo)
	if err != nil {
		log.Errorf("Retrieval query: storageDealsForPiece: %s", err)
		return types.GraphsyncFilecoinV1Response{}, fmt.Errorf("failed to fetch storage deals containing payload: %w", err)
	}

	// get pricing based on dynamic parameters & associated storage deals
	input := retrievalmarket.PricingInput{
		// piece from which the payload will be retrieved
		// If user hasn't given a PieceCID, we try to choose an unsealed piece in the call to `getPieceInfoFromCid` above.
		PieceCID: pieceInfo.PieceCID,

		PayloadCID: *q.PayloadCID,
		Unsealed:   isUnsealed,
		Client:     remote,
	}
	ask, err := p.GetDynamicAsk(p.ctx, input, storageDeals)
	if err != nil {
		log.Errorf("Retrieval query: GetAsk: %s", err)
		return types.GraphsyncFilecoinV1Response{}, fmt.Errorf("failed to price deal: %w", err)
	}

	// set pricing params
	graphsyncFilecoinV1Response.MinPricePerByte = ask.PricePerByte
	graphsyncFilecoinV1Response.MaxPaymentInterval = ask.PaymentInterval
	graphsyncFilecoinV1Response.MaxPaymentIntervalIncrease = ask.PaymentIntervalIncrease
	graphsyncFilecoinV1Response.UnsealPrice = ask.UnsealPrice

	return *graphsyncFilecoinV1Response, nil
}

// Given the CID of a block, find a piece that contains that block.
// If the client has specified which piece they want, return that piece.
// Otherwise prefer pieces that are already unsealed.
func (p *Provider) getPieceInfoFromCid(ctx context.Context, payloadCID, clientPieceCID cid.Cid) (piecestore.PieceInfo, bool, error) {
	// Get all pieces that contain the target block
	piecesWithTargetBlock, err := p.dagStore.GetPiecesContainingBlock(payloadCID)
	if err != nil {
		return piecestore.PieceInfoUndefined, false, fmt.Errorf("getting pieces for cid %s: %w", payloadCID, err)
	}

	// For each piece that contains the target block
	var lastErr error
	var sealedPieceInfo *piecestore.PieceInfo
	for _, pieceWithTargetBlock := range piecesWithTargetBlock {
		// Get the deals for the piece
		pieceInfo, err := p.pieceStore.GetPieceInfo(pieceWithTargetBlock)
		if err != nil {
			lastErr = err
			continue
		}

		// if client wants to retrieve the payload from a specific piece, just return that piece.
		if clientPieceCID.Defined() && pieceInfo.PieceCID.Equals(clientPieceCID) {
			return pieceInfo, p.pieceInUnsealedSector(ctx, pieceInfo), nil
		}

		// if client doesn't have a preference for a particular piece, prefer a piece
		// for which an unsealed sector exists.
		if clientPieceCID.Equals(cid.Undef) {
			if p.pieceInUnsealedSector(ctx, pieceInfo) {
				// The piece is in an unsealed sector, so just return it
				return pieceInfo, true, nil
			}

			if sealedPieceInfo == nil {
				// The piece is not in an unsealed sector, so save it but keep
				// checking other pieces to see if there is one that is in an
				// unsealed sector
				sealedPieceInfo = &pieceInfo
			}
		}

	}

	// Found a piece containing the target block, piece is in a sealed sector
	if sealedPieceInfo != nil {
		return *sealedPieceInfo, false, nil
	}

	// Couldn't find a piece containing the target block
	if lastErr == nil {
		lastErr = fmt.Errorf("unknown pieceCID %s", clientPieceCID.String())
	}

	// Error finding a piece containing the target block
	return piecestore.PieceInfoUndefined, false, fmt.Errorf("could not locate piece: %w", lastErr)
}

func (p *Provider) pieceInUnsealedSector(ctx context.Context, pieceInfo piecestore.PieceInfo) bool {
	for _, di := range pieceInfo.Deals {
		isUnsealed, err := p.sa.IsUnsealed(ctx, di.SectorID, di.Offset.Unpadded(), di.Length.Unpadded())
		if err != nil {
			log.Errorf("failed to find out if sector %d is unsealed, err=%s", di.SectorID, err)
			continue
		}
		if isUnsealed {
			return true
		}
	}

	return false
}

func (p *Provider) storageDealsForPiece(clientSpecificPiece bool, payloadCID cid.Cid, pieceInfo piecestore.PieceInfo) ([]abi.DealID, error) {
	var storageDeals []abi.DealID
	var err error
	if clientSpecificPiece {
		// If the user wants to retrieve the payload from a specific piece,
		// we only need to inspect storage deals made for that piece to quote a price.
		for _, d := range pieceInfo.Deals {
			storageDeals = append(storageDeals, d.DealID)
		}
	} else {
		// If the user does NOT want to retrieve from a specific piece, we'll have to inspect all storage deals
		// made for that piece to quote a price.
		storageDeals, err = p.getAllDealsContainingPayload(payloadCID)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch deals for payload: %w", err)
		}
	}

	if len(storageDeals) == 0 {
		return nil, errors.New("no storage deals found")
	}

	return storageDeals, nil
}

func (p *Provider) getAllDealsContainingPayload(payloadCID cid.Cid) ([]abi.DealID, error) {
	// Get all pieces that contain the target block
	piecesWithTargetBlock, err := p.dagStore.GetPiecesContainingBlock(payloadCID)
	if err != nil {
		return nil, fmt.Errorf("getting pieces for cid %s: %w", payloadCID, err)
	}

	// For each piece that contains the target block
	var lastErr error
	var dealsIds []abi.DealID
	for _, pieceWithTargetBlock := range piecesWithTargetBlock {
		// Get the deals for the piece
		pieceInfo, err := p.pieceStore.GetPieceInfo(pieceWithTargetBlock)
		if err != nil {
			lastErr = err
			continue
		}

		for _, d := range pieceInfo.Deals {
			dealsIds = append(dealsIds, d.DealID)
		}
	}

	if lastErr == nil && len(dealsIds) == 0 {
		return nil, errors.New("no deals found")
	}

	if lastErr != nil && len(dealsIds) == 0 {
		return nil, fmt.Errorf("failed to fetch deals containing payload %s: %w", payloadCID, lastErr)
	}

	return dealsIds, nil
}

func (p *Provider) getRetrievalPricingInput(ctx context.Context, pieceCID cid.Cid, storageDeals []abi.DealID) (retrievalmarket.PricingInput, error) {
	resp := retrievalmarket.PricingInput{}

	var mErr error

	for _, dealID := range storageDeals {
		ds, err := p.fullnodeAPI.StateMarketStorageDeal(ctx, dealID, ctypes.EmptyTSK)
		if err != nil {
			log.Warnf("failed to look up deal %d on chain: err=%w", dealID, err)
			mErr = multierror.Append(mErr, err)
			continue
		}
		if ds.Proposal.VerifiedDeal {
			resp.VerifiedDeal = true
		}

		if ds.Proposal.PieceCID.Equals(pieceCID) {
			resp.PieceSize = ds.Proposal.PieceSize.Unpadded()
		}

		// If we've discovered a verified deal with the required PieceCID, we don't need
		// to lookup more deals and we're done.
		if resp.VerifiedDeal && resp.PieceSize != 0 {
			break
		}
	}

	// Note: The piece size can never actually be zero. We only use it to here
	// to assert that we didn't find a matching piece.
	if resp.PieceSize == 0 {
		if mErr == nil {
			return resp, errors.New("failed to find matching piece")
		}

		return resp, fmt.Errorf("failed to fetch storage deal state: %w", mErr)
	}

	return resp, nil
}

// GetDynamicAsk quotes a dynamic price for the retrieval deal by calling the user configured
// dynamic pricing function. It passes the static price parameters set in the Ask Store to the pricing function.
func (p *Provider) GetDynamicAsk(ctx context.Context, input retrievalmarket.PricingInput, storageDeals []abi.DealID) (retrievalmarket.Ask, error) {
	dp, err := p.getRetrievalPricingInput(ctx, input.PieceCID, storageDeals)
	if err != nil {
		return retrievalmarket.Ask{}, fmt.Errorf("GetRetrievalPricingInput: %s", err)
	}
	// currAsk cannot be nil as we initialize the ask store with a default ask.
	// Users can then change the values in the ask store using SetAsk but not remove it.
	currAsk := p.GetAsk()
	if currAsk == nil {
		return retrievalmarket.Ask{}, errors.New("no ask configured in ask-store")
	}

	dp.PayloadCID = input.PayloadCID
	dp.PieceCID = input.PieceCID
	dp.Unsealed = input.Unsealed
	dp.Client = input.Client
	dp.CurrentAsk = *currAsk

	ask, err := p.retrievalPricingFunc(ctx, dp)
	if err != nil {
		return retrievalmarket.Ask{}, fmt.Errorf("retrievalPricingFunc: %w", err)
	}
	return ask, nil
}

// GetAsk returns the current deal parameters this provider accepts
func (p *Provider) GetAsk() *retrievalmarket.Ask {
	return p.askStore.GetAsk()
}
