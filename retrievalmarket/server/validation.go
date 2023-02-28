package server

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket/impl/requestvalidation"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// An implementation of ValidationEnvironment for unpaid retrieval
type validationEnv struct {
	ctx context.Context
	ValidationDeps
}

var _ requestvalidation.ValidationEnvironment = (*validationEnv)(nil)

func (v *validationEnv) GetAsk(ctx context.Context, payloadCid cid.Cid, pieceCid *cid.Cid, piece piecestore.PieceInfo, isUnsealed bool, client peer.ID) (retrievalmarket.Ask, error) {
	return retrievalmarket.Ask{
		PricePerByte:            abi.NewTokenAmount(0),
		UnsealPrice:             abi.NewTokenAmount(0),
		PaymentInterval:         0,
		PaymentIntervalIncrease: 0,
	}, nil
}

func (v *validationEnv) CheckDealParams(ask retrievalmarket.Ask, pricePerByte abi.TokenAmount, paymentInterval uint64, paymentIntervalIncrease uint64, unsealPrice abi.TokenAmount) error {
	return nil
}

func (v *validationEnv) RunDealDecisioningLogic(ctx context.Context, state retrievalmarket.ProviderDealState) (bool, string, error) {
	if v.DealDecider == nil {
		return true, "", nil
	}
	return v.DealDecider(ctx, state)
}

func (v *validationEnv) BeginTracking(pds retrievalmarket.ProviderDealState) error {
	return nil
}

func (v *validationEnv) GetPiece(payloadCid cid.Cid, pieceCID *cid.Cid) (piecestore.PieceInfo, bool, error) {
	inPieceCid := cid.Undef
	if pieceCID != nil {
		inPieceCid = *pieceCID
	}

	pieces, piecesErr := GetAllPieceInfoForPayload(v.DagStore, v.PieceStore, payloadCid)
	pieceInfo, isUnsealed := GetBestPieceInfoMatch(v.ctx, v.SectorAccessor, pieces, inPieceCid)
	if pieceInfo.Defined() {
		return pieceInfo, isUnsealed, nil
	}
	if piecesErr != nil {
		return piecestore.PieceInfoUndefined, false, piecesErr
	}
	return piecestore.PieceInfoUndefined, false, fmt.Errorf("unknown pieceCID %s", pieceCID.String())
}
