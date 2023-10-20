package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/filecoin-project/boost/datatransfer"

	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes/migrations"
	"github.com/hannahhoward/go-pubsub"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/peer"
)

var allSelectorBytes []byte

func init() {
	buf := new(bytes.Buffer)
	_ = dagcbor.Encode(selectorparse.CommonSelector_ExploreAllRecursively, buf)
	allSelectorBytes = buf.Bytes()
}

type requestValidator struct {
	ctx  context.Context
	psub *pubsub.PubSub
	ValidationDeps
}

func newRequestValidator(vdeps ValidationDeps) *requestValidator {
	return &requestValidator{ValidationDeps: vdeps, psub: pubsub.New(queryValidationDispatcher)}
}

// validatePullRequest validates a request for data. This can be the initial
// request to pull data or a new request created when the data transfer is
// restarted (eg after a connection failure).
func (rv *requestValidator) validatePullRequest(isRestart bool, receiver peer.ID, voucher datatransfer.Voucher, baseCid cid.Cid, selector ipld.Node) (datatransfer.VoucherResult, error) {
	proposal, ok := voucher.(*legacyretrievaltypes.DealProposal)
	var legacyProtocol bool
	if !ok {
		legacyProposal, ok := voucher.(*migrations.DealProposal0)
		if !ok {
			return nil, errors.New("wrong voucher type")
		}
		newProposal := migrations.MigrateDealProposal0To1(*legacyProposal)
		proposal = &newProposal
		legacyProtocol = true
	}
	response, err := rv.validatePull(receiver, proposal, legacyProtocol, baseCid, selector)
	_ = rv.psub.Publish(legacyretrievaltypes.ProviderValidationEvent{
		IsRestart: isRestart,
		Receiver:  receiver,
		Proposal:  proposal,
		BaseCid:   baseCid,
		Selector:  selector,
		Response:  &response,
		Error:     err,
	})
	if legacyProtocol {
		downgradedResponse := migrations.DealResponse0{
			Status:      response.Status,
			ID:          response.ID,
			Message:     response.Message,
			PaymentOwed: response.PaymentOwed,
		}
		return &downgradedResponse, err
	}
	return &response, err
}

func (rv *requestValidator) validatePull(receiver peer.ID, proposal *legacyretrievaltypes.DealProposal, legacyProtocol bool, baseCid cid.Cid, selector ipld.Node) (legacyretrievaltypes.DealResponse, error) {
	response := legacyretrievaltypes.DealResponse{
		ID:     proposal.ID,
		Status: legacyretrievaltypes.DealStatusAccepted,
	}

	// Decide whether to accept the deal
	err := rv.acceptDeal(receiver, proposal, legacyProtocol, baseCid, selector)
	if err != nil {
		response.Status = legacyretrievaltypes.DealStatusRejected
		response.Message = err.Error()
		return response, err
	}

	return response, nil
}

func (rv *requestValidator) acceptDeal(receiver peer.ID, proposal *legacyretrievaltypes.DealProposal, legacyProtocol bool, baseCid cid.Cid, selector ipld.Node) error {
	// Check the proposal CID matches
	if proposal.PayloadCID != baseCid {
		return errors.New("incorrect CID for this proposal")
	}

	// Check the proposal selector matches
	buf := new(bytes.Buffer)
	err := dagcbor.Encode(selector, buf)
	if err != nil {
		return err
	}
	bytesCompare := allSelectorBytes
	if proposal.SelectorSpecified() {
		bytesCompare = proposal.Selector.Raw
	}
	if !bytes.Equal(buf.Bytes(), bytesCompare) {
		return errors.New("incorrect selector for this proposal")
	}

	// Check if the piece is unsealed
	_, isUnsealed, err := rv.getPiece(proposal.PayloadCID, proposal.PieceCID)
	if err != nil {
		if errors.Is(err, legacyretrievaltypes.ErrNotFound) {
			return fmt.Errorf("there is no piece containing payload cid %s: %w", proposal.PayloadCID, err)
		}
		return err
	}
	if !isUnsealed {
		return fmt.Errorf("there is no unsealed piece containing payload cid %s", proposal.PayloadCID)
	}

	// Check the retrieval ask price
	ask := rv.AskStore.GetAsk()
	if ask == nil {
		return fmt.Errorf("retrieval ask price is not configured")
	}

	// Check if the price per byte is non-zero.
	// Note that we don't check the unseal price, because we only serve
	// unsealed copies, so the unseal price is irrelevant.
	if !ask.PricePerByte.IsZero() {
		return fmt.Errorf("request for unpaid retrieval but ask price is non-zero: %d per byte", ask.PricePerByte)
	}
	if err != nil {
		return err
	}

	// Check the deal filter
	if rv.DealDecider != nil {
		state := legacyretrievaltypes.ProviderDealState{
			DealProposal:    *proposal,
			Receiver:        receiver,
			LegacyProtocol:  legacyProtocol,
			CurrentInterval: proposal.PaymentInterval,
		}
		accepted, reason, err := rv.DealDecider(rv.ctx, state)
		if err != nil {
			return fmt.Errorf("error running retrieval filter: %w", err)
		}
		if !accepted {
			return fmt.Errorf("rejected by retrieval filter: %s", reason)
		}
	}

	return nil
}

// Get the best piece containing the payload cid (first unsealed piece)
func (rv *requestValidator) getPiece(payloadCid cid.Cid, pieceCID *cid.Cid) (PieceInfo, bool, error) {
	inPieceCid := cid.Undef
	if pieceCID != nil {
		inPieceCid = *pieceCID
	}

	pieces, piecesErr := GetAllPieceInfoForPayload(rv.ctx, rv.PieceDirectory, payloadCid)
	pieceInfo, isUnsealed := GetBestPieceInfoMatch(rv.ctx, rv.SectorAccessor, pieces, inPieceCid)
	if pieceInfo.PieceCID.Defined() && len(pieceInfo.Deals) > 0 {
		return pieceInfo, isUnsealed, nil
	}
	if piecesErr != nil {
		return PieceInfo{}, false, piecesErr
	}
	return PieceInfo{}, false, fmt.Errorf("piece cid not found for payload cid %s", payloadCid.String())
}

func (rv *requestValidator) Subscribe(subscriber ProviderValidationSubscriber) legacyretrievaltypes.Unsubscribe {
	return legacyretrievaltypes.Unsubscribe(rv.psub.Subscribe(subscriber))
}

func queryValidationDispatcher(evt pubsub.Event, subscriberFn pubsub.SubscriberFn) error {
	e, ok := evt.(legacyretrievaltypes.ProviderValidationEvent)
	if !ok {
		return errors.New("wrong type of event")
	}
	cb, ok := subscriberFn.(ProviderValidationSubscriber)
	if !ok {
		return errors.New("wrong type of callback")
	}
	cb(e)
	return nil
}
