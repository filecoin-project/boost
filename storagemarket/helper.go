package storagemarket

import (
	"bytes"
	"context"

	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/lotus/api/v0api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	ctypes "github.com/filecoin-project/lotus/chain/types"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

type ChainDealManager struct {
	fullnodeApi v0api.FullNode
}

func NewChainDealManager(a v0api.FullNode) *ChainDealManager {
	return &ChainDealManager{a}
}

func (c *ChainDealManager) WaitForPublishDeals(ctx context.Context, publishCid cid.Cid, proposal market2.DealProposal) (*storagemarket.PublishDealsWaitResult, error) {
	// Wait for deal to be published (plus additional time for confidence)
	receipt, err := c.fullnodeApi.StateWaitMsg(ctx, publishCid, 2*build.MessageConfidence)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals errored: %w", err)
	}
	if receipt.Receipt.ExitCode != exitcode.Ok {
		return nil, xerrors.Errorf("WaitForPublishDeals exit code: %s", receipt.Receipt.ExitCode)
	}

	// The deal ID may have changed since publish if there was a reorg, so
	// get the current deal ID
	head, err := c.fullnodeApi.ChainHead(ctx)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals failed to get chain head: %w", err)
	}

	res, err := c.GetCurrentDealInfo(ctx, head.Key(), (*market.DealProposal)(&proposal), publishCid)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals getting deal info errored: %w", err)
	}

	return &storagemarket.PublishDealsWaitResult{DealID: res.DealID, FinalCid: receipt.Message}, nil
}

// GetCurrentDealInfo gets the current deal state and deal ID.
// Note that the deal ID is assigned when the deal is published, so it may
// have changed if there was a reorg after the deal was published.
func (c *ChainDealManager) GetCurrentDealInfo(ctx context.Context, tok ctypes.TipSetKey, proposal *market.DealProposal, publishCid cid.Cid) (CurrentDealInfo, error) {
	// Lookup the deal ID by comparing the deal proposal to the proposals in
	// the publish deals message, and indexing into the message return value
	dealID, pubMsgTok, err := c.dealIDFromPublishDealsMsg(ctx, tok, proposal, publishCid)
	if err != nil {
		return CurrentDealInfo{}, err
	}

	// Lookup the deal state by deal ID
	marketDeal, err := c.fullnodeApi.StateMarketStorageDeal(ctx, dealID, tok)
	if err == nil && proposal != nil {
		// Make sure the retrieved deal proposal matches the target proposal
		equal, err := c.CheckDealEquality(ctx, tok, *proposal, marketDeal.Proposal)
		if err != nil {
			return CurrentDealInfo{}, err
		}
		if !equal {
			return CurrentDealInfo{}, xerrors.Errorf("Deal proposals for publish message %s did not match", publishCid)
		}
	}
	return CurrentDealInfo{DealID: dealID, MarketDeal: marketDeal, PublishMsgTipSet: pubMsgTok}, err
}

// dealIDFromPublishDealsMsg looks up the publish deals message by cid, and finds the deal ID
// by looking at the message return value
func (c *ChainDealManager) dealIDFromPublishDealsMsg(ctx context.Context, tok ctypes.TipSetKey, proposal *market.DealProposal, publishCid cid.Cid) (abi.DealID, ctypes.TipSetKey, error) {
	dealID := abi.DealID(0)

	// Get the return value of the publish deals message
	wmsg, err := c.fullnodeApi.StateSearchMsg(ctx, publishCid)
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("getting publish deals message return value: %w", err)
	}

	if wmsg == nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("looking for publish deal message %s: not found", publishCid)
	}

	if wmsg.Receipt.ExitCode != exitcode.Ok {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("looking for publish deal message %s: non-ok exit code: %s", publishCid, wmsg.Receipt.ExitCode)
	}

	nv, err := c.fullnodeApi.StateNetworkVersion(ctx, wmsg.TipSet)
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("getting network version: %w", err)
	}

	retval, err := market.DecodePublishStorageDealsReturn(wmsg.Receipt.Return, nv)
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("looking for publish deal message %s: decoding message return: %w", publishCid, err)
	}

	dealIDs, err := retval.DealIDs()
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("looking for publish deal message %s: getting dealIDs: %w", publishCid, err)
	}

	// Get the parameters to the publish deals message
	pubmsg, err := c.fullnodeApi.ChainGetMessage(ctx, publishCid)
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("getting publish deal message %s: %w", publishCid, err)
	}

	var pubDealsParams market2.PublishStorageDealsParams
	if err := pubDealsParams.UnmarshalCBOR(bytes.NewReader(pubmsg.Params)); err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("unmarshalling publish deal message params for message %s: %w", publishCid, err)
	}

	// Scan through the deal proposals in the message parameters to find the
	// index of the target deal proposal
	dealIdx := -1
	for i, paramDeal := range pubDealsParams.Deals {
		eq, err := c.CheckDealEquality(ctx, tok, *proposal, market.DealProposal(paramDeal.Proposal))
		if err != nil {
			return dealID, ctypes.EmptyTSK, xerrors.Errorf("comparing publish deal message %s proposal to deal proposal: %w", publishCid, err)
		}
		if eq {
			dealIdx = i
			break
		}
	}

	if dealIdx == -1 {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("could not find deal in publish deals message %s", publishCid)
	}

	if dealIdx >= len(dealIDs) {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf(
			"deal index %d out of bounds of deals (len %d) in publish deals message %s",
			dealIdx, len(dealIDs), publishCid)
	}

	valid, err := retval.IsDealValid(uint64(dealIdx))
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("determining deal validity: %w", err)
	}

	if !valid {
		return dealID, ctypes.EmptyTSK, xerrors.New("deal was invalid at publication")
	}

	return dealIDs[dealIdx], wmsg.TipSet, nil
}
func (c *ChainDealManager) CheckDealEquality(ctx context.Context, tok ctypes.TipSetKey, p1, p2 market.DealProposal) (bool, error) {
	p1ClientID, err := c.fullnodeApi.StateLookupID(ctx, p1.Client, tok)
	if err != nil {
		return false, err
	}
	p2ClientID, err := c.fullnodeApi.StateLookupID(ctx, p2.Client, tok)
	if err != nil {
		return false, err
	}
	return p1.PieceCID.Equals(p2.PieceCID) &&
		p1.PieceSize == p2.PieceSize &&
		p1.VerifiedDeal == p2.VerifiedDeal &&
		p1.Label == p2.Label &&
		p1.StartEpoch == p2.StartEpoch &&
		p1.EndEpoch == p2.EndEpoch &&
		p1.StoragePricePerEpoch.Equals(p2.StoragePricePerEpoch) &&
		p1.ProviderCollateral.Equals(p2.ProviderCollateral) &&
		p1.ClientCollateral.Equals(p2.ClientCollateral) &&
		p1.Provider == p2.Provider &&
		p1ClientID == p2ClientID, nil
}
