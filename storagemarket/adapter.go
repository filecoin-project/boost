package storagemarket

// this file implements storagemarket.StorageProviderNode

// TODO: we should remove this, and incorporate in the actual Provider

import (
	"context"
	"io"
	"time"

	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"

	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/storage/sectorblocks"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/extern/storage-sealing"
	"github.com/filecoin-project/lotus/lib/sigs"
	"github.com/filecoin-project/lotus/markets/utils"
)

var addPieceRetryWait = 5 * time.Minute
var addPieceRetryTimeout = 6 * time.Hour

type Adapter struct {
	v1api.FullNode

	secb *sectorblocks.SectorBlocks
	//ev   *events.Events

	addBalanceSpec              *api.MessageSendSpec
	maxDealCollateralMultiplier uint64
	//dsMatcher                   *dealStateMatcher
	scMgr *SectorCommittedManager
}

func (n *Adapter) AddPieceToSector(ctx context.Context, deal smtypes.ProviderDealState, pieceData io.Reader) (*storagemarket.PackingResult, error) {
	// Sanity check - we must have published the deal before handing it off
	// to the sealing subsystem
	if deal.PublishCID == nil {
		return nil, xerrors.Errorf("deal.PublishCid can't be nil")
	}

	sdInfo := api.PieceDealInfo{
		DealID:       deal.ChainDealID,
		DealProposal: &deal.ClientDealProposal.Proposal,
		PublishCid:   deal.PublishCID,
		DealSchedule: api.DealSchedule{
			StartEpoch: deal.ClientDealProposal.Proposal.StartEpoch,
			EndEpoch:   deal.ClientDealProposal.Proposal.EndEpoch,
		},
		// Assume that it doesn't make sense for a miner not to keep an
		// unsealed copy. TODO: Check that's a valid assumption.
		//KeepUnsealed: deal.FastRetrieval,
		KeepUnsealed: true,
	}

	// Attempt to add the piece to a sector (repeatedly if necessary)
	pieceSize := deal.ClientDealProposal.Proposal.PieceSize.Unpadded()
	p, offset, err := n.secb.AddPiece(ctx, pieceSize, pieceData, sdInfo)
	curTime := build.Clock.Now()
	for build.Clock.Since(curTime) < addPieceRetryTimeout {
		if !xerrors.Is(err, sealing.ErrTooManySectorsSealing) {
			if err != nil {
				log.Errorw("failed to addPiece for deal", "id", deal.DealUuid, "err", err)
			}
			break
		}
		select {
		case <-build.Clock.After(addPieceRetryWait):
			p, offset, err = n.secb.AddPiece(ctx, pieceSize, pieceData, sdInfo)
		case <-ctx.Done():
			return nil, xerrors.New("context expired while waiting to retry AddPiece")
		}
	}

	if err != nil {
		return nil, xerrors.Errorf("AddPiece failed: %s", err)
	}
	log.Infow("Added new deal to sector", "id", deal.DealUuid, "sector", p)

	return &storagemarket.PackingResult{
		SectorNumber: p,
		Offset:       offset,
		Size:         pieceSize.Padded(),
	}, nil
}

func (n *Adapter) VerifySignature(ctx context.Context, sig crypto.Signature, addr address.Address, input []byte, encodedTs shared.TipSetToken) (bool, error) {
	addr, err := n.StateAccountKey(ctx, addr, types.EmptyTSK)
	if err != nil {
		return false, err
	}

	err = sigs.Verify(&sig, addr, input)
	return err == nil, err
}

func (n *Adapter) GetBalance(ctx context.Context, addr address.Address, encodedTs shared.TipSetToken) (storagemarket.Balance, error) {
	tsk, err := types.TipSetKeyFromBytes(encodedTs)
	if err != nil {
		return storagemarket.Balance{}, err
	}

	bal, err := n.StateMarketBalance(ctx, addr, tsk)
	if err != nil {
		return storagemarket.Balance{}, err
	}

	return utils.ToSharedBalance(bal), nil
}

func (n *Adapter) WaitForPublishDeals(ctx context.Context, publishCid cid.Cid, proposal market2.DealProposal) (*storagemarket.PublishDealsWaitResult, error) {
	// Wait for deal to be published (plus additional time for confidence)
	receipt, err := n.StateWaitMsg(ctx, publishCid, 2*build.MessageConfidence, api.LookbackNoLimit, true)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals errored: %w", err)
	}
	if receipt.Receipt.ExitCode != exitcode.Ok {
		return nil, xerrors.Errorf("WaitForPublishDeals exit code: %s", receipt.Receipt.ExitCode)
	}

	// The deal ID may have changed since publish if there was a reorg, so
	// get the current deal ID
	//head, err := n.ChainHead(ctx)
	//if err != nil {
	//return nil, xerrors.Errorf("WaitForPublishDeals failed to get chain head: %w", err)
	//}

	//res, err := n.scMgr.dealInfo.GetCurrentDealInfo(ctx, head.Key().Bytes(), (*market.DealProposal)(&proposal), publishCid)
	//if err != nil {
	//return nil, xerrors.Errorf("WaitForPublishDeals getting deal info errored: %w", err)
	//}

	return &storagemarket.PublishDealsWaitResult{DealID: abi.DealID(4), FinalCid: receipt.Message}, nil
	//return &storagemarket.PublishDealsWaitResult{DealID: res.DealID, FinalCid: receipt.Message}, nil
}
