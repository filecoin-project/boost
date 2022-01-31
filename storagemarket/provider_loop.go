package storagemarket

import (
	"fmt"
	"time"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/sealingpipeline"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
)

type acceptDealReq struct {
	rsp  chan acceptDealResp
	deal *types.ProviderDealState
	dh   *dealHandler
}

type acceptDealResp struct {
	ri  *api.ProviderDealRejectionInfo
	err error
}

type finishedDealReq struct {
	deal *types.ProviderDealState
	done chan struct{}
}

type publishDealReq struct {
	deal *types.ProviderDealState
	done chan struct{}
}

func (p *Provider) processDealRequest(deal *types.ProviderDealState) (bool, string, error) {
	// get current sealing pipeline status
	status, err := sealingpipeline.GetStatus(p.ctx, p.fullnodeApi, p.sps)
	if err != nil {
		return false, "server error", err
	}

	// run custom decision logic
	params := types.DealParams{
		DealUUID:             deal.DealUuid,
		ClientDealProposal:   deal.ClientDealProposal,
		DealDataRoot:         deal.DealDataRoot,
		Transfer:             deal.Transfer,
		SealingPipelineState: status,
	}
	accept, reason, err := p.df(p.ctx, params)
	if err != nil {
		return false, err.Error(), nil
	}

	if !accept {
		return false, reason, nil
	}

	cleanup := func() {
		errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
		if errf != nil && !xerrors.Is(errf, db.ErrNotFound) {
			log.Errorw("untagging funds", "id", deal.DealUuid, "err", errf)
		}

		errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
		if errs != nil && !xerrors.Is(errf, db.ErrNotFound) {
			log.Errorw("untagging storage", "id", deal.DealUuid, "err", errs)
		}
	}

	// tag the funds required for escrow and sending the publish deal message
	// so that they are not used for other deals
	err = p.fundManager.TagFunds(p.ctx, deal.DealUuid, deal.ClientDealProposal.Proposal)
	if err != nil {
		cleanup()

		return false, err.Error(), nil
	}

	// tag the storage required for the deal in the staging area
	err = p.storageManager.Tag(p.ctx, deal.DealUuid, deal.Transfer.Size)
	if err != nil {
		cleanup()

		return false, err.Error(), nil
	}

	// write deal state to the database
	log.Infow("inserting deal into DB", "id", deal.DealUuid)
	deal.CreatedAt = time.Now()
	deal.Checkpoint = dealcheckpoints.Accepted
	err = p.dealsDB.Insert(p.ctx, deal)
	if err != nil {
		cleanup()

		return false, "", fmt.Errorf("failed to insert deal in db: %w", err)
	}

	log.Infow("inserted deal into DB", "id", deal.DealUuid)

	return true, "", nil
}

func (p *Provider) loop() {
	defer p.wg.Done()

	for {
		select {
		case dealReq := <-p.acceptDealChan:
			deal := dealReq.deal
			log.Infow("process accept deal request", "id", deal.DealUuid)

			ok, reason, err := p.processDealRequest(dealReq.deal)
			if !ok {
				if err != nil {
					log.Error("rejecting storage deal", "err", err)

					dealReq.rsp <- acceptDealResp{ri: &api.ProviderDealRejectionInfo{Accepted: false, Reason: reason}, err: err}
					continue
				}

				log.Warnw("rejecting storage deal", "reason", reason)

				dealReq.rsp <- acceptDealResp{ri: &api.ProviderDealRejectionInfo{Accepted: false, Reason: reason}, err: nil}
				continue
			}

			// start executing the deal
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				p.doDeal(deal, dealReq.dh)
			}()

			dealReq.rsp <- acceptDealResp{&api.ProviderDealRejectionInfo{Accepted: true}, nil}
			log.Infow("deal execution started", "id", deal.DealUuid)

		case publishedDeal := <-p.publishedDealChan:
			deal := publishedDeal.deal
			errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
			if errf != nil {
				log.Errorw("untagging funds", "id", deal.DealUuid, "err", errf)
			}
			publishedDeal.done <- struct{}{}

		case finishedDeal := <-p.finishedDealChan:
			deal := finishedDeal.deal
			log.Infow("deal finished", "id", deal.DealUuid)
			errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
			if errf != nil && !xerrors.Is(errf, db.ErrNotFound) {
				log.Errorw("untagging funds", "id", deal.DealUuid, "err", errf)
			}

			errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
			if errs != nil && !xerrors.Is(errs, db.ErrNotFound) {
				log.Errorw("untagging storage", "id", deal.DealUuid, "err", errs)
			}
			finishedDeal.done <- struct{}{}

		case <-p.ctx.Done():
			return
		}
	}
}
