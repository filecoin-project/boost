package storagemarket

import (
	"fmt"
	"time"

	"github.com/filecoin-project/boost/db"
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
	accepted bool
	ri       *api.ProviderDealRejectionInfo
	err      error
}

type finishedDealsReq struct {
	deal *types.ProviderDealState
	done chan struct{}
}

func (p *Provider) loop() {
	defer p.wg.Done()

	for {
		select {
		case dealReq := <-p.acceptDealsChan:
			deal := dealReq.deal
			log.Infow("process accept deal request", "id", deal.DealUuid)

			// setup cleanup function
			cleanup := func() {
				errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
				if errf != nil && !xerrors.Is(errf, db.ErrDealNotFound) {
					log.Errorw("untagging funds", "id", deal.DealUuid, "err", errf)
				}

				errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
				if errs != nil && !xerrors.Is(errf, db.ErrDealNotFound) {
					log.Errorw("untagging storage", "id", deal.DealUuid, "err", errs)
				}
			}

			// Tag the funds required for escrow and sending the publish deal message
			// so that they are not used for other deals
			err := p.fundManager.TagFunds(p.ctx, deal.DealUuid, deal.ClientDealProposal.Proposal)
			if err != nil {
				cleanup()
				dealReq.rsp <- acceptDealResp{accepted: false, ri: &api.ProviderDealRejectionInfo{Reason: err.Error()}, err: nil}
				continue
			}

			// Tag the storage required for the deal in the staging area
			err = p.storageManager.Tag(p.ctx, deal.DealUuid, deal.ClientDealProposal.Proposal.PieceSize)
			if err != nil {
				cleanup()
				dealReq.rsp <- acceptDealResp{accepted: false, ri: &api.ProviderDealRejectionInfo{Reason: err.Error()}, err: nil}
				continue
			}

			// write deal state to the database
			log.Infow("inserting deal into DB", "id", deal.DealUuid)
			deal.CreatedAt = time.Now()
			deal.Checkpoint = dealcheckpoints.Accepted
			err = p.dealsDB.Insert(p.ctx, deal)
			if err != nil {
				cleanup()
				dealReq.rsp <- acceptDealResp{false, nil, fmt.Errorf("failed to insert deal in db: %w", err)}
				continue
			}
			log.Infow("inserted deal into DB", "id", deal.DealUuid)

			// start executing the deal
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				p.doDeal(deal, dealReq.dh)
			}()

			dealReq.rsp <- acceptDealResp{true, nil, nil}
			log.Infow("deal execution started", "id", deal.DealUuid)

		case finishedDeal := <-p.finishedDealsChan:
			deal := finishedDeal.deal
			log.Errorw("deal finished", "id", deal.DealUuid)
			errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
			if errf != nil {
				log.Errorw("untagging funds", "id", deal.DealUuid, "err", errf)
			}

			errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
			if errs != nil {
				log.Errorw("untagging storage", "id", deal.DealUuid, "err", errs)
			}
			finishedDeal.done <- struct{}{}

		case <-p.ctx.Done():
			return
		}
	}
}
