package storagemarket

import (
	"fmt"
	"time"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
)

type acceptDealReq struct {
	rsp  chan acceptDealResp
	deal *types.ProviderDealState
}

type acceptDealResp struct {
	accepted bool
	ri       *api.ProviderDealRejectionInfo
	err      error
}

type failedDealReq struct {
	st  *types.ProviderDealState
	err error
}

func (p *Provider) loop() {
	defer p.wg.Done()

	for {
		select {
		case dealReq := <-p.acceptDealsChan:
			deal := dealReq.deal
			log.Infow("process accept deal request", "id", deal.DealUuid)

			writeDealResp := func(accepted bool, ri *api.ProviderDealRejectionInfo, err error) {
				select {
				case dealReq.rsp <- acceptDealResp{accepted, ri, err}:
				case <-p.ctx.Done():
					return
				}
			}

			// Tag the funds required for escrow and sending the publish deal message
			// so that they are not used for other deals
			err := p.fundManager.TagFunds(p.ctx, deal.DealUuid, deal.ClientDealProposal.Proposal)
			if err != nil {
				go writeDealResp(false, &api.ProviderDealRejectionInfo{
					// TODO: provide a custom reason message (instead of sending provider
					// error messages back to client) eg "Not enough provider funds for deal"
					Reason: err.Error(),
				}, nil)
				continue
			}

			// Tag the storage required for the deal in the staging area
			err = p.storageManager.Tag(p.ctx, deal.DealUuid, deal.ClientDealProposal.Proposal.PieceSize)
			if err != nil {
				go writeDealResp(false, &api.ProviderDealRejectionInfo{
					// TODO: provide a custom reason message (instead of sending provider
					// error messages back to client) eg "Not enough staging storage for deal"
					Reason: err.Error(),
				}, nil)

				errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
				if errf != nil {
					log.Errorw("untagging funds", "id", deal.DealUuid, "err", errf)
				}
				continue
			}

			// write deal state to the database
			log.Infow("inserting deal into DB", "id", deal.DealUuid)

			deal.CreatedAt = time.Now()
			deal.Checkpoint = dealcheckpoints.Accepted

			err = p.dealsDB.Insert(p.ctx, deal)
			if err != nil {
				go writeDealResp(false, nil, fmt.Errorf("failed to insert deal in db: %w", err))

				errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
				if errf != nil {
					log.Errorw("untagging funds", "id", deal.DealUuid, "err", errf)
				}

				errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
				if errs != nil {
					log.Errorw("untagging storage", "id", deal.DealUuid, "err", errs)
				}
				continue
			}
			log.Infow("inserted deal into DB", "id", deal.DealUuid)

			// start executing the deal
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()

				p.doDeal(deal)
			}()

			go writeDealResp(true, nil, nil)
			log.Infow("deal execution started", "id", deal.DealUuid)

		case failedDeal := <-p.failedDealsChan:
			log.Errorw("deal failed", "id", failedDeal.st.DealUuid, "err", failedDeal.err)
			// Release storage space , funds, shared resources etc etc.

		case <-p.ctx.Done():
			return
		}
	}
}
