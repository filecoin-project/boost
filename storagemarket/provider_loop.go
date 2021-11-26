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

type restartReq struct {
	deal *types.ProviderDealState
}

func (p *Provider) loop() {
	defer p.wg.Done()

	for {
		select {
		case restartReq := <-p.restartDealsChan:
			log.Infow("restarting deal", "id", restartReq.deal.DealUuid)

			// Put ANY RESTART SYNCHRONIZATION LOGIC HERE.
			// ....
			//
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()

				p.doDeal(restartReq.deal)
			}()
			log.Infow("restarted deal", "id", restartReq.deal.DealUuid)

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

			// write deal state to the database
			log.Infow("inserting deal into DB", "id", deal.DealUuid)

			deal.CreatedAt = time.Now()
			deal.Checkpoint = dealcheckpoints.New

			err = p.dealsDB.Insert(p.ctx, deal)
			if err != nil {
				go writeDealResp(false, nil, fmt.Errorf("failed to insert deal in db: %w", err))
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
