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
			log.Infow("finished restarting deal", "id", restartReq.deal.DealUuid)

		case dealReq := <-p.acceptDealsChan:
			deal := dealReq.deal
			log.Infow("process accept deal request", "id", deal.DealUuid)

			writeRespAsync := func(accepted bool, ri *api.ProviderDealRejectionInfo, err error) {
				go func() {
					select {
					case dealReq.rsp <- acceptDealResp{accepted, ri, err}:
					case <-p.ctx.Done():
					}
				}()
			}

			// TODO: Deal filter, storage space manager, fund manager etc . basically synchronization
			// send rejection if deal is not accepted by the above filters
			var err error
			if err != nil {
				writeRespAsync(false, nil, err)
				continue
			}
			accepted := true
			if !accepted {
				writeRespAsync(false, &api.ProviderDealRejectionInfo{}, nil)
				continue
			}

			// write deal state to the database
			log.Infow("inserting deal into DB", "id", deal.DealUuid)
			deal.CreatedAt = time.Now()
			deal.Checkpoint = dealcheckpoints.New

			err = p.db.Insert(p.ctx, deal)
			if err != nil {
				writeRespAsync(false, nil, fmt.Errorf("failed to insert deal in db: %w", err))
				continue
			}
			log.Infow("finished inserting deal into DB", "id", deal.DealUuid)

			// start executing the deal
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				p.doDeal(deal)
			}()

			writeRespAsync(true, nil, nil)
			log.Infow("deal execution started", "id", deal.DealUuid)

		case failedDeal := <-p.failedDealsChan:
			log.Errorw("deal failed", "id", failedDeal.st.DealUuid, "err", failedDeal.err)
			// Release storage space , funds, shared resources etc etc.

		case <-p.ctx.Done():
			return
		}
	}
}
