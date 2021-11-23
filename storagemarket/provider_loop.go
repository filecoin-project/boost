package storagemarket

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/google/uuid"
	"golang.org/x/xerrors"
)

var ErrDealNotFound = fmt.Errorf("deal not found")

func (p *Provider) Deal(ctx context.Context, dealUuid uuid.UUID) (*types.ProviderDealState, error) {
	deal, err := p.db.ByID(ctx, dealUuid)
	if xerrors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("getting deal %s: %w", dealUuid, ErrDealNotFound)
	}
	return deal, nil
}

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

// TODO: This is transient -> If it dosen't work out, we will use locks.
// 1:N will move this problem elsewhere.
func (p *Provider) loop() {
	defer p.wg.Done()

	for {
		select {
		case restartReq := <-p.restartDealsChan:
			log.Infow("restart deal", "id", restartReq.deal.DealUuid)

			// Put ANY RESTART SYNCHRONIZATION LOGIC HERE.
			// ....
			//
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()

				p.doDeal(restartReq.deal)
			}()

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

			var err error
			if err != nil {
				go writeDealResp(false, nil, err)
				continue
			}

			// TODO: Deal filter, storage space manager, fund manager etc . basically synchronization
			// send rejection if deal is not accepted by the above filters
			accepted := true
			if !accepted {
				go writeDealResp(false, &api.ProviderDealRejectionInfo{}, nil)
				continue
			}
			go writeDealResp(true, nil, nil)

			// write deal state to the database
			log.Infow("insert deal into DB", "id", deal.DealUuid)

			deal.CreatedAt = time.Now()
			deal.Checkpoint = dealcheckpoints.New

			err = p.db.Insert(p.ctx, deal)
			if err != nil {
				go writeDealResp(false, nil, err)
				continue
			}

			// start executing the deal
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()

				p.doDeal(deal)
			}()

		case failedDeal := <-p.failedDealsChan:
			log.Errorw("deal failed", "id", failedDeal.st.DealUuid, "err", failedDeal.err)
			// Release storage space , funds, shared resources etc etc.

		case <-p.ctx.Done():
			return
		}
	}
}
