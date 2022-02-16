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
		return false, "server error", fmt.Errorf("failed to fetch sealing pipleine status: %w", err)
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
		return false, "deal filter error", fmt.Errorf("failed to invoke deal filter: %w", err)
	}

	if !accept {
		return false, reason, nil
	}

	cleanup := func() {
		pub, collat, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
		if errf != nil && !xerrors.Is(errf, db.ErrNotFound) {
			p.dealLogger.LogError(deal.DealUuid, "failed to untag funds during deal cleanup", err)
		} else {
			p.dealLogger.Infow(deal.DealUuid, "untagged funds for deal cleanup", "untagged publish", pub, "untagged collateral", collat,
				"err", errf)
		}

		errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
		if errs != nil && !xerrors.Is(errf, db.ErrNotFound) {
			p.dealLogger.LogError(deal.DealUuid, "failed to untag storage during deal cleanup", err)
		}
	}

	// tag the funds required for escrow and sending the publish deal message
	// so that they are not used for other deals
	trsp, err := p.fundManager.TagFunds(p.ctx, deal.DealUuid, deal.ClientDealProposal.Proposal)
	if err != nil {
		cleanup()

		return false, "server error", fmt.Errorf("failed to tag funds for deal: %w", err)
	}
	p.dealLogger.Infow(deal.DealUuid, "tagged funds for deal",
		"tagged for deal publish", trsp.ForPublish,
		"tagged for deal collateral", trsp.ForCollat,
		"total tagged for publish", trsp.TotalTaggedPublish,
		"total tagged for collateral", trsp.TotalTaggedCollat,
		"total remaining for publish", trsp.RemainingPublish,
		"total remaining for collateral", trsp.RemainingCollat)

	// tag the storage required for the deal in the staging area
	err = p.storageManager.Tag(p.ctx, deal.DealUuid, deal.Transfer.Size)
	if err != nil {
		cleanup()

		return false, err.Error(), nil
	}

	// write deal state to the database
	deal.CreatedAt = time.Now()
	deal.Checkpoint = dealcheckpoints.Accepted
	err = p.dealsDB.Insert(p.ctx, deal)
	if err != nil {
		cleanup()

		return false, "server error", fmt.Errorf("failed to insert deal in db: %w", err)
	}

	p.dealLogger.Infow(deal.DealUuid, "inserted deal into deals DB")

	return true, "", nil
}

func (p *Provider) loop() {
	defer p.wg.Done()

	for {
		select {
		case dealReq := <-p.acceptDealChan:
			deal := dealReq.deal
			p.dealLogger.Infow(deal.DealUuid, "processing deal acceptance request")

			ok, reason, err := p.processDealRequest(dealReq.deal)
			if !ok {
				if err != nil {
					p.dealLogger.LogError(deal.DealUuid, "error while processing deal acceptance request", err)
					dealReq.rsp <- acceptDealResp{ri: &api.ProviderDealRejectionInfo{Accepted: false, Reason: reason}, err: err}
					continue
				}

				p.dealLogger.Infow(deal.DealUuid, "deal acceptance request rejected", "reason", reason)
				dealReq.rsp <- acceptDealResp{ri: &api.ProviderDealRejectionInfo{Accepted: false, Reason: reason}, err: nil}
				continue
			}

			// start executing the deal
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				p.doDeal(deal, dealReq.dh, false)
				p.dealLogger.Infow(deal.DealUuid, "deal go-routine finished execution")
			}()

			dealReq.rsp <- acceptDealResp{&api.ProviderDealRejectionInfo{Accepted: true}, nil}

		case publishedDeal := <-p.publishedDealChan:
			deal := publishedDeal.deal
			pub, collat, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
			if errf != nil {
				p.dealLogger.LogError(deal.DealUuid, "failed to untag funds", errf)
			} else {
				p.dealLogger.Infow(deal.DealUuid, "untagged funds for deal after publish", "untagged publish", pub, "untagged collateral", collat)
			}
			publishedDeal.done <- struct{}{}

		case finishedDeal := <-p.finishedDealChan:
			deal := finishedDeal.deal
			p.dealLogger.Infow(deal.DealUuid, "deal finished")
			pub, collat, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
			if errf != nil && !xerrors.Is(errf, db.ErrNotFound) {
				p.dealLogger.LogError(deal.DealUuid, "failed to untag funds", errf)
			} else {
				p.dealLogger.Infow(deal.DealUuid, "untagged funds for deal as deal finished", "untagged publish", pub, "untagged collateral", collat,
					"err", errf)
			}

			errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
			if errs != nil && !xerrors.Is(errs, db.ErrNotFound) {
				p.dealLogger.LogError(deal.DealUuid, "failed to untag storage", errs)
			}
			finishedDeal.done <- struct{}{}

		case <-p.ctx.Done():
			return
		}
	}
}
