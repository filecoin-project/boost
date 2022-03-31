package storagemarket

import (
	"fmt"
	"time"

	"github.com/filecoin-project/boost/fundmanager"
	"github.com/google/uuid"

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

type storageSpaceDealReq struct {
	deal *types.ProviderDealState
	done chan struct{}
}

func (p *Provider) logFunds(id uuid.UUID, trsp *fundmanager.TagFundsResp) {
	p.dealLogger.Infow(id, "tagged funds for deal",
		"tagged for deal publish", trsp.PublishMessage,
		"tagged for deal collateral", trsp.Collateral,
		"total tagged for publish", trsp.TotalPublishMessage,
		"total tagged for collateral", trsp.TotalCollateral,
		"total available for publish", trsp.AvailablePublishMessage,
		"total available for collateral", trsp.AvailableCollateral)
}

func (p *Provider) processDealRequest(deal *types.ProviderDealState) (bool, string, error) {
	// get current sealing pipeline status
	status, err := sealingpipeline.GetStatus(p.ctx, p.fullnodeApi, p.sps)
	if err != nil {
		return false, "server error", fmt.Errorf("failed to fetch sealing pipleine status: %w", err)
	}

	// run custom decision logic
	params := types.DealParams{
		DealUUID:           deal.DealUuid,
		ClientDealProposal: deal.ClientDealProposal,
		DealDataRoot:       deal.DealDataRoot,
		Transfer:           deal.Transfer,
	}

	accept, reason, err := p.df(p.ctx, types.DealFilterParams{
		DealParams:           &params,
		SealingPipelineState: status})

	if err != nil {
		return false, "deal filter error", fmt.Errorf("failed to invoke deal filter: %w", err)
	}

	if !accept {
		return false, reason, nil
	}

	cleanup := func() {
		collat, pub, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
		if errf != nil && !xerrors.Is(errf, db.ErrNotFound) {
			p.dealLogger.LogError(deal.DealUuid, "failed to untag funds during deal cleanup", errf)
		} else if errf == nil {
			p.dealLogger.Infow(deal.DealUuid, "untagged funds for deal cleanup", "untagged publish", pub, "untagged collateral", collat,
				"err", errf)
		}

		errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
		if errs != nil && !xerrors.Is(errs, db.ErrNotFound) {
			p.dealLogger.LogError(deal.DealUuid, "failed to untag storage during deal cleanup", errs)
		} else if errs == nil {
			p.dealLogger.Infow(deal.DealUuid, "untagged storage for deal cleanup", deal.Transfer.Size)
		}
	}

	// tag the funds required for escrow and sending the publish deal message
	// so that they are not used for other deals
	trsp, err := p.fundManager.TagFunds(p.ctx, deal.DealUuid, deal.ClientDealProposal.Proposal)
	if err != nil {
		cleanup()

		return false, "server error", fmt.Errorf("failed to tag funds for deal: %w", err)
	}
	p.logFunds(deal.DealUuid, trsp)

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

func (p *Provider) processOfflineDealRequest(deal *types.ProviderDealState) (bool, string, error) {
	// tag funds and start executing deal
	cleanup := func() {
		collat, pub, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
		if errf != nil && !xerrors.Is(errf, db.ErrNotFound) {
			p.dealLogger.LogError(deal.DealUuid, "failed to untag funds during deal cleanup", errf)
		} else if errf == nil {
			p.dealLogger.Infow(deal.DealUuid, "untagged funds for deal cleanup", "untagged publish", pub, "untagged collateral", collat)
		}
	}

	// tag the funds required for escrow and sending the publish deal message
	// so that they are not used for other deals
	trsp, err := p.fundManager.TagFunds(p.ctx, deal.DealUuid, deal.ClientDealProposal.Proposal)
	if err != nil {
		cleanup()
		return false, "server error", fmt.Errorf("failed to tag funds for deal: %w", err)
	}
	p.logFunds(deal.DealUuid, trsp)
	return true, "", nil
}

func (p *Provider) loop() {
	defer func() {
		p.wg.Done()
		log.Info("provider event loop complete")
	}()

	for {
		select {
		case dealReq := <-p.acceptDealChan:
			deal := dealReq.deal
			p.dealLogger.Infow(deal.DealUuid, "processing deal acceptance request")

			var ok bool
			var reason string
			var err error
			if deal.IsOffline {
				ok, reason, err = p.processOfflineDealRequest(dealReq.deal)
			} else {
				ok, reason, err = p.processDealRequest(dealReq.deal)
			}
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
				p.doDeal(deal, dealReq.dh)
				p.dealLogger.Infow(deal.DealUuid, "deal go-routine finished execution")
			}()

			dealReq.rsp <- acceptDealResp{&api.ProviderDealRejectionInfo{Accepted: true}, nil}

		case storageSpaceDealReq := <-p.storageSpaceChan:
			deal := storageSpaceDealReq.deal
			if err := p.storageManager.Untag(p.ctx, deal.DealUuid); err != nil && !xerrors.Is(err, db.ErrNotFound) {
				p.dealLogger.LogError(deal.DealUuid, "failed to untag storage space", err)
			} else {
				p.dealLogger.Infow(deal.DealUuid, "untagged storage space")
			}
			close(storageSpaceDealReq.done)

		case publishedDeal := <-p.publishedDealChan:
			deal := publishedDeal.deal
			collat, pub, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
			if errf != nil {
				p.dealLogger.LogError(deal.DealUuid, "failed to untag funds", errf)
			} else {
				p.dealLogger.Infow(deal.DealUuid, "untagged funds for deal after publish", "untagged publish", pub, "untagged collateral", collat)
			}
			publishedDeal.done <- struct{}{}

		case finishedDeal := <-p.finishedDealChan:
			deal := finishedDeal.deal
			p.dealLogger.Infow(deal.DealUuid, "deal finished")
			collat, pub, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
			if errf != nil && !xerrors.Is(errf, db.ErrNotFound) {
				p.dealLogger.LogError(deal.DealUuid, "failed to untag funds", errf)
			} else if errf == nil {
				p.dealLogger.Infow(deal.DealUuid, "untagged funds for deal as deal finished", "untagged publish", pub, "untagged collateral", collat,
					"err", errf)
			}

			errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
			if errs != nil && !xerrors.Is(errs, db.ErrNotFound) {
				p.dealLogger.LogError(deal.DealUuid, "failed to untag storage", errs)
			} else if errs == nil {
				p.dealLogger.Infow(deal.DealUuid, "untagged storage space for deal")
			}
			finishedDeal.done <- struct{}{}

		case <-p.ctx.Done():
			return
		}
	}
}
