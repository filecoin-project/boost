package storagemarket

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/google/uuid"
)

type acceptDealReq struct {
	rsp      chan acceptDealResp
	deal     *types.ProviderDealState
	isImport bool
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
	done chan error
}

type storageSpaceDealReq struct {
	deal *types.ProviderDealState
	done chan struct{}
}

type updateRetryStateReq struct {
	dealUuid uuid.UUID
	retry    bool // whether to retry or to terminate the deal
	done     chan error
}

type processedDealReq struct {
	err  *acceptError
	rsp  chan acceptDealResp
	deal *types.ProviderDealState
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

// acceptError is used to distinguish between a regular error and a severe error
type acceptError struct {
	error
	// isSevereError indicates whether the error is severe (eg can't connect
	// to database) or not (eg not enough funds for deal)
	isSevereError bool
	// The reason sent to the client for why their deal was rejected
	reason string
}

// we still need to call the BasicDealFilter() even when external deal filter is not set.
// Once BasicDealFilter() completes the checks like are we accepting online deal, verified deal etc.
// Then it runs the external "cmd" filter. Thus, runDealFilters is not optional of any type of deal
func (p *Provider) runDealFilters(deal *types.ProviderDealState) *acceptError {

	// run custom storage deal filter decision logic
	dealFilterParams, aerr := p.getDealFilterParams(deal)
	if aerr != nil {
		return aerr
	}
	accept, reason, err := p.df(p.ctx, *dealFilterParams)
	if err != nil {
		return &acceptError{
			error:         fmt.Errorf("failed to invoke deal filter: %w", err),
			reason:        "server error: deal filter error",
			isSevereError: true,
		}
	}

	if !accept {
		return &acceptError{
			error:         fmt.Errorf("deal filter rejected deal: %s", reason),
			reason:        reason,
			isSevereError: false,
		}
	}
	return nil
}

func (p *Provider) processOnlineDealProposal(deal *types.ProviderDealState) *acceptError {
	host, err := deal.Transfer.Host()
	if err != nil {
		return &acceptError{
			error:         fmt.Errorf("failed to get deal transfer host: %w", err),
			reason:        fmt.Sprintf("server error: get deal transfer host: %s", err),
			isSevereError: false,
		}
	}

	// Check that the deal proposal is unique
	if aerr := p.checkDealPropUnique(deal); aerr != nil {
		return aerr
	}

	// Check that the deal uuid is unique
	if aerr := p.checkDealUuidUnique(deal); aerr != nil {
		return aerr
	}

	// we still need to call runDealFilters() even when external deal filter is not set
	if aerr := p.runDealFilters(deal); aerr != nil {
		return aerr
	}

	cleanup := func() {
		collat, pub, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
		if errf != nil && !errors.Is(errf, db.ErrNotFound) {
			p.dealLogger.LogError(deal.DealUuid, "failed to untag funds during deal cleanup", errf)
		} else if errf == nil {
			p.dealLogger.Infow(deal.DealUuid, "untagged funds for deal cleanup", "untagged publish", pub, "untagged collateral", collat,
				"err", errf)
		}

		errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
		if errs != nil && !errors.Is(errs, db.ErrNotFound) {
			p.dealLogger.LogError(deal.DealUuid, "failed to untag storage during deal cleanup", errs)
		} else if errs == nil {
			p.dealLogger.Infow(deal.DealUuid, "untagged storage for deal cleanup", deal.Transfer.Size)
		}

		if deal.InboundFilePath != "" {
			_ = os.Remove(deal.InboundFilePath)
		}
	}

	// tag the funds required for escrow and sending the publish deal message
	// so that they are not used for other deals
	trsp, err := p.fundManager.TagFunds(p.ctx, deal.DealUuid, deal.ClientDealProposal.Proposal)
	if err != nil {
		cleanup()

		err = fmt.Errorf("failed to tag funds for deal: %w", err)
		aerr := &acceptError{
			error:         err,
			reason:        "server error: tag funds",
			isSevereError: true,
		}
		if errors.Is(err, fundmanager.ErrInsufficientFunds) {
			aerr.reason = "server error: provider has insufficient funds to accept deal"
			aerr.isSevereError = false
		}
		return aerr
	}
	p.logFunds(deal.DealUuid, trsp)

	// tag the storage required for the deal in the staging area
	err = p.storageManager.Tag(p.ctx, deal.DealUuid, deal.Transfer.Size, host)
	if err != nil {
		cleanup()

		err = fmt.Errorf("failed to tag storage for deal: %w", err)
		aerr := &acceptError{
			error:         err,
			reason:        "server error: tag storage",
			isSevereError: true,
		}
		if errors.Is(err, storagemanager.ErrNoSpaceLeft) {
			aerr.reason = "server error: provider has no space left for storage deals"
			aerr.isSevereError = false
		}
		return aerr
	}

	// create a file in the staging area to which we will download the deal data
	downloadFilePath, err := p.storageManager.DownloadFilePath(deal.DealUuid)
	if err != nil {
		cleanup()

		return &acceptError{
			error:         fmt.Errorf("failed to create download staging file for deal: %w", err),
			reason:        "server error: creating download staging file",
			isSevereError: true,
		}
	}
	deal.InboundFilePath = downloadFilePath
	p.dealLogger.Infow(deal.DealUuid, "created deal download staging file", "path", deal.InboundFilePath)

	// write deal state to the database
	deal.CreatedAt = time.Now()
	deal.Checkpoint = dealcheckpoints.Accepted
	deal.CheckpointAt = time.Now()
	err = p.dealsDB.Insert(p.ctx, deal)
	if err != nil {
		cleanup()

		return &acceptError{
			error:         fmt.Errorf("failed to insert deal in db: %w", err),
			reason:        "server error: save to db",
			isSevereError: true,
		}
	}

	p.dealLogger.Infow(deal.DealUuid, "inserted deal into deals DB")

	return nil
}

// processOfflineDealProposal just saves the deal to the database after running deal filters
// Execution resumes when processImportOfflineDealData is called.
func (p *Provider) processOfflineDealProposal(ds *types.ProviderDealState, dh *dealHandler) *acceptError {
	// Check that the deal proposal is unique
	if aerr := p.checkDealPropUnique(ds); aerr != nil {
		return aerr
	}

	// Check that the deal uuid is unique
	if aerr := p.checkDealUuidUnique(ds); aerr != nil {
		return aerr
	}

	// we still need to call runDealFilters() even when external deal filter is not set
	if aerr := p.runDealFilters(ds); aerr != nil {
		return aerr
	}

	// Save deal to DB
	ds.CreatedAt = time.Now()
	ds.Checkpoint = dealcheckpoints.Accepted
	ds.CheckpointAt = time.Now()
	if err := p.dealsDB.Insert(p.ctx, ds); err != nil {
		return &acceptError{
			error:         fmt.Errorf("failed to insert deal in db: %w", err),
			reason:        "server error: save to db",
			isSevereError: true,
		}
	}

	// publish "new deal" event
	p.fireEventDealNew(ds)
	// publish an event with the current state of the deal
	p.fireEventDealUpdate(dh.Publisher, ds)

	return nil
}

func (p *Provider) processDeal(deal *types.ProviderDealState, rsp chan acceptDealResp) {
	var err *acceptError
	if !deal.IsOffline {
		err = p.processOnlineDealProposal(deal)
	} else {
		dh, herr := p.mkAndInsertDealHandler(deal.DealUuid)
		if herr == nil {
			err = p.processOfflineDealProposal(deal, dh)
		} else {
			err.error = herr
			err.isSevereError = true
			err.reason = "server error: creating deal thread"
		}
	}
	select {
	case p.processedDealChan <- processedDealReq{deal: deal, err: err, rsp: rsp}:
	case <-p.ctx.Done():
	}
}

func (p *Provider) processImportOfflineDealData(deal *types.ProviderDealState) *acceptError {
	cleanup := func() {
		collat, pub, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
		if errf != nil && !errors.Is(errf, db.ErrNotFound) {
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

		err = fmt.Errorf("failed to tag funds for deal: %w", err)
		aerr := &acceptError{
			error:         err,
			reason:        "server error: tag funds",
			isSevereError: true,
		}
		if errors.Is(err, fundmanager.ErrInsufficientFunds) {
			aerr.reason = "server error: provider has insufficient funds to accept deal"
			aerr.isSevereError = false
		}
		return aerr
	}
	p.logFunds(deal.DealUuid, trsp)
	return nil
}

func (p *Provider) checkDealPropUnique(deal *types.ProviderDealState) *acceptError {
	signedPropCid, err := deal.SignedProposalCid()
	if err != nil {
		return &acceptError{
			error:         fmt.Errorf("getting signed deal proposal cid: %w", err),
			reason:        "server error: signed proposal cid",
			isSevereError: true,
		}
	}

	dl, err := p.dealsDB.BySignedProposalCID(p.ctx, signedPropCid)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// If there was no deal in the DB with this signed proposal cid,
			// then it's unique
			return nil
		}
		return &acceptError{
			error:         fmt.Errorf("looking up deal by signed deal proposal cid: %w", err),
			reason:        "server error: lookup by proposal cid",
			isSevereError: true,
		}
	}

	// The database lookup did not return a "not found" error, meaning we found
	// a deal with a matching deal proposal cid. Therefore the deal proposal
	// is not unique.
	err = fmt.Errorf("deal proposal is identical to deal %s (proposed at %s)", dl.DealUuid, dl.CreatedAt)
	return &acceptError{
		error:         err,
		reason:        err.Error(),
		isSevereError: false,
	}
}

func (p *Provider) checkDealUuidUnique(deal *types.ProviderDealState) *acceptError {
	dl, err := p.dealsDB.ByID(p.ctx, deal.DealUuid)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// If there was no deal in the DB with this uuid, then it's unique
			return nil
		}
		return &acceptError{
			error:         fmt.Errorf("looking up deal by uuid: %w", err),
			reason:        "server error: unique check: lookup by deal uuid",
			isSevereError: true,
		}
	}

	// The database lookup did not return a "not found" error, meaning we found
	// a deal with a matching deal uuid. Therefore the deal proposal is not unique.
	err = fmt.Errorf("deal has the same uuid as deal %s (proposed at %s)", dl.DealUuid, dl.CreatedAt)
	return &acceptError{
		error:         err,
		reason:        err.Error(),
		isSevereError: false,
	}
}

// The provider run loop effectively implements a lock over resources used by
// the provider, like funds and storage space, so that only one deal at a
// time can change the value of these resources.
func (p *Provider) run() {
	log.Info("provider run loop: start")
	p.runWG.Add(1)
	defer func() {
		p.runWG.Done()
		log.Info("provider run loop: complete")
	}()

	for {
		select {
		// Process a request to
		// - accept a deal proposal and execute it immediately
		// - accept an offline deal proposal and save it for execution later
		//   when the data is imported
		// - accept a request to import data for an offline deal
		case dealReq := <-p.acceptDealChan:
			deal := dealReq.deal
			p.dealLogger.Infow(deal.DealUuid, "processing deal acceptance request")

			if deal.IsOffline && dealReq.isImport {
				// The Storage Provider is importing offline deal data, so tag
				// funds for the deal and execute it
				aerr := p.processImportOfflineDealData(dealReq.deal)
				if aerr != nil {
					p.sendErrorResp(aerr, dealReq.rsp, deal.DealUuid)
					continue
				}

				p.setupHandlerAndStartDeal(deal, dealReq.rsp)

				// send an accept response
				dealReq.rsp <- acceptDealResp{ri: &api.ProviderDealRejectionInfo{Accepted: true}}
				continue
			}

			// Send new online and offline deals for processing.
			// When the client proposes an offline deal, save the deal
			// to the database but don't execute the deal. The deal
			// will be executed when the Storage Provider imports the
			// deal data.
			go p.processDeal(deal, dealReq.rsp)

		case storageSpaceDealReq := <-p.storageSpaceChan:
			deal := storageSpaceDealReq.deal
			if err := p.storageManager.Untag(p.ctx, deal.DealUuid); err != nil && !errors.Is(err, db.ErrNotFound) {
				p.dealLogger.LogError(deal.DealUuid, "failed to untag storage space", err)
			} else {
				p.dealLogger.Infow(deal.DealUuid, "untagged storage space")
			}
			close(storageSpaceDealReq.done)

		case publishedDeal := <-p.publishedDealChan:
			deal := publishedDeal.deal
			_, _, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
			if errf != nil && !errors.Is(errf, db.ErrNotFound) {
				publishedDeal.done <- errf
			} else {
				publishedDeal.done <- nil
			}

		case retryDealReq := <-p.updateRetryStateChan:
			err := func() error {
				// Get deal by uuid from the database
				deal, err := p.dealsDB.ByID(p.ctx, retryDealReq.dealUuid)
				if err != nil {
					return fmt.Errorf("getting deal from db by id: %w", err)
				}

				if deal.Checkpoint == dealcheckpoints.Complete {
					return errors.New("deal is already complete")
				}

				// Set up deal handler so that clients can subscribe to deal update events
				dh, err := p.mkAndInsertDealHandler(deal.DealUuid)
				if err != nil {
					return err
				}

				// If the user wants to retry the deal
				if retryDealReq.retry {
					// Start executing the deal
					started, err := p.startDealThread(dh, deal)
					if err != nil {
						return fmt.Errorf("starting deal thread: %w", err)
					}
					if started {
						// If the deal wasn't already running, log a message saying
						// that it was restarted
						p.dealLogger.Infow(deal.DealUuid, "user initiated deal retry", "checkpoint", deal.Checkpoint.String())
					} else {
						// the deal was already running - log a message saying so
						p.dealLogger.Infow(deal.DealUuid, "user initiated deal retry but deal is already running", "checkpoint", deal.Checkpoint.String())
					}

					return nil
				}

				// The user wants to fail the deal
				return p.failPausedDeal(dh, deal)
			}()
			retryDealReq.done <- err

		case finishedDeal := <-p.finishedDealChan:
			deal := finishedDeal.deal
			p.dealLogger.Infow(deal.DealUuid, "deal finished")
			collat, pub, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
			if errf != nil && !errors.Is(errf, db.ErrNotFound) {
				p.dealLogger.LogError(deal.DealUuid, "failed to untag funds", errf)
			} else if errf == nil {
				p.dealLogger.Infow(deal.DealUuid, "untagged funds for deal as deal finished", "untagged publish", pub, "untagged collateral", collat,
					"err", errf)
			}

			errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
			if errs != nil && !errors.Is(errs, db.ErrNotFound) {
				p.dealLogger.LogError(deal.DealUuid, "failed to untag storage", errs)
			} else if errs == nil {
				p.dealLogger.Infow(deal.DealUuid, "untagged storage space for deal")
			}
			finishedDeal.done <- struct{}{}

		case processedDeal := <-p.processedDealChan:
			deal := processedDeal.deal
			if processedDeal.err != nil {
				// Reject offline deal
				if deal.IsOffline {
					dh, err := p.mkAndInsertDealHandler(deal.DealUuid)
					if err != nil {
						p.sendErrorResp(&acceptError{error: err, isSevereError: true, reason: "server error: getting deal thread"}, processedDeal.rsp, deal.DealUuid)
						continue
					}
					dh.close()
					p.delDealHandler(deal.DealUuid)
					p.sendErrorResp(processedDeal.err, processedDeal.rsp, deal.DealUuid)
					continue
				}
				// Reject online deal
				p.sendErrorResp(processedDeal.err, processedDeal.rsp, deal.DealUuid)
				continue
			}

			// Accept offline deal
			if deal.IsOffline {
				// The deal proposal was successful. Send an Accept response to the client.
				processedDeal.rsp <- acceptDealResp{ri: &api.ProviderDealRejectionInfo{Accepted: true}}
				// Don't execute the deal now, wait for data import.
				continue
			}

			p.setupHandlerAndStartDeal(deal, processedDeal.rsp)

			// send an accept response
			processedDeal.rsp <- acceptDealResp{ri: &api.ProviderDealRejectionInfo{Accepted: true}}

		case <-p.ctx.Done():
			return
		}
	}
}

// startDealThread sets up a deal handler and wait group monitoring for a deal, then
// executes the deal in a new go routine
func (p *Provider) startDealThread(dh *dealHandler, deal *types.ProviderDealState) (bool, error) {
	// Check if the deal is already running
	if ok := dh.setRunning(true); !ok {
		return false, nil
	}

	p.runWG.Add(1)
	go func() {
		defer p.runWG.Done()
		defer func() {
			dh.setRunning(false)
		}()

		// Run deal
		p.runDeal(deal, dh)
		p.dealLogger.Infow(deal.DealUuid, "deal go-routine finished execution")
	}()

	return true, nil
}

// failPausedDeal moves a deal from the paused to the failed state and cleans
// up the deal
func (p *Provider) failPausedDeal(dh *dealHandler, deal *types.ProviderDealState) error {
	// Check if the deal is running
	if dh.isRunning() {
		return fmt.Errorf("the deal %s is running; cannot fail running deal", deal.DealUuid)
	}

	// Update state in DB with error
	deal.Checkpoint = dealcheckpoints.Complete
	deal.Retry = types.DealRetryFatal
	var err error
	if deal.Err == "" {
		err = errors.New("user manually terminated the deal")
	} else {
		err = errors.New(deal.Err)
	}
	deal.Err = "user manually terminated the deal"
	p.dealLogger.LogError(deal.DealUuid, deal.Err, err)
	p.saveDealToDB(dh.Publisher, deal)

	// Call cleanupDeal in a go-routine because it sends a message to the provider
	// run loop (and failPausedDeal is called from the same run loop so otherwise
	// it will deadlock)
	go p.cleanupDeal(deal)

	return nil
}

func (p *Provider) sendErrorResp(aerr *acceptError, resp chan acceptDealResp, dealId uuid.UUID) {
	// If the error is a severe error (eg can't connect to database)
	if aerr.isSevereError {
		// Send a rejection message to the client with a reason for rejection
		rsp := acceptDealResp{ri: &api.ProviderDealRejectionInfo{Accepted: false, Reason: aerr.reason}}
		// Log an error with more details for the provider
		p.dealLogger.LogError(dealId, "error while processing deal acceptance request", aerr)
		resp <- rsp
		return
	}

	// The error is not a severe error, so don't log an error, just
	// send a message to the client with a rejection reason
	p.dealLogger.Infow(dealId, "deal acceptance request rejected", "reason", aerr.reason, "error", aerr.error)
	resp <- acceptDealResp{ri: &api.ProviderDealRejectionInfo{Accepted: false, Reason: aerr.reason}, err: nil}
}

func (p *Provider) setupHandlerAndStartDeal(deal *types.ProviderDealState, rsp chan acceptDealResp) {
	// Handle online accepted deal
	// set up deal handler so that clients can subscribe to deal update events
	dh, err := p.mkAndInsertDealHandler(deal.DealUuid)
	if err != nil {
		p.sendErrorResp(&acceptError{error: err, isSevereError: true, reason: "server error: setting up deal handler"}, rsp, deal.DealUuid)
		return
	}

	// start executing the deal
	_, err = p.startDealThread(dh, deal)
	if err != nil {
		p.sendErrorResp(&acceptError{error: err, isSevereError: true, reason: "server error: starting deal thread"}, rsp, deal.DealUuid)
	}
}
