package storagemarket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"time"

	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/transport"
	transporttypes "github.com/filecoin-project/boost/transport/types"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	acrypto "github.com/filecoin-project/go-state-types/crypto"
	lapi "github.com/filecoin-project/lotus/api"
	types2 "github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	"github.com/google/uuid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/libp2p/go-libp2p/core/event"
)

const (
	// DealCancelled means that a deal has been cancelled by the caller
	DealCancelled = "Cancelled"
)

type dealMakingError struct {
	error
	retry types.DealRetryType
}

func (p *Provider) runDeal(deal *types.ProviderDealState, dh *dealHandler) {
	// Log out the deal state at the start of execution (to help with debugging)
	dcpy := *deal
	dcpy.Transfer.Params = nil
	dcpy.ClientDealProposal.ClientSignature = acrypto.Signature{}
	p.dealLogger.Infow(deal.DealUuid, "deal execution initiated", "deal state", dcpy)

	// Clear any error from a previous run
	if deal.Err != "" || deal.Retry == types.DealRetryAuto {
		deal.Err = ""
		deal.Retry = types.DealRetryAuto
		p.saveDealToDB(dh.Publisher, deal)
	}

	// Execute the deal
	err := p.execDeal(deal, dh)
	if err == nil {
		// Deal completed successfully
		return
	}

	// If the error is fatal, fail the deal and cleanup resources (delete
	// downloaded data, untag funds etc)
	if err.retry == types.DealRetryFatal {
		cancelled := dh.TransferCancelledByUser()
		p.failDeal(dh.Publisher, deal, err, cancelled)
		return
	}

	// The error is recoverable, so just add a line to the deal log and
	// wait for the deal to be executed again (either manually by the user
	// or automatically when boost restarts)
	if errors.Is(err.error, context.Canceled) {
		p.dealLogger.Infow(deal.DealUuid, "deal paused because boost was shut down",
			"checkpoint", deal.Checkpoint.String())
	} else {
		p.dealLogger.Infow(deal.DealUuid, "deal paused because of recoverable error", "err", err.Error(),
			"checkpoint", deal.Checkpoint.String(), "retry", err.retry)
	}

	deal.Retry = err.retry
	deal.Err = err.Error()
	p.saveDealToDB(dh.Publisher, deal)
}

func (p *Provider) execDeal(deal *types.ProviderDealState, dh *dealHandler) (dmerr *dealMakingError) {
	// Capture any panic as a manually retryable error
	defer func() {
		if err := recover(); err != nil {
			log.Errorw("caught panic executing deal", "id", deal.DealUuid, "err", err)
			_, _ = fmt.Fprint(os.Stderr, string(debug.Stack()))
			dmerr = &dealMakingError{
				error: fmt.Errorf("caught panic in deal execution: %s\n%s", err, debug.Stack()),
				retry: types.DealRetryManual,
			}
		}
	}()

	// If the deal has not yet been handed off to the sealer
	if deal.Checkpoint < dealcheckpoints.AddedPiece {
		transferType := "downloaded file"
		if deal.IsOffline {
			transferType = "imported offline deal file"
		}

		// Read the bytes received from the downloaded / imported file
		fi, err := os.Stat(deal.InboundFilePath)
		if err != nil {
			return &dealMakingError{
				error: fmt.Errorf("failed to get size of %s '%s': %w", transferType, deal.InboundFilePath, err),
				retry: types.DealRetryFatal,
			}
		}
		deal.NBytesReceived = fi.Size()
		p.dealLogger.Infow(deal.DealUuid, "size of "+transferType, "filepath", deal.InboundFilePath, "size", fi.Size())
	} else if !deal.IsOffline {
		// For online deals where the deal has already been handed to the sealer,
		// the inbound file could already have been removed and in that case,
		// the number of bytes received should be the same as deal size as
		// we've already verified the transfer.
		deal.NBytesReceived = int64(deal.Transfer.Size)
	}

	// Execute the deal synchronously
	if derr := p.execDealUptoAddPiece(dh.providerCtx, deal, dh); derr != nil {
		return derr
	}

	// deal has been sent for sealing -> we can cleanup the deal state now and simply watch the deal on chain
	// to wait for deal completion/slashing and update the state in DB accordingly.
	p.cleanupDeal(deal)
	p.dealLogger.Infow(deal.DealUuid, "finished deal cleanup after successful execution")

	// Watch the sealing status of the deal and fire events for each change
	p.dealLogger.Infow(deal.DealUuid, "watching deal sealing state changes")
	if derr := p.fireSealingUpdateEvents(dh, deal.DealUuid, deal.SectorID); derr != nil {
		return derr
	}
	p.cleanupDealHandler(deal.DealUuid)
	p.dealLogger.Infow(deal.DealUuid, "deal sealing reached termination state")

	// TODO
	// Watch deal on chain and change state in DB and emit notifications.
	return nil
}

func (p *Provider) execDealUptoAddPiece(ctx context.Context, deal *types.ProviderDealState, dh *dealHandler) *dealMakingError {
	pub := dh.Publisher
	// publish "new deal" event
	p.fireEventDealNew(deal)
	// publish an event with the current state of the deal
	p.fireEventDealUpdate(pub, deal)

	p.dealLogger.Infow(deal.DealUuid, "deal execution in progress")

	// Transfer Data step will be executed only if it's NOT an offline deal
	if !deal.IsOffline {
		if deal.Checkpoint < dealcheckpoints.Transferred {
			// Check that the deal's start epoch hasn't already elapsed
			if derr := p.checkDealProposalStartEpoch(deal); derr != nil {
				return derr
			}

			if err := p.transferAndVerify(dh, pub, deal); err != nil {
				// The transfer has failed. If the user tries to cancel the
				// transfer after this point it's a no-op.
				dh.setCancelTransferResponse(nil)

				return err
			}

			p.dealLogger.Infow(deal.DealUuid, "deal data transfer finished successfully")
		} else {
			p.dealLogger.Infow(deal.DealUuid, "deal data transfer has already been completed")
		}
		// transfer can no longer be cancelled
		dh.setCancelTransferResponse(errors.New("transfer already complete"))
		p.dealLogger.Infow(deal.DealUuid, "deal data-transfer can no longer be cancelled")
	} else if deal.Checkpoint < dealcheckpoints.Transferred {
		// verify CommP matches for an offline deal
		if err := p.verifyCommP(deal); err != nil {
			err.error = fmt.Errorf("error when matching commP for imported data for offline deal: %w", err)
			return err
		}
		p.dealLogger.Infow(deal.DealUuid, "commp matched successfully for imported data for offline deal")

		// update checkpoint
		if derr := p.updateCheckpoint(pub, deal, dealcheckpoints.Transferred); derr != nil {
			return derr
		}
	}

	// Publish
	if deal.Checkpoint <= dealcheckpoints.Published {
		if err := p.publishDeal(ctx, pub, deal); err != nil {
			return err
		}
		p.dealLogger.Infow(deal.DealUuid, "deal successfully published and confirmed-publish")
	} else {
		p.dealLogger.Infow(deal.DealUuid, "deal has already been published and confirmed-publish")
	}

	// Now that the deal has been published, we no longer need to have funds
	// tagged as being for this deal (the publish message moves collateral
	// from the storage market actor escrow balance to the locked balance)
	if err := p.untagFundsAfterPublish(ctx, deal); err != nil {
		// If there's an error untagging funds we should still try to continue,
		// so just log the error
		p.dealLogger.Warnw(deal.DealUuid, "failed to untag funds after sending publish message", "err", err)
	} else {
		p.dealLogger.Infow(deal.DealUuid, "funds successfully untagged for deal after publish")
	}

	// AddPiece
	if deal.Checkpoint < dealcheckpoints.AddedPiece {
		if err := p.addPiece(ctx, pub, deal); err != nil {
			err.error = fmt.Errorf("failed to add piece: %w", err.error)
			return err
		}
		p.dealLogger.Infow(deal.DealUuid, "deal successfully handed over to the sealing subsystem")
	} else {
		p.dealLogger.Infow(deal.DealUuid, "deal has already been handed over to the sealing subsystem")
	}

	// as deal has already been handed to the sealer, we can remove the inbound file and reclaim the tagged space
	if deal.CleanupData {
		_ = os.Remove(deal.InboundFilePath)
		p.dealLogger.Infow(deal.DealUuid, "removed piece data from disk as deal has been added to a sector", "path", deal.InboundFilePath)
	}
	if err := p.untagStorageSpaceAfterSealing(ctx, deal); err != nil {
		// If there's an error untagging storage space we should still try to continue,
		// so just log the error
		p.dealLogger.Warnw(deal.DealUuid, "failed to untag storage space after handing deal to sealer", "err", err)
	} else {
		p.dealLogger.Infow(deal.DealUuid, "storage space successfully untagged for deal after it was handed to sealer")
	}

	// Index and Announce deal
	if deal.Checkpoint < dealcheckpoints.IndexedAndAnnounced {
		if err := p.indexAndAnnounce(ctx, pub, deal); err != nil {
			err.error = fmt.Errorf("failed to add index and announce deal: %w", err.error)
			return err
		}
		if deal.AnnounceToIPNI {
			p.dealLogger.Infow(deal.DealUuid, "deal successfully indexed and announced")
		} else {
			p.dealLogger.Infow(deal.DealUuid, "deal successfully indexed")
		}
	} else {
		p.dealLogger.Infow(deal.DealUuid, "deal has already been indexed and announced")
	}

	return nil
}

func (p *Provider) untagStorageSpaceAfterSealing(ctx context.Context, deal *types.ProviderDealState) error {
	presp := make(chan struct{}, 1)
	select {
	case p.storageSpaceChan <- storageSpaceDealReq{deal: deal, done: presp}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-presp:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (p *Provider) untagFundsAfterPublish(ctx context.Context, deal *types.ProviderDealState) error {
	presp := make(chan error, 1)
	select {
	case p.publishedDealChan <- publishDealReq{deal: deal, done: presp}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-presp:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Provider) transferAndVerify(dh *dealHandler, pub event.Emitter, deal *types.ProviderDealState) *dealMakingError {
	// Use a context specifically for transfers, that can be cancelled by the user
	ctx := dh.transferCtx

	p.dealLogger.Infow(deal.DealUuid, "deal queued for transfer", "transfer client id", deal.Transfer.ClientID)

	// Wait for a spot in the transfer queue
	err := p.xferLimiter.waitInQueue(ctx, deal)
	if err != nil {
		// If the transfer failed because the user cancelled the
		// transfer, it's non-recoverable
		if dh.TransferCancelledByUser() {
			return &dealMakingError{
				retry: types.DealRetryFatal,
				error: fmt.Errorf("data transfer manually cancelled by user: %w", err),
			}
		}

		// If boost was shutdown while waiting for the transfer to start,
		// automatically retry on restart.
		if errors.Is(err, context.Canceled) {
			return &dealMakingError{
				retry: types.DealRetryAuto,
				error: fmt.Errorf("boost shutdown while waiting to start transfer for deal %s: %w", deal.DealUuid, err),
			}
		}
		return &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("queued transfer failed to start for deal %s: %w", deal.DealUuid, err),
		}
	}
	defer p.xferLimiter.complete(deal.DealUuid)

	p.dealLogger.Infow(deal.DealUuid, "start deal data transfer", "transfer client id", deal.Transfer.ClientID)
	transferStart := time.Now()
	tctx, cancel := context.WithDeadline(ctx, transferStart.Add(p.config.MaxTransferDuration))
	defer cancel()

	st := time.Now()
	handler, err := p.Transport.Execute(tctx, deal.Transfer.Params, &transporttypes.TransportDealInfo{
		OutputFile: deal.InboundFilePath,
		DealUuid:   deal.DealUuid,
		DealSize:   int64(deal.Transfer.Size),
	})
	if err != nil {
		return &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("transferAndVerify failed to start data transfer: %w", err),
		}
	}

	// wait for data-transfer to finish
	if err := p.waitForTransferFinish(tctx, handler, pub, deal); err != nil {
		// If the transfer failed because the user cancelled the
		// transfer, it's non-recoverable
		if dh.TransferCancelledByUser() {
			return &dealMakingError{
				retry: types.DealRetryFatal,
				error: fmt.Errorf("data transfer manually cancelled by user after %d bytes: %w", deal.NBytesReceived, err),
			}
		}

		// If the transfer failed because boost was shut down, it's
		// automatically recoverable
		if errors.Is(err, context.Canceled) && time.Since(transferStart) < p.config.MaxTransferDuration {
			return &dealMakingError{
				retry: types.DealRetryAuto,
				error: fmt.Errorf("data transfer paused by boost shutdown after %d bytes: %w", deal.NBytesReceived, err),
			}
		}

		// Note that the data transfer has automatic retries built in, so if
		// it fails, it means it's already retried several times and we should
		// fail the deal
		return &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("data-transfer failed: %w", err),
		}
	}

	// Make room in the transfer queue for the next transfer
	p.xferLimiter.complete(deal.DealUuid)

	p.dealLogger.Infow(deal.DealUuid, "deal data-transfer completed successfully", "bytes received", deal.NBytesReceived, "time taken",
		time.Since(st).String())

	// Verify CommP matches
	if err := p.verifyCommP(deal); err != nil {
		err.error = fmt.Errorf("failed to verify CommP: %w", err.error)
		return err
	}

	p.dealLogger.Infow(deal.DealUuid, "commP matched successfully: deal-data verified")
	return p.updateCheckpoint(pub, deal, dealcheckpoints.Transferred)
}

const OneGib = 1024 * 1024 * 1024

func (p *Provider) waitForTransferFinish(ctx context.Context, handler transport.Handler, pub event.Emitter, deal *types.ProviderDealState) error {
	defer handler.Close()
	defer p.transfers.complete(deal.DealUuid)

	// log transfer progress to the deal log every 10% or every GiB
	var lastOutput int64
	logTransferProgress := func(received int64) {
		if deal.Transfer.Size > 0 {
			pct := (100 * received) / int64(deal.Transfer.Size)
			outputPct := pct / 10
			if outputPct != lastOutput {
				lastOutput = outputPct
				p.dealLogger.Infow(deal.DealUuid, "transfer progress", "bytes received", received,
					"deal size", deal.Transfer.Size, "percent complete", pct)
			}
		} else {
			gib := received / OneGib
			if gib != lastOutput {
				lastOutput = gib
				p.dealLogger.Infow(deal.DealUuid, "transfer progress", "bytes received", received)
			}
		}
	}

	for {
		select {
		case evt, ok := <-handler.Sub():
			if !ok {
				return nil
			}
			if evt.Error != nil {
				return evt.Error
			}
			deal.NBytesReceived = evt.NBytesReceived
			p.transfers.setBytes(deal.DealUuid, uint64(evt.NBytesReceived))
			p.xferLimiter.setBytes(deal.DealUuid, uint64(evt.NBytesReceived))
			p.fireEventDealUpdate(pub, deal)
			logTransferProgress(deal.NBytesReceived)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *Provider) publishDeal(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState) *dealMakingError {
	// Check that the deal's start epoch hasn't already elapsed
	if derr := p.checkDealProposalStartEpoch(deal); derr != nil {
		return derr
	}

	// Publish the deal on chain. At this point collateral and payment for the
	// deal are locked and can no longer be withdrawn. Payment is transferred
	// to the provider's wallet at each epoch.
	if deal.Checkpoint < dealcheckpoints.Published {
		p.dealLogger.Infow(deal.DealUuid, "sending deal to deal publisher")

		mcid, err := p.dealPublisher.Publish(p.ctx, deal.ClientDealProposal)
		if err != nil {
			// Check if the deal start epoch has expired
			if derr := p.checkDealProposalStartEpoch(deal); derr != nil {
				return derr
			}

			// If boost was shutdown while waiting for the deal to be
			// published, automatically retry on restart.
			// Note that deals are published in batches, and the batch is
			// kept in memory.
			if errors.Is(err, context.Canceled) {
				return &dealMakingError{
					retry: types.DealRetryAuto,
					error: fmt.Errorf("boost shutdown while waiting to publish deal %s: %w", deal.DealUuid, err),
				}
			}

			// Check if the client is an f4 address, ie an FVM contract
			clientAddr := deal.ClientDealProposal.Proposal.Client.String()
			isContractClient := len(clientAddr) >= 2 && (clientAddr[:2] == "t4" || clientAddr[:2] == "f4")

			if isContractClient {
				// For contract deal publish errors the deal fails fatally: we don't
				// want to retry because the deal is probably taken by another SP
				return &dealMakingError{
					retry: types.DealRetryFatal,
					error: fmt.Errorf("fatal error to publish deal %s: %w", deal.DealUuid, err),
				}
			}

			// For any other publish error the user must manually retry: we don't
			// want to automatically retry because deal publishing costs money
			return &dealMakingError{
				retry: types.DealRetryManual,
				error: fmt.Errorf("recoverable error to publish deal %s: %w", deal.DealUuid, err),
			}
		}

		deal.PublishCID = &mcid
		if derr := p.updateCheckpoint(pub, deal, dealcheckpoints.Published); derr != nil {
			return derr
		}
		p.dealLogger.Infow(deal.DealUuid, "deal published successfully, will await deal publish confirmation")
	} else {
		p.dealLogger.Infow(deal.DealUuid, "deal has already been published")
	}

	// Wait for the publish deals message to land on chain.
	// Note that multiple deals may be published in a batch, so the message CID
	// may be for a batch of deals.
	p.dealLogger.Infow(deal.DealUuid, "awaiting deal publish confirmation")
	res, err := p.chainDealManager.WaitForPublishDeals(p.ctx, *deal.PublishCID, deal.ClientDealProposal.Proposal)

	// The `WaitForPublishDeals` call above is a remote RPC call to the full node
	// and if it fails because of a context cancellation, the error we get back doesn't
	// unwrap into a context cancelled error because of how error handling is implemented in the RPC layer.
	// The below check is a work around for that.
	if err != nil && ctx.Err() != nil {
		p.dealLogger.Warnw(deal.DealUuid, "context timed out while waiting for publish confirmation")
		return &dealMakingError{
			retry: types.DealRetryAuto,
			error: fmt.Errorf("wait for publish confirmation did not complete: %w", ctx.Err()),
		}
	}

	if err != nil {
		p.dealLogger.LogError(deal.DealUuid, "error while waiting for publish confirm", err)
		return &dealMakingError{
			retry: types.DealRetryAuto,
			error: fmt.Errorf("wait for confirmation of publish message %s failed: %w", deal.PublishCID, err),
		}
	}
	p.dealLogger.Infow(deal.DealUuid, "deal publish confirmed")

	// If there's a re-org, the publish deal CID may change, so use the
	// final CID.
	deal.PublishCID = &res.FinalCid
	deal.ChainDealID = res.DealID
	if derr := p.updateCheckpoint(pub, deal, dealcheckpoints.PublishConfirmed); derr != nil {
		return derr
	}

	return nil
}

// addPiece hands off a published deal for sealing and commitment in a sector
func (p *Provider) addPiece(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState) *dealMakingError {
	// Check that the deal's start epoch hasn't already elapsed
	if derr := p.checkDealProposalStartEpoch(deal); derr != nil {
		return derr
	}

	p.dealLogger.Infow(deal.DealUuid, "add piece called")

	proposal := deal.ClientDealProposal.Proposal
	paddedReader, err := openReader(deal.InboundFilePath, proposal.PieceSize.Unpadded())
	if err != nil {
		return &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("failed to read piece data: %w", err),
		}
	}

	// Add the piece to a sector
	packingInfo, packingErr := p.AddPieceToSector(ctx, *deal, paddedReader)
	_ = paddedReader.Close()
	if packingErr != nil {
		if ctx.Err() != nil {
			p.dealLogger.Warnw(deal.DealUuid, "context timed out while trying to add piece")
		}

		return &dealMakingError{
			retry: types.DealRetryAuto,
			error: fmt.Errorf("packing piece %s: %w", proposal.PieceCID, packingErr),
		}
	}

	deal.SectorID = packingInfo.SectorNumber
	deal.Offset = packingInfo.Offset
	deal.Length = packingInfo.Size
	p.dealLogger.Infow(deal.DealUuid, "deal successfully handed to the sealing subsystem",
		"sectorNum", deal.SectorID.String(), "offset", deal.Offset, "length", deal.Length)

	if derr := p.updateCheckpoint(pub, deal, dealcheckpoints.AddedPiece); derr != nil {
		return derr
	}

	return nil
}

func openReader(filePath string, pieceSize abi.UnpaddedPieceSize) (io.ReadCloser, error) {
	st, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat %s: %w", filePath, err)
	}
	size := uint64(st.Size())

	// Open a reader against the CAR file with the deal data
	v2r, err := carv2.OpenReader(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CAR reader over %s: %w", filePath, err)
	}
	_ = v2r.Close()

	r, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", filePath, err)
	}

	reader, err := padreader.NewInflator(r, size, pieceSize)
	if err != nil {
		return nil, fmt.Errorf("failed to inflate data: %w", err)
	}

	return struct {
		io.Reader
		io.Closer
	}{
		Reader: reader,
		Closer: r,
	}, nil
}

func (p *Provider) indexAndAnnounce(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState) *dealMakingError {
	// If this is Curio sealer then we should wait till sector finishes sealing
	if p.config.Curio {
		// Wait for sector to finish sealing
		err := p.trackCurioSealing(deal.SectorID)
		if err != nil {
			return err
		}
	}

	// add deal to piece metadata store
	pc := deal.ClientDealProposal.Proposal.PieceCID
	p.dealLogger.Infow(deal.DealUuid, "about to add deal for piece in LID")
	if err := p.piecedirectory.AddDealForPiece(ctx, pc, model.DealInfo{
		DealUuid:     deal.DealUuid.String(),
		ChainDealID:  deal.ChainDealID,
		MinerAddr:    p.Address,
		SectorID:     deal.SectorID,
		PieceOffset:  deal.Offset,
		PieceLength:  deal.Length,
		CarLength:    uint64(deal.NBytesReceived),
		IsDirectDeal: false,
	}); err != nil {
		return &dealMakingError{
			retry: types.DealRetryAuto,
			error: fmt.Errorf("failed to add deal to piece metadata store: %w", err),
		}
	}
	p.dealLogger.Infow(deal.DealUuid, "deal successfully added to LID")

	// if the index provider is enabled
	if p.ip.Enabled() {
		if deal.AnnounceToIPNI {
			// announce to the network indexer but do not fail the deal if the announcement fails,
			// just retry the next time boost restarts
			annCid, err := p.ip.AnnounceBoostDeal(ctx, deal)
			if err != nil {
				return &dealMakingError{
					retry: types.DealRetryAuto,
					error: fmt.Errorf("failed to announce deal to network indexer: %w", err),
				}
			}
			p.dealLogger.Infow(deal.DealUuid, "announced deal to network indexer", "announcement-cid", annCid)
		} else {
			p.dealLogger.Infow(deal.DealUuid, "didn't announce deal as requested in the deal proposal")
		}
	} else {
		p.dealLogger.Infow(deal.DealUuid, "didn't announce deal because network indexer is disabled")
	}

	if derr := p.updateCheckpoint(pub, deal, dealcheckpoints.IndexedAndAnnounced); derr != nil {
		return derr
	}

	return nil
}

// fireSealingUpdateEvents periodically checks the sealing status of the deal
// and fires events for each change
func (p *Provider) fireSealingUpdateEvents(dh *dealHandler, dealUuid uuid.UUID, sectorNum abi.SectorNumber) *dealMakingError {
	var deal *types.ProviderDealState
	var lastSealingState lapi.SectorState
	checkStatus := func(force bool) lapi.SectorInfo {
		// To avoid overloading the sealing service, only get the sector status
		// if there's at least one subscriber to the event that will be published
		if !force && !dh.hasActiveSubscribers() {
			return lapi.SectorInfo{}
		}

		// Get the sector status
		si, err := p.sps.SectorsStatus(p.ctx, sectorNum, false)
		if err == nil && si.State != lastSealingState {
			lastSealingState = si.State

			// Sector status has changed, fire an update event
			deal, err = p.dealsDB.ByID(p.ctx, dealUuid)
			if err != nil {
				log.Errorf("getting deal %s with sealing update: %w", dealUuid, err)
				return si
			}

			p.dealLogger.Infow(dealUuid, "current sealing state", "state", si.State)
			p.fireEventDealUpdate(dh.Publisher, deal)
		}
		return si
	}

	retErr := &dealMakingError{
		retry: types.DealRetryFatal,
		error: ErrDealNotInSector,
	}

	// Check status immediately
	info := checkStatus(true)
	if IsFinalSealingState(info.State) {
		if p.config.Curio {
			md, err := p.fullnodeApi.StateMarketStorageDeal(p.ctx, deal.ChainDealID, types2.EmptyTSK)
			if err != nil {
				return &dealMakingError{
					retry: types.DealRetryAuto,
					error: fmt.Errorf("getting deal %s from chain: %w", dealUuid, err),
				}
			}
			if md != nil {
				if md.State.SectorStartEpoch > 0 {
					return nil
				}
				return retErr
			}
			return retErr
		}
		if HasDeal(info.Deals, deal.ChainDealID) {
			return nil
		}
		return retErr
	}

	// Check status every 10 second. There is no advantage of checking it every second
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	count := 0
	forceCount := 12
	for {
		select {
		case <-p.ctx.Done():
			return nil
		case <-ticker.C:
			count++
			// Force a status check every (ticker * forceCount) seconds, even if there
			// are no subscribers (so that we can stop checking altogether
			// if the sector reaches a final sealing state)
			info = checkStatus(count >= forceCount)
			if count >= forceCount {
				count = 0
			}

			if IsFinalSealingState(info.State) {
				if p.config.Curio {
					md, err := p.fullnodeApi.StateMarketStorageDeal(p.ctx, deal.ChainDealID, types2.EmptyTSK)
					if err != nil {
						return &dealMakingError{
							retry: types.DealRetryAuto,
							error: fmt.Errorf("getting deal %s from chain: %w", dealUuid, err),
						}
					}
					if md != nil {
						if md.State.SectorStartEpoch > 0 {
							return nil
						}
						return retErr
					}
					return retErr
				}
				if HasDeal(info.Deals, deal.ChainDealID) {
					return nil
				}
				return retErr
			}
		}
	}
}

func IsFinalSealingState(state lapi.SectorState) bool {
	switch sealing.SectorState(state) {
	case
		sealing.Proving,
		sealing.Available,
		sealing.UpdateActivating,
		sealing.ReleaseSectorKey,
		sealing.Removed,
		sealing.Removing,
		sealing.Terminating,
		sealing.TerminateWait,
		sealing.TerminateFinality,
		sealing.TerminateFailed,
		sealing.FailedUnrecoverable:
		return true
	}
	return false
}

func HasDeal(deals []abi.DealID, pdsDealId abi.DealID) bool {
	var ret bool
	for _, d := range deals {
		if d == pdsDealId {
			ret = true
			break
		}
	}
	return ret
}

func (p *Provider) failDeal(pub event.Emitter, deal *types.ProviderDealState, err error, cancelled bool) {
	// Update state in DB with error
	deal.Checkpoint = dealcheckpoints.Complete
	deal.Retry = types.DealRetryFatal
	if cancelled {
		deal.Err = DealCancelled
		p.dealLogger.Infow(deal.DealUuid, "deal cancelled by user")
	} else {
		deal.Err = err.Error()
		p.dealLogger.LogError(deal.DealUuid, "deal failed", err)
	}

	p.saveDealToDB(pub, deal)
	p.cleanupDeal(deal)
}

func (p *Provider) saveDealToDB(pub event.Emitter, deal *types.ProviderDealState) {
	// In the case that the provider has been shutdown, the provider's context
	// will be cancelled, so use a background context when saving state to the
	// DB to avoid this edge case.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dberr := p.dealsDB.Update(ctx, deal)
	if dberr != nil {
		p.dealLogger.LogError(deal.DealUuid, "failed to update deal state in DB", dberr)
	}

	// Fire deal update event
	if pub != nil {
		p.fireEventDealUpdate(pub, deal)
	}
}

func (p *Provider) cleanupDeal(deal *types.ProviderDealState) {
	p.dealLogger.Infow(deal.DealUuid, "cleaning up deal")
	defer p.dealLogger.Infow(deal.DealUuid, "finished cleaning up deal")

	// remove the temp file created for inbound deal data if it is not an offline deal
	if !deal.IsOffline {
		_ = os.Remove(deal.InboundFilePath)
	}

	if deal.Checkpoint == dealcheckpoints.Complete {
		p.cleanupDealHandler(deal.DealUuid)
	}

	done := make(chan struct{}, 1)
	// submit req to event loop to untag tagged funds and storage space
	select {
	case p.finishedDealChan <- finishedDealReq{deal: deal, done: done}:
	case <-p.ctx.Done():
	}

	// wait for event loop to finish cleanup and return before we return from here
	// so caller is guaranteed that all resources associated with the deal have been cleanedup before
	// taking further action.
	select {
	case <-done:
	case <-p.ctx.Done():
	}
}

// cleanupDealHandler closes and cleans up the deal handler
func (p *Provider) cleanupDealHandler(dealUuid uuid.UUID) {
	dh := p.getDealHandler(dealUuid)
	if dh == nil {
		return
	}

	dh.setCancelTransferResponse(errors.New("deal cleaned up"))
	dh.close()
	p.delDealHandler(dealUuid)
}

func (p *Provider) fireEventDealNew(deal *types.ProviderDealState) {
	if err := p.newDealPS.NewDeals.Emit(*deal); err != nil {
		p.dealLogger.Warnw(deal.DealUuid, "publishing new deal event", "err", err.Error())
	}
}

func (p *Provider) fireEventDealUpdate(pub event.Emitter, deal *types.ProviderDealState) {
	if err := pub.Emit(*deal); err != nil {
		p.dealLogger.Warnw(deal.DealUuid, "publishing deal state update", "err", err.Error())
	}
}

func (p *Provider) updateCheckpoint(pub event.Emitter, deal *types.ProviderDealState, ckpt dealcheckpoints.Checkpoint) *dealMakingError {
	prev := deal.Checkpoint
	deal.Checkpoint = ckpt
	deal.CheckpointAt = time.Now()
	// we don't want a graceful shutdown to mess with db updates so pass a background context
	if err := p.dealsDB.Update(context.Background(), deal); err != nil {
		return &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("failed to persist deal state: %w", err),
		}
	}
	p.dealLogger.Infow(deal.DealUuid, "updated deal checkpoint in DB", "old checkpoint", prev.String(), "new checkpoint", ckpt.String())
	p.fireEventDealUpdate(pub, deal)

	return nil
}

func (p *Provider) checkDealProposalStartEpoch(deal *types.ProviderDealState) *dealMakingError {
	chainHead, err := p.fullnodeApi.ChainHead(p.ctx)
	if err != nil {
		log.Warnw("failed to check deal proposal start epoch", "err", err)
		return nil
	}

	// A storage deal must appear in a sealed (proven) sector no later than
	// StartEpoch, otherwise it is invalid
	height := chainHead.Height()
	if height > deal.ClientDealProposal.Proposal.StartEpoch {
		return &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("deal proposal must be proven on chain by deal proposal "+
				"start epoch %d, but it has expired: current chain height: %d",
				deal.ClientDealProposal.Proposal.StartEpoch, height),
		}
	}

	return nil
}

func (p *Provider) trackCurioSealing(sectorNum abi.SectorNumber) *dealMakingError {
	var lastSealingState lapi.SectorState
	checkStatus := func() lapi.SectorInfo {
		// Get the sector status
		si, err := p.sps.SectorsStatus(p.ctx, sectorNum, false)
		if err == nil && si.State != lastSealingState {
			lastSealingState = si.State
		}
		return si
	}

	retErr := &dealMakingError{
		retry: types.DealRetryFatal,
		error: ErrSectorSealingFailed,
	}

	// Check status immediately
	info := checkStatus()
	if IsFinalSealingState(info.State) {
		return nil
	}

	// Check status every 10 second. There is no advantage of checking it every second
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return nil
		case <-ticker.C:
			info = checkStatus()
			if IsFinalSealingState(info.State) {
				if sealing.SectorState(info.State) == sealing.FailedUnrecoverable {
					return retErr
				}
				return nil
			}
		}
	}
}
