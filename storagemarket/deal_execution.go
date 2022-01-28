package storagemarket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/filecoin-project/boost/transport"
	"github.com/filecoin-project/go-state-types/abi"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemarket/types"
	transporttypes "github.com/filecoin-project/boost/transport/types"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-commp-utils/writer"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/filecoin-project/go-padreader"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
)

const (
	// DealCancelled means that a deal has been cancelled by the caller
	DealCancelled = "Cancelled"
)

type dealMakingError struct {
	recoverable bool
	err         error
	uiMsg       string
}

func (p *Provider) doDeal(deal *types.ProviderDealState, dh *dealHandler) {
	// Check if deal is already complete
	if deal.Checkpoint >= dealcheckpoints.AddedPiece {
		return
	}

	// Set up pubsub for deal updates
	pub, err := dh.bus.Emitter(&types.ProviderDealState{}, eventbus.Stateful)
	if err != nil {
		err = fmt.Errorf("failed to create event emitter: %w", err)
		p.cleanupDeal(deal)
		p.failDeal(pub, deal, err)
		return
	}

	// build in-memory state
	fi, err := os.Stat(deal.InboundFilePath)
	if err != nil {
		err := fmt.Errorf("failed to stat output file: %w", err)
		p.cleanupDeal(deal)
		p.failDeal(pub, deal, err)
		return
	}
	deal.NBytesReceived = fi.Size()

	// Execute the deal synchronously
	if derr := p.execDealUptoAddPiece(dh.providerCtx, pub, deal, dh); derr != nil {
		// If the error is NOT recoverable, fail the deal and cleanup state.
		if !derr.recoverable {
			p.cleanupDeal(deal)
			p.failDeal(pub, deal, derr.err)
		} else {
			// TODO For now, we will get recoverable errors only when the process is gracefully shutdown and
			// the provider context gets cancelled.
			// However, down the road, other stages of deal making will start returning
			// recoverable errors as well and we will have to build the DB/UX/resumption support for it.

			// if the error is recoverable, persist that fact to the deal log and return
			p.addDealLog(deal.DealUuid, "Deal Paused because of recoverable error: %s", derr.err)
		}
		return
	}

	// deal has been sent for sealing -> we can cleanup the deal state now and simply watch the deal on chain
	// to wait for deal completion/slashing and update the state in DB accordingly.
	p.cleanupDeal(deal)

	// TODO
	// Watch deal on chain and change state in DB and emit notifications.
	// Given that cleanup deal above also gets rid of the deal handler, subscriptions to deal updates from here on
	// will fail, we can look into it when we implement deal completion.
}

func (p *Provider) execDealUptoAddPiece(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState, dh *dealHandler) *dealMakingError {
	// publish "new deal" event
	p.fireEventDealNew(deal)
	// publish an event with the current state of the deal
	p.fireEventDealUpdate(pub, deal)

	p.addDealLog(deal.DealUuid, "Deal Execution Started")

	// Transfer Data
	if deal.Checkpoint < dealcheckpoints.Transferred {
		if err := p.transferAndVerify(dh.transferCtx, pub, deal); err != nil {
			dh.transferCancelled(nil)
			// if the transfer failed because of context cancellation and the context was not
			// cancelled because of the user explicitly cancelling the transfer, this is a recoverable error.
			if xerrors.Is(err, context.Canceled) && !dh.TransferCancelledByUser() {
				return &dealMakingError{
					recoverable: true,
					err:         fmt.Errorf("data transfer failed with a recoverable error after %d bytes with error: %w", deal.NBytesReceived, err),
					uiMsg:       fmt.Sprintf("data transfer paused after transferring %d bytes because Boost is shutting down", deal.NBytesReceived),
				}
			}
			return &dealMakingError{
				err: fmt.Errorf("execDeal failed data transfer: %w", err),
			}
		}
	}
	// transfer can no longer be cancelled
	dh.transferCancelled(errors.New("transfer already complete"))

	// Publish
	if deal.Checkpoint <= dealcheckpoints.Published {
		if err := p.publishDeal(ctx, pub, deal); err != nil {
			if xerrors.Is(err, context.Canceled) {
				return &dealMakingError{
					recoverable: true,
					err:         fmt.Errorf("deal publish failed with a recoverable error: %w", err),
					uiMsg:       "deal was paused in the publish state because Boost was shut down",
				}
			}

			return &dealMakingError{
				err: fmt.Errorf("failed to publish deal: %w", err),
			}
		}
	}
	// Now that the deal has been published, we no longer need to have funds
	// tagged as being for this deal (the publish message moves collateral
	// from the storage market actor escrow balance to the locked balance)
	if err := p.untagFundsAfterPublish(ctx, deal); err != nil {
		if xerrors.Is(err, context.Canceled) {
			return &dealMakingError{recoverable: true,
				err:   fmt.Errorf("deal failed with recoverbale error while untagging funds after publish: %w", err),
				uiMsg: "the deal was paused in the Publishing state because Boost was shut down"}
		}

		return &dealMakingError{
			err: fmt.Errorf("failed to untag funds after sending publish message: %w", err),
		}
	}

	// AddPiece
	if deal.Checkpoint < dealcheckpoints.AddedPiece {
		if err := p.addPiece(ctx, pub, deal); err != nil {
			if xerrors.Is(err, context.Canceled) {
				return &dealMakingError{
					recoverable: true,
					err:         fmt.Errorf("add piece failed with a recoverable error: %w", err),
					uiMsg:       "deal was paused while being sent to the sealing subsystem because Boost was shut down",
				}
			}

			return &dealMakingError{
				err: fmt.Errorf("failed to add piece: %w", err),
			}
		}
	}

	return nil
}

func (p *Provider) untagFundsAfterPublish(ctx context.Context, deal *types.ProviderDealState) error {
	presp := make(chan struct{}, 1)
	select {
	case p.publishedDealChan <- publishDealReq{deal: deal, done: presp}:
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

func (p *Provider) transferAndVerify(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState) error {
	log.Infow("transferring deal data", "id", deal.DealUuid)
	p.addDealLog(deal.DealUuid, "Start data transfer")

	tctx, cancel := context.WithDeadline(ctx, time.Now().Add(p.config.MaxTransferDuration))
	defer cancel()

	handler, err := p.Transport.Execute(tctx, deal.Transfer.Params, &transporttypes.TransportDealInfo{
		OutputFile: deal.InboundFilePath,
		DealUuid:   deal.DealUuid,
		DealSize:   int64(deal.Transfer.Size),
	})
	if err != nil {
		return fmt.Errorf("transferAndVerify failed data transfer: %w", err)
	}

	// wait for data-transfer to finish
	if err := p.waitForTransferFinish(tctx, handler, pub, deal); err != nil {
		return fmt.Errorf("data-transfer failed: %w", err)
	}

	p.addDealLog(deal.DealUuid, "Data transfer complete")

	// Verify CommP matches
	pieceCid, err := GeneratePieceCommitment(deal.InboundFilePath, deal.ClientDealProposal.Proposal.PieceSize)
	if err != nil {
		return fmt.Errorf("failed to generate CommP: %w", err)
	}

	clientPieceCid := deal.ClientDealProposal.Proposal.PieceCID
	if pieceCid != clientPieceCid {
		return fmt.Errorf("commP mismatch, expected=%s, actual=%s", clientPieceCid, pieceCid)
	}

	p.addDealLog(deal.DealUuid, "Data verification successful")

	return p.updateCheckpoint(ctx, pub, deal, dealcheckpoints.Transferred)
}

func (p *Provider) waitForTransferFinish(ctx context.Context, handler transport.Handler, pub event.Emitter, deal *types.ProviderDealState) error {
	defer handler.Close()
	defer p.transfers.complete(deal.DealUuid)

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
			p.fireEventDealUpdate(pub, deal)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// GenerateCommP
func GenerateCommP(filepath string) (cidAndSize *writer.DataCIDSize, finalErr error) {
	rd, err := carv2.OpenReader(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to get CARv2 reader: %w", err)
	}

	defer func() {
		if err := rd.Close(); err != nil {
			if finalErr == nil {
				cidAndSize = nil
				finalErr = fmt.Errorf("failed to close CARv2 reader: %w", err)
				return
			}
		}
	}()

	// dump the CARv1 payload of the CARv2 file to the Commp Writer and get back the CommP.
	w := &writer.Writer{}
	written, err := io.Copy(w, rd.DataReader())
	if err != nil {
		return nil, fmt.Errorf("failed to write to CommP writer: %w", err)
	}

	var size int64
	switch rd.Version {
	case 2:
		size = int64(rd.Header.DataSize)
	case 1:
		st, err := os.Stat(filepath)
		if err != nil {
			return nil, fmt.Errorf("failed to stat CARv1 file: %w", err)
		}
		size = st.Size()
	}

	if written != size {
		return nil, fmt.Errorf("number of bytes written to CommP writer %d not equal to the CARv1 payload size %d", written, rd.Header.DataSize)
	}

	cidAndSize = &writer.DataCIDSize{}
	*cidAndSize, err = w.Sum()
	if err != nil {
		return nil, fmt.Errorf("failed to get CommP: %w", err)
	}

	return cidAndSize, nil
}

// GeneratePieceCommitment generates the pieceCid for the CARv1 deal payload in
// the CARv2 file that already exists at the given path.
func GeneratePieceCommitment(filepath string, dealSize abi.PaddedPieceSize) (c cid.Cid, finalErr error) {
	cidAndSize, err := GenerateCommP(filepath)
	if err != nil {
		return cid.Undef, err
	}

	if cidAndSize.PieceSize < dealSize {
		// need to pad up!
		rawPaddedCommp, err := commp.PadCommP(
			// we know how long a pieceCid "hash" is, just blindly extract the trailing 32 bytes
			cidAndSize.PieceCID.Hash()[len(cidAndSize.PieceCID.Hash())-32:],
			uint64(cidAndSize.PieceSize),
			uint64(dealSize),
		)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to pad data: %w", err)
		}
		cidAndSize.PieceCID, _ = commcid.DataCommitmentV1ToCID(rawPaddedCommp)
	}

	return cidAndSize.PieceCID, err
}

func (p *Provider) publishDeal(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState) error {
	// Publish the deal on chain. At this point collateral and payment for the
	// deal are locked and can no longer be withdrawn. Payment is transferred
	// to the provider's wallet at each epoch.
	if deal.Checkpoint < dealcheckpoints.Published {
		p.addDealLog(deal.DealUuid, "Publishing deal")

		mcid, err := p.dealPublisher.Publish(p.ctx, deal.DealUuid, deal.ClientDealProposal)
		if err != nil {
			return fmt.Errorf("failed to publish deal %s: %w", deal.DealUuid, err)
		}

		deal.PublishCID = &mcid
		if err := p.updateCheckpoint(ctx, pub, deal, dealcheckpoints.Published); err != nil {
			return err
		}
	}

	// Wait for the publish deals message to land on chain.
	// Note that multiple deals may be published in a batch, so the message CID
	// may be for a batch of deals.
	p.addDealLog(deal.DealUuid, "Awaiting deal publish confirmation")
	res, err := p.chainDealManager.WaitForPublishDeals(p.ctx, *deal.PublishCID, deal.ClientDealProposal.Proposal)
	if err != nil {
		return fmt.Errorf("wait for publish message %s failed: %w", deal.PublishCID, err)
	}

	// If there's a re-org, the publish deal CID may change, so use the
	// final CID.
	deal.PublishCID = &res.FinalCid
	deal.ChainDealID = res.DealID
	if err := p.updateCheckpoint(ctx, pub, deal, dealcheckpoints.PublishConfirmed); err != nil {
		return err
	}

	p.addDealLog(deal.DealUuid, "Deal published successfully")

	return nil
}

// addPiece hands off a published deal for sealing and commitment in a sector
func (p *Provider) addPiece(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState) error {
	p.addDealLog(deal.DealUuid, "Hand off deal to sealer")

	// Open a reader against the CAR file with the deal data
	v2r, err := carv2.OpenReader(deal.InboundFilePath)
	if err != nil {
		return fmt.Errorf("failed to open CARv2 file: %w", err)
	}

	var size uint64
	switch v2r.Version {
	case 1:
		st, err := os.Stat(deal.InboundFilePath)
		if err != nil {
			return fmt.Errorf("failed to stat CARv1 file: %w", err)
		}
		size = uint64(st.Size())
	case 2:
		size = v2r.Header.DataSize
	}

	// Inflate the deal size so that it exactly fills a piece
	proposal := deal.ClientDealProposal.Proposal
	paddedReader, err := padreader.NewInflator(v2r.DataReader(), size, proposal.PieceSize.Unpadded())
	if err != nil {
		return fmt.Errorf("failed to create inflator: %w", err)
	}

	// Add the piece to a sector
	packingInfo, packingErr := p.AddPieceToSector(p.ctx, *deal, paddedReader)

	// Close the reader as we're done reading from it.
	if err := v2r.Close(); err != nil {
		return fmt.Errorf("failed to close CARv2 reader: %w", err)
	}

	if packingErr != nil {
		return fmt.Errorf("packing piece %s: %w", proposal.PieceCID, packingErr)
	}

	deal.SectorID = packingInfo.SectorNumber
	deal.Offset = packingInfo.Offset
	deal.Length = packingInfo.Size

	// TODO:
	//// Register the deal data as a "shard" with the DAG store. Later it can be
	//// fetched from the DAG store during retrieval.
	//if err := stores.RegisterShardSync(p.ctx, p.dagStore, ds.ClientDealProposal.Proposal.PieceCID, ds.InboundCARPath, true); err != nil {
	//err = fmt.Errorf("failed to activate shard: %w", err)
	//log.Error(err)
	//}

	p.addDealLog(deal.DealUuid, "Deal handed off to sealer successfully")

	return p.updateCheckpoint(ctx, pub, deal, dealcheckpoints.AddedPiece)
}

func (p *Provider) failDeal(pub event.Emitter, deal *types.ProviderDealState, err error) {
	log.Errorw("deal failed", "id", deal.DealUuid, "err", err)
	// Update state in DB with error
	deal.Checkpoint = dealcheckpoints.Complete
	if xerrors.Is(err, context.Canceled) {
		deal.Err = DealCancelled
		p.addDealLog(deal.DealUuid, "Deal cancelled")
	} else {
		deal.Err = err.Error()
		p.addDealLog(deal.DealUuid, "Deal failed: %s", deal.Err)
	}
	dberr := p.dealsDB.Update(p.ctx, deal)
	if dberr != nil {
		log.Errorw("updating failed deal in db", "id", deal.DealUuid, "err", dberr)
	}

	// Fire deal update event
	if pub != nil {
		p.fireEventDealUpdate(pub, deal)
	}
}

func (p *Provider) cleanupDeal(deal *types.ProviderDealState) {
	// remove the temp file created for inbound deal data
	_ = os.Remove(deal.InboundFilePath)

	// close and clean up the deal handler
	dh := p.getDealHandler(deal.DealUuid)
	if dh != nil {
		dh.transferCancelled(errors.New("deal cleaned up"))
		dh.close()
		p.delDealHandler(deal.DealUuid)
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

func (p *Provider) fireEventDealNew(deal *types.ProviderDealState) {
	if err := p.newDealPS.NewDeals.Emit(*deal); err != nil {
		log.Warn("publishing new deal event", "id", deal.DealUuid, "err", err)
	}
}

func (p *Provider) fireEventDealUpdate(pub event.Emitter, deal *types.ProviderDealState) {
	if err := pub.Emit(*deal); err != nil {
		log.Warn("publishing deal state update", "id", deal.DealUuid, "err", err)
	}
}

func (p *Provider) updateCheckpoint(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState, ckpt dealcheckpoints.Checkpoint) error {
	deal.Checkpoint = ckpt
	if err := p.dealsDB.Update(ctx, deal); err != nil {
		return fmt.Errorf("failed to persist deal state: %w", err)
	}

	p.fireEventDealUpdate(pub, deal)
	return nil
}

func (p *Provider) addDealLog(dealUuid uuid.UUID, format string, args ...interface{}) {
	l := &db.DealLog{
		DealUUID:  dealUuid,
		Text:      fmt.Sprintf(format, args...),
		CreatedAt: time.Now(),
	}
	if err := p.dealsDB.InsertLog(p.ctx, l); err != nil {
		log.Warnw("failed to persist deal log", "id", dealUuid, "err", err)
	}
}
