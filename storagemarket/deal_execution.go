package storagemarket

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemarket/datatransfer"
	"github.com/filecoin-project/boost/storagemarket/types"
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

func (p *Provider) doDeal(deal *types.ProviderDealState) {
	// Set up pubsub for deal updates
	bus := eventbus.NewBus()
	pub, err := bus.Emitter(&types.ProviderDealInfo{}, eventbus.Stateful)
	if err != nil {
		err := fmt.Errorf("failed to create event emitter: %w", err)
		p.failDeal(pub, deal, err)
		return
	}

	// Create a context that can be cancelled for this deal if the user wants
	// to cancel the deal early
	ctx, stop := context.WithCancel(p.ctx)
	defer stop()

	stopped := make(chan struct{})
	defer close(stopped)

	// Keep track of the fields to subscribe to or cancel the deal
	p.dealHandlers.track(&dealHandler{
		dealUuid: deal.DealUuid,
		ctx:      ctx,
		stop:     stop,
		stopped:  stopped,
		bus:      bus,
	})

	// Execute the deal synchronously
	if err := p.execDeal(ctx, pub, deal); err != nil {
		p.failDeal(pub, deal, err)
	}
}

func (p *Provider) execDeal(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState) error {
	// publish "new deal" event
	p.fireEventDealNew(deal)

	// publish an event with the current state of the deal
	p.fireEventDealUpdate(pub, deal, 0)

	p.addDealLog(deal.DealUuid, "Deal Accepted")

	// Transfer Data
	if deal.Checkpoint < dealcheckpoints.Transferred {
		if err := p.transferAndVerify(ctx, pub, deal); err != nil {
			return fmt.Errorf("failed data transfer: %w", err)
		}
	}

	// Publish
	if deal.Checkpoint <= dealcheckpoints.Published {
		if err := p.publishDeal(ctx, pub, deal); err != nil {
			return fmt.Errorf("failed to publish deal: %w", err)
		}
	}

	// AddPiece
	if deal.Checkpoint < dealcheckpoints.AddedPiece {
		if err := p.addPiece(ctx, pub, deal); err != nil {
			return fmt.Errorf("failed to add piece: %w", err)
		}
	}

	// Watch deal on chain and change state in DB and emit notifications.

	// TODO: Clean up deal when it completes
	//d.cleanupDeal()
	return nil
}

func (p *Provider) transferAndVerify(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState) error {
	log.Infow("transferring deal data", "id", deal.DealUuid)
	p.addDealLog(deal.DealUuid, "Start data transfer")

	tctx, cancel := context.WithDeadline(ctx, time.Now().Add(p.config.MaxTransferDuration))
	defer cancel()

	// TODO: need to ensure that passing the padded piece size here makes
	// sense to the transport layer which will receive the raw unpadded bytes
	execParams := datatransfer.ExecuteParams{
		TransferType:   deal.TransferType,
		TransferParams: deal.TransferParams,
		DealUuid:       deal.DealUuid,
		FilePath:       deal.InboundFilePath,
		Size:           uint64(deal.ClientDealProposal.Proposal.PieceSize),
	}
	sub, err := p.Transport.Execute(tctx, execParams)
	if err != nil {
		return fmt.Errorf("failed data transfer: %w", err)
	}

	// Fire deal update events as the deal progresses
	for transferred := range sub {
		p.fireEventDealUpdate(pub, deal, transferred)
	}

	p.addDealLog(deal.DealUuid, "Data transfer complete")

	// if dtEvent.Type == Completed || Cancelled || Error {
	// move ahead, fail deal etc.
	// }

	if err := tctx.Err(); err != nil {
		return fmt.Errorf("data transfer ctx errored: %w", err)
	}

	// Verify CommP matches
	pieceCid, err := p.generatePieceCommitment(deal)
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

// GeneratePieceCommitment generates the pieceCid for the CARv1 deal payload in
// the CARv2 file that already exists at the given path.
func (p *Provider) generatePieceCommitment(deal *types.ProviderDealState) (c cid.Cid, finalErr error) {
	rd, err := carv2.OpenReader(deal.InboundFilePath)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to get CARv2 reader: %w", err)
	}

	defer func() {
		if err := rd.Close(); err != nil {

			if finalErr == nil {
				c = cid.Undef
				finalErr = fmt.Errorf("failed to close CARv2 reader: %w", err)
				return
			}
		}
	}()

	// dump the CARv1 payload of the CARv2 file to the Commp Writer and get back the CommP.
	w := &writer.Writer{}
	//written, err := io.Copy(w, rd.DataReader())
	_, err = io.Copy(w, rd.DataReader())
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to write to CommP writer: %w", err)
	}

	// TODO: figure out why the CARv1 payload size is always 0
	//if written != int64(rd.Header.DataSize) {
	//	return cid.Undef, fmt.Errorf("number of bytes written to CommP writer %d not equal to the CARv1 payload size %d", written, rd.Header.DataSize)
	//}

	cidAndSize, err := w.Sum()
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to get CommP: %w", err)
	}

	dealSize := deal.ClientDealProposal.Proposal.PieceSize
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

		mcid, err := p.dealPublisher.Publish(p.ctx, deal.ClientDealProposal)
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
	res, err := p.adapter.WaitForPublishDeals(p.ctx, *deal.PublishCID, deal.ClientDealProposal.Proposal)
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

	// TODO:
	// Now that the deal has been published, mark the funds that were tagged
	// for the deal as being locked

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

	// Inflate the deal size so that it exactly fills a piece
	proposal := deal.ClientDealProposal.Proposal
	paddedReader, err := padreader.NewInflator(v2r.DataReader(), v2r.Header.DataSize, proposal.PieceSize.Unpadded())
	if err != nil {
		return fmt.Errorf("failed to create inflator: %w", err)
	}

	// Add the piece to a sector
	packingInfo, packingErr := p.adapter.AddPieceToSector(p.ctx, *deal, paddedReader)

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
	p.cleanupDeal(deal)

	// Update state in DB with error
	deal.Checkpoint = dealcheckpoints.Complete
	if xerrors.Is(err, context.Canceled) {
		deal.Err = "Cancelled"
		p.addDealLog(deal.DealUuid, "Deal cancelled")
	} else {
		deal.Err = err.Error()
		p.addDealLog(deal.DealUuid, "Deal failed: %s", deal.Err)
	}
	dberr := p.db.Update(p.ctx, deal)
	if dberr != nil {
		log.Errorw("updating failed deal in db", "id", deal.DealUuid, "err", err)
	}

	// Fire deal update event
	if pub != nil {
		p.fireEventDealUpdate(pub, deal, p.Transport.Transferred(deal.DealUuid))
	}

	select {
	case p.failedDealsChan <- failedDealReq{deal, err}:
	case <-p.ctx.Done():
	}
}

func (p *Provider) cleanupDeal(deal *types.ProviderDealState) {
	_ = os.Remove(deal.InboundFilePath)
	// ...
	//cleanup resources here

	p.dealHandlers.del(deal.DealUuid)
}

func (p *Provider) fireEventDealNew(deal *types.ProviderDealState) {
	evt := types.ProviderDealInfo{Deal: deal}
	if err := p.newDealPS.NewDeals.Emit(evt); err != nil {
		log.Warn("publishing new deal event", "id", deal.DealUuid, "err", err)
	}
}

func (p *Provider) fireEventDealUpdate(pub event.Emitter, deal *types.ProviderDealState, transferred uint64) {
	evt := types.ProviderDealInfo{
		Deal:        deal,
		Transferred: transferred,
	}

	if err := pub.Emit(evt); err != nil {
		log.Warn("publishing deal state update", "id", deal.DealUuid, "err", err)
	}
}

func (p *Provider) updateCheckpoint(ctx context.Context, pub event.Emitter, deal *types.ProviderDealState, ckpt dealcheckpoints.Checkpoint) error {
	deal.Checkpoint = ckpt
	if err := p.db.Update(ctx, deal); err != nil {
		return fmt.Errorf("failed to persist deal state: %w", err)
	}

	p.fireEventDealUpdate(pub, deal, p.Transport.Transferred(deal.DealUuid))
	return nil
}

func (p *Provider) addDealLog(dealUuid uuid.UUID, format string, args ...interface{}) {
	l := &db.DealLog{
		DealUuid:  dealUuid,
		Text:      fmt.Sprintf(format, args...),
		CreatedAt: time.Now(),
	}
	if err := p.db.InsertLog(p.ctx, l); err != nil {
		log.Warnw("failed to persist deal log: %s", err)
	}
}
