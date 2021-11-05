package storagemarket

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/filecoin-project/boost/db"

	"golang.org/x/xerrors"

	"github.com/libp2p/go-eventbus"

	"github.com/filecoin-project/boost/storagemarket/datatransfer"

	"github.com/libp2p/go-libp2p-core/event"

	"github.com/filecoin-project/go-padreader"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	"github.com/filecoin-project/go-commp-utils/writer"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"

	"github.com/filecoin-project/boost/storagemarket/types"
)

var ErrDealExecNotFound = xerrors.Errorf("deal exec not found")

// dealExec keeps track of the deal's status while it's executing
type dealExec struct {
	ctx     context.Context
	stop    context.CancelFunc
	stopped chan struct{}

	prov           *Provider
	stateUpdatePS  event.Bus
	stateUpdatePub event.Emitter

	lk          sync.RWMutex
	deal        *types.ProviderDealState
	transferred uint64
}

func (p *Provider) newDealExec(deal *types.ProviderDealState) (*dealExec, error) {
	bus := eventbus.NewBus()
	emitter, err := bus.Emitter(&types.ProviderDealInfo{}, eventbus.Stateful)
	if err != nil {
		return nil, fmt.Errorf("failed to create event emitter: %w", err)
	}

	ctx, stop := context.WithCancel(p.ctx)
	de := &dealExec{
		ctx:     ctx,
		stop:    stop,
		stopped: make(chan struct{}),

		prov:           p,
		stateUpdatePS:  bus,
		stateUpdatePub: emitter,
		deal:           deal,
	}

	p.dealExecs.track(de)

	return de, nil
}

func (d *dealExec) onTransferred(transferred uint64) {
	d.lk.Lock()
	d.transferred = transferred
	d.lk.Unlock()

	d.fireEventDealUpdate()
}

func (d *dealExec) doDeal() {
	defer close(d.stopped)

	// publish "new deal" event
	d.fireEventDealNew()

	// publish an event with the current state of the deal
	d.fireEventDealUpdate()

	d.addDealLog("Deal Accepted")

	// Transfer Data
	if d.deal.Checkpoint < dealcheckpoints.Transferred {
		if err := d.transferAndVerify(d.onTransferred); err != nil {
			d.failDeal(fmt.Errorf("failed data transfer: %w", err))
			return
		}

		d.updateCheckpoint(dealcheckpoints.Transferred)
	}

	// Publish
	if d.deal.Checkpoint <= dealcheckpoints.Published {
		if err := d.publishDeal(); err != nil {
			d.failDeal(fmt.Errorf("failed to publish deal: %w", err))
			return
		}

		d.updateCheckpoint(dealcheckpoints.Published)
	}

	// AddPiece
	if d.deal.Checkpoint < dealcheckpoints.AddedPiece {
		if err := d.addPiece(); err != nil {
			d.failDeal(fmt.Errorf("failed to add piece: %w", err))
			return
		}

		d.updateCheckpoint(dealcheckpoints.AddedPiece)
	}

	// Watch deal on chain and change state in DB and emit notifications.

	// TODO: Clean up deal when it completes
	//d.cleanupDeal()
}

func (d *dealExec) transferAndVerify(onTransferred func(uint64)) error {
	log.Infow("transferring deal data", "id", d.deal.DealUuid)
	d.addDealLog("Start data transfer")

	tctx, cancel := context.WithDeadline(d.ctx, time.Now().Add(d.prov.config.MaxTransferDuration))
	defer cancel()

	// TODO: need to ensure that passing the padded piece size here makes
	// sense to the transport layer which will receive the raw unpadded bytes
	execParams := datatransfer.ExecuteParams{
		TransferType:   d.deal.TransferType,
		TransferParams: d.deal.TransferParams,
		DealUuid:       d.deal.DealUuid,
		FilePath:       d.deal.InboundFilePath,
		Size:           uint64(d.deal.ClientDealProposal.Proposal.PieceSize),
		OnTransferred:  onTransferred,
	}
	err := d.prov.Transport.Execute(tctx, execParams)
	if err != nil {
		return fmt.Errorf("failed data transfer: %w", err)
	}

	d.addDealLog("Data transfer complete")

	// if dtEvent.Type == Completed || Cancelled || Error {
	// move ahead, fail deal etc.
	// }

	if err := tctx.Err(); err != nil {
		return fmt.Errorf("data transfer timed out: %w", err)
	}

	// Verify CommP matches
	pieceCid, err := d.generatePieceCommitment()
	if err != nil {
		return fmt.Errorf("failed to generate CommP: %w", err)
	}

	clientPieceCid := d.deal.ClientDealProposal.Proposal.PieceCID
	if pieceCid != clientPieceCid {
		return fmt.Errorf("commP mismatch, expected=%s, actual=%s", clientPieceCid, pieceCid)
	}

	d.addDealLog("Data verification successful")

	return nil
}

// GeneratePieceCommitment generates the pieceCid for the CARv1 deal payload in
// the CARv2 file that already exists at the given path.
func (d *dealExec) generatePieceCommitment() (c cid.Cid, finalErr error) {
	rd, err := carv2.OpenReader(d.deal.InboundFilePath)
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

	dealSize := d.deal.ClientDealProposal.Proposal.PieceSize
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

func (d *dealExec) publishDeal() error {
	d.addDealLog("Publishing deal")

	//if ds.Checkpoint < dealcheckpoints.Published {
	//mcid, err := p.fullnodeApi.PublishDeals(p.ctx, *ds)
	//if err != nil {
	//return fmt.Errorf("failed to publish deal: %w", err)
	//}

	//ds.PublishCid = mcid
	//ds.Checkpoint = dealcheckpoints.Published
	//if err := p.dbApi.CreateOrUpdateDeal(ds); err != nil {
	//return fmt.Errorf("failed to update deal: %w", err)
	//}
	//}

	//res, err := p.lotusNode.WaitForPublishDeals(p.ctx, ds.PublishCid, ds.ClientDealProposal.Proposal)
	//if err != nil {
	//return fmt.Errorf("wait for publish failed: %w", err)
	//}

	//ds.PublishCid = res.FinalCid
	//ds.DealID = res.DealID
	//ds.Checkpoint = dealcheckpoints.PublishConfirmed
	//if err := p.dbApi.CreateOrUpdateDeal(ds); err != nil {
	//return fmt.Errorf("failed to update deal: %w", err)
	//}

	// TODO Release funds ? How does that work ?

	select {
	case <-d.ctx.Done():
	case <-time.After(10 * time.Second):
	}

	d.addDealLog("Deal published successfully")
	return nil
}

// HandoffDeal hands off a published deal for sealing and commitment in a sector
func (d *dealExec) addPiece() error {
	d.addDealLog("Hand off deal to sealer")

	v2r, err := carv2.OpenReader(d.deal.InboundFilePath)
	if err != nil {
		return fmt.Errorf("failed to open CARv2 file: %w", err)
	}

	// Hand the deal off to the process that adds it to a sector
	paddedReader, err := padreader.NewInflator(v2r.DataReader(), v2r.Header.DataSize, d.deal.ClientDealProposal.Proposal.PieceSize.Unpadded())
	if err != nil {
		return fmt.Errorf("failed to create inflator: %w", err)
	}

	_ = paddedReader

	//packingInfo, packingErr := p.fullnodeApi.OnDealComplete(
	//p.ctx,
	//*ds,
	//ds.ClientDealProposal.Proposal.PieceSize.Unpadded(),
	//paddedReader,
	//)

	// Close the reader as we're done reading from it.
	//if err := v2r.Close(); err != nil {
	//return fmt.Errorf("failed to close CARv2 reader: %w", err)
	//}

	//if packingErr != nil {
	//return fmt.Errorf("packing piece %s: %w", ds.ClientDealProposal.Proposal.PieceCID, packingErr)
	//}

	//ds.SectorID = packingInfo.SectorNumber
	//ds.Offset = packingInfo.Offset
	//ds.Length = packingInfo.Size
	//ds.Checkpoint = dealcheckpoints.AddedPiece
	//if err := p.dbApi.CreateOrUpdateDeal(ds); err != nil {
	//return fmt.Errorf("failed to update deal: %w", err)
	//}

	//// Register the deal data as a "shard" with the DAG store. Later it can be
	//// fetched from the DAG store during retrieval.
	//if err := stores.RegisterShardSync(p.ctx, p.dagStore, ds.ClientDealProposal.Proposal.PieceCID, ds.InboundCARPath, true); err != nil {
	//err = fmt.Errorf("failed to activate shard: %w", err)
	//log.Error(err)
	//}

	d.addDealLog("Deal handed off to sealer successfully")

	return nil
}

func (d *dealExec) failDeal(err error) {
	d.cleanupDeal()

	// Update state in DB with error
	d.deal.Checkpoint = dealcheckpoints.Complete
	if xerrors.Is(err, context.Canceled) {
		d.deal.Err = "Cancelled"
		d.addDealLog("Deal cancelled")
	} else {
		d.deal.Err = err.Error()
		d.addDealLog("Deal failed: %s", d.deal.Err)
	}
	dberr := d.prov.db.Update(d.prov.ctx, d.deal)
	if dberr != nil {
		log.Errorw("updating failed deal in db", "id", d.deal.DealUuid, "err", err)
	}

	// Fire deal update event
	d.fireEventDealUpdate()

	select {
	case d.prov.failedDealsChan <- failedDealReq{d.deal, err}:
	case <-d.prov.ctx.Done():
	}
}

func (d *dealExec) cleanupDeal() {
	_ = os.Remove(d.deal.InboundFilePath)
	// ...
	//cleanup resources here

	d.prov.dealExecs.del(d.deal.DealUuid)
}

func (d *dealExec) fireEventDealNew() {
	evt := types.ProviderDealInfo{Deal: d.deal}
	if err := d.prov.newDealPS.NewDeals.Emit(evt); err != nil {
		log.Warn("publishing new deal event", "id", d.deal.DealUuid, "err", err)
	}
}

func (d *dealExec) fireEventDealUpdate() {
	d.lk.RLock()
	deal := *d.deal
	evt := types.ProviderDealInfo{
		Deal:        &deal,
		Transferred: d.transferred,
	}
	d.lk.RUnlock()

	if err := d.stateUpdatePub.Emit(evt); err != nil {
		log.Warn("publishing deal state update", "id", deal.DealUuid, "err", err)
	}
}

func (d *dealExec) subscribeUpdates() (event.Subscription, error) {
	sub, err := d.stateUpdatePS.Subscribe(new(types.ProviderDealInfo), eventbus.BufSize(256))
	if err != nil {
		return nil, fmt.Errorf("failed to create deal update subscriber to %s: %w", d.deal.DealUuid, err)
	}
	return sub, nil
}

func (d *dealExec) updateCheckpoint(ckpt dealcheckpoints.Checkpoint) {
	d.deal.Checkpoint = ckpt
	if err := d.prov.db.Update(d.ctx, d.deal); err != nil {
		d.failDeal(fmt.Errorf("failed to persist deal state: %w", err))
		return
	}

	d.fireEventDealUpdate()
}

func (d *dealExec) addDealLog(format string, args ...interface{}) {
	l := &db.DealLog{
		DealUuid:  d.deal.DealUuid,
		Text:      fmt.Sprintf(format, args...),
		CreatedAt: time.Now(),
	}
	if err := d.prov.db.InsertLog(d.prov.ctx, l); err != nil {
		log.Warnw("failed to persist deal log: %s", err)
	}
}

func (d *dealExec) cancel(ctx context.Context) {
	d.stop()
	select {
	case <-ctx.Done():
		return
	case <-d.stopped:
		return
	}
}
