package storagemarket

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

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

func (p *Provider) failDeal(ds *types.ProviderDealState, err error) {
	p.cleanupDeal(ds)

	select {
	case p.failedDealsChan <- failedDealReq{ds, err}:
	case <-p.ctx.Done():
	}
}

func (p *Provider) cleanupDeal(ds *types.ProviderDealState) {
	_ = os.Remove(ds.InboundCARPath)
	// ...
	//cleanup resources here
}

func dealStateToEvent(ds *types.ProviderDealState) types.ProviderDealEvent {
	// TODO flush out this function based on UX needs !
	return types.ProviderDealEvent{}
}

func transferEventToProviderEvent(ds *types.ProviderDealState, evt types.DataTransferEvent) types.ProviderDealEvent {
	// TODO flush out based on UX needs
	return types.ProviderDealEvent{}
}

func (p *Provider) doDeal(ds *types.ProviderDealState, publisher event.Emitter) {
	// publish an event with the current state of the deal
	if err := publisher.Emit(dealStateToEvent(ds)); err != nil {
		panic(err)
		// log
	}

	// Transfer Data
	if ds.Checkpoint < dealcheckpoints.Transferred {
		if err := p.transferAndVerify(ds, publisher); err != nil {
			p.failDeal(ds, fmt.Errorf("failed data transfer: %w", err))
			return
		}
		if err := publisher.Emit(dealStateToEvent(ds)); err != nil {
			panic(err)
			// log
		}
	}

	// Publish
	if ds.Checkpoint <= dealcheckpoints.Published {
		if err := p.publishDeal(ds); err != nil {
			p.failDeal(ds, fmt.Errorf("failed to publish deal: %w", err))
			return
		}
		if err := publisher.Emit(dealStateToEvent(ds)); err != nil {
			panic(err)
			// log
		}
	}

	// AddPiece
	if ds.Checkpoint < dealcheckpoints.AddedPiece {
		if err := p.addPiece(ds); err != nil {
			p.failDeal(ds, fmt.Errorf("failed to add piece: %w", err))
			return
		}

		if err := publisher.Emit(dealStateToEvent(ds)); err != nil {
			panic(err)
			// log
		}
	}

	// Lie back, sip your cocktail and watch the deal as it goes through the cycle of life and eventually expires.
	// The deal will eventually wonder if it was all worth it, only to then realise that if it was able to make life
	// easy for even one Web2 user, it really was all worth it, for kindness keeps us warm in an otherwise cold and indifferent cosmos.
	// ...
	// ...
	// Watch deal on chain and change state in DB and emit notifications.
}

func (p *Provider) transferAndVerify(ds *types.ProviderDealState, publisher event.Emitter) error {
	// Transfer Data
	u, err := url.Parse(ds.TransferURL)
	if err != nil {
		return fmt.Errorf("failed to parse transfer URL: %w", err)
	}

	tctx, cancel := context.WithDeadline(p.ctx, time.Now().Add(p.config.MaxTransferDuration))
	defer cancel()
	// TODO Execute SHOULD respect the context here !
	// async call returns the subscription -> dosen't block
	// need to ensure that  that passing the padded piece size here makes sense to the transport layer which will receieve the raw unpadded bytes.
	transferSub, err := p.transport.Execute(tctx, u, ds.InboundCARPath, ds.ClientDealProposal.Proposal.PieceSize)
	if err != nil {
		return fmt.Errorf("failed data transfer: %w", err)
	}
	defer transferSub.Close()
	select {
	// similar to boost notifications, the transport layer too will first push the current state before resuming the transfer
	case evt := <-transferSub.Out():
		dtEvent := evt.(types.DataTransferEvent)
		if err := publisher.Emit(transferEventToProviderEvent(ds, dtEvent)); err != nil {
			panic(err)
			// log
		}
		// if dtEvent.Type == Completed || Cancelled || Error {
		// move ahead, fail deal etc.
		// }

	case <-tctx.Done():
		return fmt.Errorf("data transfer timed out: %w", tctx.Err())
	}

	// Verify CommP matches
	pieceCid, err := p.generatePieceCommitment(ds)
	if err != nil {
		return fmt.Errorf("failed to generate CommP: %w", err)
	}

	clientPieceCid := ds.ClientDealProposal.Proposal.PieceCID
	if pieceCid != clientPieceCid {
		return fmt.Errorf("commP mismatch, expected=%s, actual=%s", clientPieceCid, pieceCid)
	}

	// persist transferred checkpoint
	ds.Checkpoint = dealcheckpoints.Transferred
	//if err := p.dbApi.CreateOrUpdateDeal(ds); err != nil {
	//return fmt.Errorf("failed to persist deal state: %w", err)
	//}

	// TODO : Emit a notification here
	return nil
}

// GeneratePieceCommitment generates the pieceCid for the CARv1 deal payload in
// the CARv2 file that already exists at the given path.
func (p *Provider) generatePieceCommitment(ds *types.ProviderDealState) (c cid.Cid, finalErr error) {
	rd, err := carv2.OpenReader(ds.InboundCARPath)
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
	written, err := io.Copy(w, rd.DataReader())
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to write to CommP writer: %w", err)
	}

	if written != int64(rd.Header.DataSize) {
		return cid.Undef, fmt.Errorf("number of bytes written to CommP writer %d not equal to the CARv1 payload size %d", written, rd.Header.DataSize)
	}

	cidAndSize, err := w.Sum()
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to get CommP: %w", err)
	}

	dealSize := ds.ClientDealProposal.Proposal.PieceSize
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

func (p *Provider) publishDeal(ds *types.ProviderDealState) error {
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
	return nil
}

// HandoffDeal hands off a published deal for sealing and commitment in a sector
func (p *Provider) addPiece(ds *types.ProviderDealState) error {
	v2r, err := carv2.OpenReader(ds.InboundCARPath)
	if err != nil {
		return fmt.Errorf("failed to open CARv2 file: %w", err)
	}

	// Hand the deal off to the process that adds it to a sector
	paddedReader, err := padreader.NewInflator(v2r.DataReader(), v2r.Header.DataSize, ds.ClientDealProposal.Proposal.PieceSize.Unpadded())
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

	return nil
}
