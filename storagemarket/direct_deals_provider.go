package storagemarket

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/filecoin-project/go-state-types/builtin/v12/verifreg"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/types"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v12/miner"
	verifregst "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"

	"github.com/google/uuid"
)

//var log = logging.Logger("direct-deals-providers")

type DirectDealsProvider struct {
	ctx context.Context // context to be stopped when stopping boostd

	fullnodeApi v1api.FullNode
	pieceAdder  types.PieceAdder
	commpCalc   smtypes.CommpCalculator

	//db            *sql.DB
	directDealsDB *db.DirectDataDB
	//logsSqlDB     *sql.DB
	//logsDB        *db.LogsDB

	dealLogger *logs.DealLogger

	startEpochSealingBuffer abi.ChainEpoch // TODO: move to config and init properly
	remoteCommp             bool
}

func NewDirectDealsProvider(fullnodeApi v1api.FullNode, pieceAdder types.PieceAdder, commpCalc smtypes.CommpCalculator, directDealsDB *db.DirectDataDB, dealLogger *logs.DealLogger) *DirectDealsProvider {
	return &DirectDealsProvider{
		fullnodeApi: fullnodeApi,
		pieceAdder:  pieceAdder,
		commpCalc:   commpCalc,

		//db: db,
		directDealsDB: directDealsDB,
		//logsSqlDB: logsSqlDB,
		//logsDB: logsDB,

		dealLogger: dealLogger,
	}
}

func (ddp *DirectDealsProvider) Start(ctx context.Context) error {
	ddp.ctx = ctx

	deals, err := ddp.directDealsDB.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("failed to list active direct deals: %w", err)
	}

	for _, entry := range deals {
		log.Infow("direct deal entry", "uuid", entry.ID, "checkpoint", entry.Checkpoint)

		go func() {
			err := ddp.Process(ctx, entry.ID)
			if err != nil {
				log.Errorw("error while processing direct deal", "uuid", entry.ID, "err", err)
			}
		}()
	}

	return nil
}

func (ddp *DirectDealsProvider) Accept(ctx context.Context, entry *types.DirectDataEntry) (*api.ProviderDealRejectionInfo, error) {
	chainHead, err := ddp.fullnodeApi.ChainHead(ctx)
	if err != nil {
		log.Warnw("failed to get chain head", "err", err)
		return nil, err
	}

	log.Infow("chain head", "epoch", chainHead)

	if chainHead.Height()+ddp.startEpochSealingBuffer > entry.StartEpoch {
		return &api.ProviderDealRejectionInfo{
			Accepted: false,
			Reason: fmt.Sprintf(
				"cannot propose direct deal with piece CID %s: current epoch %d has passed direct deal proposal start epoch %d",
				entry.PieceCID, chainHead.Height(), entry.StartEpoch),
		}, nil
	}

	allocation, err := ddp.fullnodeApi.StateGetAllocation(ctx, entry.Client, entry.AllocationID, ltypes.EmptyTSK)
	if err != nil {
		return nil, fmt.Errorf("failed to get allocations: %w", err)
	}

	if allocation == nil {
		return &api.ProviderDealRejectionInfo{
			Accepted: false,
			Reason:   fmt.Sprintf("allocation %d not found for client %s", entry.AllocationID, entry.Client),
		}, nil
	}

	log.Infow("found allocation for client", "allocation", spew.Sdump(allocation))

	// TODO: validate the deal proposal and check for deal acceptance (allocation id, term, start epoch, end epoch, etc.)
	// TermMin ; TermMax

	return &api.ProviderDealRejectionInfo{
		Accepted: true,
		Reason:   "",
	}, nil
}

func (ddp *DirectDealsProvider) Import(ctx context.Context, piececid cid.Cid, filepath string, deleteAfterImport bool, allocationId uint64, clientAddr address.Address, removeUnsealedCopy bool, skipIpniAnnounce bool, startEpoch, endEpoch abi.ChainEpoch) (*api.ProviderDealRejectionInfo, error) {
	log.Infow("received direct data import", "piececid", piececid, "filepath", filepath, "clientAddr", clientAddr, "allocationId", allocationId)

	entry := &types.DirectDataEntry{
		ID:        uuid.New(),
		CreatedAt: time.Now(),
		//CreatedAt time.Time
		PieceCID: piececid,
		//PieceSize abi.PaddedPieceSize
		Client: clientAddr,
		//Provider  address.Address

		StartEpoch: startEpoch,
		EndEpoch:   endEpoch,

		CleanupData:      deleteAfterImport,
		InboundFilePath:  filepath,
		AllocationID:     verifregst.AllocationId(allocationId),
		KeepUnsealedCopy: !removeUnsealedCopy,
		AnnounceToIPNI:   !skipIpniAnnounce,
		//SectorID abi.SectorNumber
		//Offset   abi.PaddedPieceSize
		//Length   abi.PaddedPieceSize
		//Checkpoint dealcheckpoints.Checkpoint
		//CheckpointAt time.Time
		//Err string
		//Retry DealRetryType
	}

	ddp.dealLogger.Infow(entry.ID, "executing direct deal import", "client", clientAddr, "piececid", piececid)

	log.Infow("check for deal acceptance", "uuid", entry.ID, "piececid", piececid, "filepath", filepath)

	res, err := ddp.Accept(ctx, entry)
	if err != nil {
		return nil, err
	}

	log.Infow("deal accepted. insert direct deal entry to local db", "uuid", entry.ID, "piececid", piececid, "filepath", filepath)

	if res.Accepted {
		err := ddp.directDealsDB.Insert(ctx, entry)
		if err != nil {
			return nil, fmt.Errorf("failed to insert direct deal entry to local db: %w", err)
		}

		go func() {
			err := ddp.Process(ddp.ctx, entry.ID)
			if err != nil {
				log.Errorw("error while processing direct deal", "uuid", entry.ID, "err", err)
			}
		}()
	}

	return res, nil
}

func (ddp *DirectDealsProvider) Process(ctx context.Context, dealUuid uuid.UUID) error {
	entry, err := ddp.directDealsDB.ByID(ctx, dealUuid)
	if err != nil {
		return err
	}

	log.Infow("processing direct deal", "uuid", dealUuid, "checkpoint", entry.Checkpoint, "piececid", entry.PieceCID, "filepath", entry.InboundFilePath, "clientAddr", entry.Client, "allocationId", entry.AllocationID)

	if entry.Checkpoint <= dealcheckpoints.Accepted { // before commp

		// os stat
		fstat, err := os.Stat(entry.InboundFilePath)
		if err != nil {
			return err
		}

		// throttle for local commp
		throttle := make(chan struct{}, 1)
		// TODO: should we be passing pieceSize here ??!?
		pieceSize := abi.UnpaddedPieceSize(fstat.Size())

		generatedPieceInfo, dmErr := generatePieceCommitment(ctx, ddp.commpCalc, throttle, entry.InboundFilePath, pieceSize.Padded(), ddp.remoteCommp)
		if dmErr != nil {
			return fmt.Errorf("couldnt generate commp: %w", dmErr) // retry
		}

		log.Infow("direct deal details", "filepath", entry.InboundFilePath, "supplied-piececid", entry.PieceCID, "calculated-piececid", generatedPieceInfo.PieceCID, "calculated-piecesize", generatedPieceInfo.Size, "os stat size", fstat.Size())

		if !entry.PieceCID.Equals(generatedPieceInfo.PieceCID) {
			return fmt.Errorf("commp mismatch: %v vs %v", entry.PieceCID, generatedPieceInfo.PieceCID) // retry
		}

		entry.PieceSize = generatedPieceInfo.Size

		entry.Checkpoint = dealcheckpoints.Transferred
		err = ddp.directDealsDB.Update(ctx, entry)
		if err != nil {
			return err
		}
	}

	// In this context Transferred === supplied and generated commp match for data
	if entry.Checkpoint <= dealcheckpoints.Transferred {
		// Open a reader against the CAR file with the deal data
		v2r, err := carv2.OpenReader(entry.InboundFilePath)
		if err != nil {
			return err
		}

		defer func() {
			_ = v2r.Close()
		}()

		var size uint64
		switch v2r.Version {
		case 1:
			st, err := os.Stat(entry.InboundFilePath)
			if err != nil {
				return err
			}
			size = uint64(st.Size())
		case 2:
			size = v2r.Header.DataSize
		}

		// Inflate the deal size so that it exactly fills a piece
		r, err := v2r.DataReader()
		if err != nil {
			return err
		}

		log.Infow("got v2r.DataReader over inbound file")

		paddedReader, err := padreader.NewInflator(r, size, entry.PieceSize.Unpadded())
		if err != nil {
			return err
		}

		log.Infow("got paddedReader")

		stateAddr, err := ddp.fullnodeApi.StateLookupID(ctx, entry.Client, ltypes.EmptyTSK)
		if err != nil {
			return err
		}

		clientId, err := address.IDFromAddress(stateAddr)
		if err != nil {
			return err
		}

		// Add the piece to a sector
		sdInfo := lapi.PieceDealInfo{
			// "Old" builtin-market deal info
			//PublishCid   *cid.Cid
			//DealID       abi.DealID
			//DealProposal *market.DealProposal

			// Common deal info, required for all pieces
			DealSchedule: lapi.DealSchedule{
				StartEpoch: entry.StartEpoch,
				EndEpoch:   entry.EndEpoch,
			},

			// Direct Data Onboarding
			// When PieceActivationManifest is set, builtin-market deal info must not be set
			PieceActivationManifest: &miner.PieceActivationManifest{
				CID:  entry.PieceCID,
				Size: entry.PieceSize,
				VerifiedAllocationKey: &miner.VerifiedAllocationKey{
					Client: abi.ActorID(clientId),
					ID:     verifreg.AllocationId(uint64(entry.AllocationID)), // TODO: fix verifreg v9 or v12
				},
				//Notify                []DataActivationNotification
				Notify: nil,
			},

			// Best-effort deal asks
			KeepUnsealed: entry.KeepUnsealedCopy,
		}

		// Attempt to add the piece to a sector (repeatedly if necessary)
		sectorNum, offset, err := ddp.pieceAdder.AddPiece(ctx, entry.PieceSize.Unpadded(), paddedReader, sdInfo)
		if err != nil {
			return fmt.Errorf("AddPiece failed: %w", err)
		}

		ddp.dealLogger.Infow(entry.ID, "direct deal successfully handed to the sealing subsystem", "sectorNum", sectorNum.String(), "offset", offset)

		entry.SectorID = sectorNum
		entry.Offset = offset
		entry.Length = entry.PieceSize

		entry.Checkpoint = dealcheckpoints.AddedPiece
		err = ddp.directDealsDB.Update(ctx, entry)
		if err != nil {
			return err
		}
	}

	if entry.Checkpoint <= dealcheckpoints.AddedPiece {
		// add index and announce

	}

	//curTime := build.Clock.Now()

	//for build.Clock.Since(curTime) < addPieceRetryTimeout {
	//if !errors.Is(err, sealing.ErrTooManySectorsSealing) {
	//if err != nil {
	////p.dealLogger.Warnw(deal.DealUuid, "failed to addPiece for deal, will-retry", "err", err.Error())
	//}
	//break
	//}
	//select {
	//case <-build.Clock.After(addPieceRetryWait):
	//sectorNum, offset, err = p.pieceAdder.AddPiece(ctx, entry.PieceSize, pieceData, sdInfo)
	//case <-ctx.Done():
	//return nil, fmt.Errorf("error while waiting to retry AddPiece: %w", ctx.Err())
	//}
	//}

	//p.dealLogger.Infow(deal.DealUuid, "added new deal to sector", "sector", sectorNum.String())

	//deal.SectorID = packingInfo.SectorNumber
	//deal.Offset = packingInfo.Offset
	//deal.Length = packingInfo.Size
	//p.dealLogger.Infow(deal.DealUuid, "deal successfully handed to the sealing subsystem",
	//"sectorNum", deal.SectorID.String(), "offset", deal.Offset, "length", deal.Length)

	//if derr := p.updateCheckpoint(pub, deal, dealcheckpoints.AddedPiece); derr != nil {
	//return derr
	//}

	return nil
}
