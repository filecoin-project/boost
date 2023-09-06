package storagemarket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/types"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin/v12/miner"
	"github.com/filecoin-project/go-state-types/builtin/v12/verifreg"
	verifregst "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
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

		id := entry.ID
		go ddp.Process(ctx, id)
	}

	return nil
}

func (ddp *DirectDealsProvider) Accept(ctx context.Context, entry *types.DirectDeal) (*api.ProviderDealRejectionInfo, error) {
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

	entry := &types.DirectDeal{
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

		go ddp.Process(ddp.ctx, entry.ID)
	}

	return res, nil
}

func (ddp *DirectDealsProvider) Process(ctx context.Context, dealUuid uuid.UUID) {
	deal, dberr := ddp.directDealsDB.ByID(ctx, dealUuid)
	if dberr != nil {
		log.Errorw("error fetching direct deal", "uuid", dealUuid, "err", dberr)
		return
	}

	ddp.dealLogger.Infow(dealUuid, "deal execution initiated", "deal state", deal)

	// Clear any error from a previous run
	if deal.Err != "" || deal.Retry == smtypes.DealRetryAuto {
		deal.Err = ""
		deal.Retry = smtypes.DealRetryAuto
		ddp.saveDealToDB(deal)
	}

	// Execute the deal
	err := ddp.execDeal(ctx, deal)
	if err == nil {
		// Deal completed successfully
		return
	}

	// If the error is fatal, fail the deal
	if err.retry == types.DealRetryFatal {
		ddp.failDeal(deal, err)
		return
	}

	// The error is recoverable, so just add a line to the deal log and
	// wait for the deal to be executed again (either manually by the user
	// or automatically when boost restarts)
	if errors.Is(err.error, context.Canceled) {
		ddp.dealLogger.Infow(dealUuid, "deal paused because boost was shut down",
			"checkpoint", deal.Checkpoint.String())
	} else {
		ddp.dealLogger.Infow(dealUuid, "deal paused because of recoverable error", "err", err.error.Error(),
			"checkpoint", deal.Checkpoint.String(), "retry", err.retry)
	}

	deal.Retry = err.retry
	deal.Err = err.Error()
	ddp.saveDealToDB(deal)
}

func (ddp *DirectDealsProvider) execDeal(ctx context.Context, entry *smtypes.DirectDeal) (dmerr *dealMakingError) {
	// Capture any panic as a manually retryable error
	dealUuid := entry.ID
	defer func() {
		if err := recover(); err != nil {
			log.Errorw("caught panic executing deal", "id", dealUuid, "err", err)
			fmt.Fprint(os.Stderr, string(debug.Stack()))
			dmerr = &dealMakingError{
				error: fmt.Errorf("Caught panic in deal execution: %s\n%s", err, debug.Stack()),
				retry: smtypes.DealRetryManual,
			}
		}
	}()

	log.Infow("processing direct deal", "uuid", dealUuid, "checkpoint", entry.Checkpoint, "piececid", entry.PieceCID, "filepath", entry.InboundFilePath, "clientAddr", entry.Client, "allocationId", entry.AllocationID)
	ddp.dealLogger.Infow(dealUuid, "deal execution initiated", "deal state", entry)

	if entry.Checkpoint <= dealcheckpoints.Accepted { // before commp

		// os stat
		fstat, err := os.Stat(entry.InboundFilePath)
		if err != nil {
			return &dealMakingError{
				error: fmt.Errorf("failed to open file '%s': %w", entry.InboundFilePath, err),
				retry: smtypes.DealRetryFatal,
			}
		}

		ddp.dealLogger.Infow(dealUuid, "size of deal", "filepath", entry.InboundFilePath, "size", fstat.Size())
		ddp.dealLogger.Infow(dealUuid, "generating commp")

		// throttle for local commp
		throttle := make(chan struct{}, 1)
		// TODO: should we be passing pieceSize here ??!?
		pieceSize := abi.UnpaddedPieceSize(fstat.Size())

		generatedPieceInfo, dmErr := generatePieceCommitment(ctx, ddp.commpCalc, throttle, entry.InboundFilePath, pieceSize.Padded(), ddp.remoteCommp)
		if dmErr != nil {
			return &dealMakingError{
				retry: types.DealRetryManual,
				error: fmt.Errorf("failed to generate commp: %w", dmErr),
			}
		}

		log.Infow("direct deal details", "filepath", entry.InboundFilePath, "supplied-piececid", entry.PieceCID, "calculated-piececid", generatedPieceInfo.PieceCID, "calculated-piecesize", generatedPieceInfo.Size, "os stat size", fstat.Size())

		if !entry.PieceCID.Equals(generatedPieceInfo.PieceCID) {
			return &dealMakingError{
				retry: types.DealRetryManual,
				error: fmt.Errorf("commP expected=%s, actual=%s: %w", entry.PieceCID, generatedPieceInfo.PieceCID, ErrCommpMismatch),
			}
		}
		ddp.dealLogger.Infow(dealUuid, "completed generating commp")

		entry.PieceSize = generatedPieceInfo.Size

		if err := ddp.updateCheckpoint(ctx, entry, dealcheckpoints.Transferred); err != nil {
			return err
		}
	}

	// In this context Transferred === supplied and generated commp match for data
	if entry.Checkpoint <= dealcheckpoints.Transferred {
		ddp.dealLogger.Infow(dealUuid, "looking up client on chain", "client", entry.Client)
		stateAddr, err := ddp.fullnodeApi.StateLookupID(ctx, entry.Client, ltypes.EmptyTSK)
		if err != nil {
			return &dealMakingError{
				retry: types.DealRetryAuto,
				error: fmt.Errorf("failed to look up client %s on chain: %w", entry.Client, err),
			}
		}

		clientId, err := address.IDFromAddress(stateAddr)
		if err != nil {
			return &dealMakingError{
				retry: types.DealRetryFatal,
				error: fmt.Errorf("failed to convert %s to id address: %w", stateAddr, err),
			}
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

		// Open a reader over the piece data
		ddp.dealLogger.Infow(dealUuid, "opening reader over piece data", "filepath", entry.InboundFilePath)
		paddedReader, err := openReader(entry.InboundFilePath, entry.PieceSize.Unpadded())
		if err != nil {
			return &dealMakingError{
				retry: types.DealRetryFatal,
				error: fmt.Errorf("failed to read piece data: %w", err),
			}
		}

		// Attempt to add the piece to a sector (repeatedly if necessary)
		ddp.dealLogger.Infow(dealUuid, "adding piece to sector", "filepath", entry.InboundFilePath)
		sectorNum, offset, err := addPieceWithRetry(ctx, ddp.pieceAdder, entry.PieceSize.Unpadded(), paddedReader, sdInfo)
		_ = paddedReader.Close()
		if err != nil {
			return &dealMakingError{
				retry: types.DealRetryAuto,
				error: fmt.Errorf("add piece %s: %w", entry.PieceCID, err),
			}
		}

		ddp.dealLogger.Infow(entry.ID, "direct deal successfully handed to the sealing subsystem", "sectorNum", sectorNum.String(), "offset", offset)

		entry.SectorID = sectorNum
		entry.Offset = offset
		entry.Length = entry.PieceSize

		if err := ddp.updateCheckpoint(ctx, entry, dealcheckpoints.AddedPiece); err != nil {
			return err
		}

		// The deal has been handed off to the sealer, so we can remove the inbound file
		if entry.CleanupData {
			_ = os.Remove(entry.InboundFilePath)
			ddp.dealLogger.Infow(dealUuid, "removed piece data from disk as deal has been added to a sector", "path", entry.InboundFilePath)
		}
	}

	if entry.Checkpoint <= dealcheckpoints.AddedPiece {
		// add index and announce
		ddp.dealLogger.Infow(dealUuid, "index and announce")
	}

	return nil
}

func (ddp *DirectDealsProvider) updateCheckpoint(ctx context.Context, entry *smtypes.DirectDeal, ckpt dealcheckpoints.Checkpoint) *dealMakingError {
	prev := entry.Checkpoint
	entry.Checkpoint = ckpt
	err := ddp.directDealsDB.Update(ctx, entry)
	if err != nil {
		return &dealMakingError{
			retry: smtypes.DealRetryFatal,
			error: fmt.Errorf("failed to persist deal state: %w", err),
		}
	}

	ddp.dealLogger.Infow(entry.ID, "updated deal checkpoint in DB",
		"old checkpoint", prev.String(), "new checkpoint", ckpt.String())

	return nil
}

func (ddp *DirectDealsProvider) saveDealToDB(deal *smtypes.DirectDeal) {
	// In the case that the provider has been shutdown, the provider's context
	// will be cancelled, so use a background context when saving state to the
	// DB to avoid this edge case.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dberr := ddp.directDealsDB.Update(ctx, deal)
	if dberr != nil {
		ddp.dealLogger.LogError(deal.ID, "failed to update deal state in DB", dberr)
	}
}

func (ddp *DirectDealsProvider) failDeal(deal *smtypes.DirectDeal, err error) {
	// Update state in DB with error
	deal.Checkpoint = dealcheckpoints.Complete
	deal.Retry = smtypes.DealRetryFatal
	deal.Err = err.Error()
	ddp.dealLogger.LogError(deal.ID, "deal failed", err)
	ddp.saveDealToDB(deal)
}
