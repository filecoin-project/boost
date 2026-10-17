package storagemarket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	miner13types "github.com/filecoin-project/go-state-types/builtin/v13/miner"
	verifreg13types "github.com/filecoin-project/go-state-types/builtin/v13/verifreg"
	verifreg9types "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/go-state-types/network"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/extern/boostd-data/model"
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/sectorstatemgr"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	minertypes "github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/actors/policy"
	ltypes "github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	lotuspiece "github.com/filecoin-project/lotus/storage/pipeline/piece"
)

//var log = logging.Logger("direct-deals-providers")

type DDPConfig struct {
	// Whether to do commp on the Boost node (local) or the sealing node (remote)
	RemoteCommp bool
	// Minimum start epoch buffer to give time for sealing of sector with deal
	StartEpochSealingBuffer abi.ChainEpoch
}

type DirectDealsProvider struct {
	config DDPConfig
	ctx    context.Context // context to be stopped when stopping boostd

	// Address of the provider on chain.
	Address       address.Address
	fullnodeApi   v1api.FullNode
	pieceAdder    types.PieceAdder
	commpCalc     types.CommpCalculator
	commpThrottle CommpThrottle
	sps           sealingpipeline.API
	directDealsDB *db.DirectDealsDB
	dealLogger    *logs.DealLogger

	runningLk sync.RWMutex
	running   map[uuid.UUID]struct{}

	pd *piecedirectory.PieceDirectory
	ip *indexprovider.Wrapper
}

func NewDirectDealsProvider(cfg DDPConfig, minerAddr address.Address, fullnodeApi v1api.FullNode, pieceAdder types.PieceAdder, commpCalc types.CommpCalculator, commpt CommpThrottle, sps sealingpipeline.API, directDealsDB *db.DirectDealsDB, dealLogger *logs.DealLogger, piecedirectory *piecedirectory.PieceDirectory, ip *indexprovider.Wrapper) *DirectDealsProvider {
	return &DirectDealsProvider{
		config:        cfg,
		Address:       minerAddr,
		fullnodeApi:   fullnodeApi,
		pieceAdder:    pieceAdder,
		commpCalc:     commpCalc,
		commpThrottle: commpt,
		sps:           sps,
		directDealsDB: directDealsDB,
		//logsSqlDB: logsSqlDB,
		//logsDB: logsDB,

		dealLogger: dealLogger,
		running:    make(map[uuid.UUID]struct{}),
		pd:         piecedirectory,
		ip:         ip,
	}
}

func (ddp *DirectDealsProvider) Start(ctx context.Context) error {
	log.Infow("direct deals provider: starting")
	ddp.ctx = ctx

	deals, err := ddp.directDealsDB.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("failed to list active direct deals: %w", err)
	}

	// For each deal
	for _, entry := range deals {
		log.Infow("direct deal entry", "uuid", entry.ID, "checkpoint", entry.Checkpoint)

		// Start executing the deal in a go routine
		ddp.startDealThread(entry.ID)
	}

	log.Infow("direct deals provider: started")
	return nil
}

// DirectDealRejectionAtNv29: the deal's VerifiedAllocationKey can no longer become a claim.
const DirectDealRejectionAtNv29 = "DDO (FIL+ verified deals) is no longer supported at network version 29+: datacap was deprecated by FIP-0118"

// RejectedAtNv29 answers for a proposal: from nv29 on, FIP-0118 writes no claim for it.
func RejectedAtNv29(nv network.Version) bool {
	return nv >= network.Version29
}

func (ddp *DirectDealsProvider) isNv29OrAbove(ctx context.Context) (bool, error) {
	nv, err := ddp.fullnodeApi.StateNetworkVersion(ctx, ltypes.EmptyTSK)
	if err != nil {
		return false, fmt.Errorf("getting network version: %w", err)
	}
	return RejectedAtNv29(nv), nil
}

func (ddp *DirectDealsProvider) Accept(ctx context.Context, entry *types.DirectDeal) (*api.ProviderDealRejectionInfo, error) {
	chainHead, err := ddp.fullnodeApi.ChainHead(ctx)
	if err != nil {
		log.Warnw("failed to get chain head", "err", err)
		return nil, err
	}

	log.Infow("chain head", "epoch", chainHead)

	// FIP-0118 (Solstice): from nv29 datacap and verifreg are deprecated, so the
	// DDO (FIL+ verified) flow can no longer be served. Reject explicitly here
	// instead of relying on an implicit allocation-not-found failure below.
	nv29, err := ddp.isNv29OrAbove(ctx)
	if err != nil {
		return nil, err
	}
	if nv29 {
		return &api.ProviderDealRejectionInfo{
			Accepted: false,
			Reason:   DirectDealRejectionAtNv29,
		}, nil
	}

	if chainHead.Height()+ddp.config.StartEpochSealingBuffer > entry.StartEpoch {
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

	mid, err := address.IDFromAddress(ddp.Address)
	if err != nil {
		log.Warnw("failed to get miner ID from address", "err", err)
		return &api.ProviderDealRejectionInfo{
			Accepted: false,
			Reason:   "server error: miner address to ID conversion",
		}, nil
	}

	if allocation.Provider != abi.ActorID(mid) {
		return &api.ProviderDealRejectionInfo{
			Accepted: false,
			Reason:   fmt.Sprintf("allocation provider %s does not match miner address %s", allocation.Provider, ddp.Address),
		}, nil
	}

	// If the TermMin is longer than initial sector duration, the deal will be dropped from the sector
	if allocation.TermMin > miner13types.MaxSectorExpirationExtension-policy.SealRandomnessLookback {
		return &api.ProviderDealRejectionInfo{
			Accepted: false,
			Reason:   fmt.Sprintf("allocation term min %d is longer than the sector lifetime %d", allocation.TermMin, miner13types.MaxSectorExpirationExtension-policy.SealRandomnessLookback),
		}, nil

	}

	allActive, err := ddp.directDealsDB.ListActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get active deals: %w", err)
	}

	for _, deal := range allActive {
		// We should only process 1 deal for each allocation at one time to avoid trying to claim
		// same allocation for 2 sectors at once
		if entry.Client == deal.Client && entry.AllocationID == deal.AllocationID {
			return &api.ProviderDealRejectionInfo{
				Accepted: false,
				Reason:   fmt.Sprintf("another deal for client %s and allocation %d is already in process", entry.Client, entry.AllocationID),
			}, nil
		}
	}

	return &api.ProviderDealRejectionInfo{
		Accepted: true,
		Reason:   "",
	}, nil
}

func (ddp *DirectDealsProvider) Import(ctx context.Context, params types.DirectDealParams) (*api.ProviderDealRejectionInfo, error) {
	piececid := params.PieceCid.String()
	clientAddr := params.ClientAddr.String()
	log.Infow("received direct data import", "piececid", piececid, "filepath", params.FilePath, "clientAddr", clientAddr, "allocationId", params.AllocationID)

	entry := &types.DirectDeal{
		ID:        params.DealUUID,
		CreatedAt: time.Now(),
		PieceCID:  params.PieceCid,
		//PieceSize abi.PaddedPieceSize
		Client: params.ClientAddr,
		//Provider  address.Address

		StartEpoch: params.StartEpoch,
		EndEpoch:   params.EndEpoch,

		CleanupData:      params.DeleteAfterImport,
		InboundFilePath:  params.FilePath,
		AllocationID:     params.AllocationID,
		KeepUnsealedCopy: !params.RemoveUnsealedCopy,
		AnnounceToIPNI:   !params.SkipIPNIAnnounce,
		Retry:            types.DealRetryAuto,
	}

	ddp.dealLogger.Infow(entry.ID, "executing direct deal import", "client", clientAddr, "piececid", piececid)

	log.Infow("check for deal acceptance", "uuid", entry.ID.String(), "piececid", piececid, "filepath", params.FilePath)

	res, err := ddp.Accept(ctx, entry)
	if err != nil {
		return nil, err
	}

	// If the deal was rejected (e.g. DDO is unsupported at nv29, or the
	// allocation is missing) there is nothing left to import.
	if !res.Accepted {
		return res, nil
	}

	allocation, err := ddp.fullnodeApi.StateGetAllocation(ctx, entry.Client, entry.AllocationID, ltypes.EmptyTSK)
	if err != nil {
		return nil, fmt.Errorf("failed to get allocations: %w", err)
	}

	// Accept checked this allocation a moment ago, so a nil here means it went
	// away in between. Reject rather than dereference it below.
	if allocation == nil {
		return &api.ProviderDealRejectionInfo{
			Accepted: false,
			Reason:   fmt.Sprintf("allocation %d not found for client %s", entry.AllocationID, entry.Client),
		}, nil
	}

	idaddr, err := address.NewIDAddress(uint64(allocation.Provider))
	if err != nil {
		return nil, fmt.Errorf("new id address for allocation %s: %w", allocation.Provider, err)
	}

	maddr, err := ddp.fullnodeApi.StateLookupID(ctx, idaddr, ltypes.EmptyTSK)
	if err != nil {
		return nil, fmt.Errorf("looking up %s: %w", idaddr, err)
	}

	if res.Accepted {
		log.Infow("deal accepted. insert direct deal entry to local db", "uuid", entry.ID, "piececid", piececid, "maddr", maddr, "filepath", params.FilePath)

		entry.Provider = maddr

		err := ddp.directDealsDB.Insert(ctx, entry)
		if err != nil {
			return nil, fmt.Errorf("failed to insert direct deal entry to local db: %w", err)
		}

		// Start executing the deal in a go routine
		ddp.startDealThread(entry.ID)
	}

	return res, nil
}

func (ddp *DirectDealsProvider) startDealThread(id uuid.UUID) bool {
	ddp.runningLk.Lock()
	defer ddp.runningLk.Unlock()

	_, isRunning := ddp.running[id]
	if !isRunning {
		ddp.running[id] = struct{}{}
		go func() {
			ddp.process(ddp.ctx, id)

			ddp.runningLk.Lock()
			defer ddp.runningLk.Unlock()
			delete(ddp.running, id)
		}()
	}

	startedNewThread := !isRunning
	return startedNewThread
}

func (ddp *DirectDealsProvider) process(ctx context.Context, dealUuid uuid.UUID) {
	deal, dberr := ddp.directDealsDB.ByID(ctx, dealUuid)
	if dberr != nil {
		log.Errorw("error fetching direct deal", "uuid", dealUuid, "err", dberr)
		return
	}

	ddp.dealLogger.Infow(dealUuid, "deal execution initiated", "deal state", deal)

	// Clear any error from a previous run
	if deal.Err != "" || deal.Retry == types.DealRetryAuto {
		deal.Err = ""
		deal.Retry = types.DealRetryAuto
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
		ddp.dealLogger.Infow(dealUuid, "deal paused because of recoverable error", "err", err.Error(),
			"checkpoint", deal.Checkpoint.String(), "retry", err.retry)
	}

	deal.Retry = err.retry
	deal.Err = err.Error()
	ddp.saveDealToDB(deal)
}

func (ddp *DirectDealsProvider) execDeal(ctx context.Context, entry *types.DirectDeal) (dmerr *dealMakingError) {
	// Capture any panic as a manually retryable error
	dealUuid := entry.ID
	defer func() {
		if err := recover(); err != nil {
			log.Errorw("caught panic executing deal", "id", dealUuid, "err", err)
			fmt.Fprint(os.Stderr, string(debug.Stack()))
			dmerr = &dealMakingError{
				error: fmt.Errorf("caught panic in deal execution: %s\n%s", err, debug.Stack()),
				retry: types.DealRetryManual,
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
				retry: types.DealRetryFatal,
			}
		}

		ddp.dealLogger.Infow(dealUuid, "size of deal", "filepath", entry.InboundFilePath, "size", fstat.Size())
		ddp.dealLogger.Infow(dealUuid, "generating commp")

		// TODO: should we be passing pieceSize here ??!?
		pieceSize := abi.UnpaddedPieceSize(fstat.Size())

		generatedPieceInfo, dmErr := generatePieceCommitment(ctx, ddp.commpCalc, ddp.commpThrottle, entry.InboundFilePath, pieceSize.Padded(), ddp.config.RemoteCommp)
		if dmErr != nil {
			return &dealMakingError{
				retry: types.DealRetryManual,
				error: fmt.Errorf("failed to generate commp: %w", dmErr),
			}
		}

		entry.InboundFileSize = fstat.Size()

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

		_ = clientId

		// Add the piece to a sector
		sdInfo := lotuspiece.PieceDealInfo{
			// Common deal info, required for all pieces
			DealSchedule: lotuspiece.DealSchedule{
				StartEpoch: entry.StartEpoch,
				EndEpoch:   entry.EndEpoch,
			},

			// Direct Data Onboarding
			// When PieceActivationManifest is set, builtin-market deal info must not be set
			PieceActivationManifest: &minertypes.PieceActivationManifest{
				CID:  entry.PieceCID,
				Size: entry.PieceSize,
				VerifiedAllocationKey: &miner13types.VerifiedAllocationKey{
					Client: abi.ActorID(clientId),
					ID:     verifreg13types.AllocationId(entry.AllocationID),
				},
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

	// Index and announce the deal
	if entry.Checkpoint < dealcheckpoints.IndexedAndAnnounced {
		if err := ddp.indexAndAnnounce(ctx, entry); err != nil {
			err.error = fmt.Errorf("failed to add index and announce deal: %w", err.error)
			return err
		}
		if entry.AnnounceToIPNI {
			ddp.dealLogger.Infow(entry.ID, "deal successfully indexed and announced")
		} else {
			ddp.dealLogger.Infow(entry.ID, "deal successfully indexed")
		}
	} else {
		ddp.dealLogger.Infow(entry.ID, "deal has already been indexed and announced")
	}

	if entry.Checkpoint < dealcheckpoints.Complete {
		// The deal has been added to a piece, so just watch the deal sealing state
		if derr := ddp.watchSealingUpdates(entry); derr != nil {
			return derr
		}
		ddp.dealLogger.Infow(entry.ID, "deal sealing reached termination state")
		if err := ddp.updateCheckpoint(ctx, entry, dealcheckpoints.Complete); err != nil {
			return err
		}
	}

	return nil
}

// watchSealingUpdates polls the sealing status of the deal and returns once it
// is active, or boost shuts down.
//
// Only a genuine terminal outcome ends the watch: everything the node cannot yet
// see -- a sector missing from this chain, an absent claim, an unsettled piece --
// is left to the next poll. A missing claim in particular is not a failure from
// nv29 on, where FIP-0118 writes none; the sector itself confirms the data.
func (ddp *DirectDealsProvider) watchSealingUpdates(entry *types.DirectDeal) *dealMakingError {
	var lastSealingState lapi.SectorState

	// The deal row is this thread's output, not an input. The only caller has
	// already found it incomplete, and no writer can complete it while the watch
	// runs: the running map admits one thread per deal, FailPausedDeal rejects a
	// running deal, RetryPausedDeal refuses a complete one, and this thread
	// writes Complete only after this function returns. So every verdict below
	// comes from the chain, and none of them is read back from the row.
	check := func() (bool, *dealMakingError) {
		step := ddp.checkSealingOnce(ddp.ctx, entry)

		if step.observed && step.state != lastSealingState {
			// Sector status has changed
			lastSealingState = step.state
			ddp.dealLogger.Infow(entry.ID, "current sealing state", "state", step.state)
		}

		if step.done && step.err == nil {
			ddp.dealLogger.Infow(entry.ID,
				"the deal's sector is committed and live on chain",
				"sector", entry.SectorID, "case", step.caseID.String())
		}

		return step.done, step.err
	}

	// Check immediately if the sector has reached a terminal state
	if done, derr := check(); done {
		return derr
	}

	// Check status every 10 seconds
	ddp.dealLogger.Infow(entry.ID, "watching deal sealing state changes")
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ddp.ctx.Done():
			return &dealMakingError{
				retry: types.DealRetryAuto,
				error: ddp.ctx.Err(),
			}
		case <-ticker.C:
			if done, derr := check(); done {
				return derr
			}
		}
	}
}

// sealingWatchStep is the outcome of a single poll of the sealer's status.
type sealingWatchStep struct {
	// state is the sealer's reported state; only meaningful when observed.
	state lapi.SectorState
	// observed is false when the state could not be read, so state means nothing.
	observed bool

	// done ends the watch: the deal is proven in the sector, or it failed.
	done bool
	// err is the failure. Set only when done and the deal did not make it: a
	// done step with a nil err is a success.
	err *dealMakingError
	// caseID names the shape the chain reads identified, for logging. Set only
	// for a successful done step.
	caseID sealingCase
}

// checkSealingOnce performs one poll: ask the sealer, and only if it is settled
// ask the chain. Split out from the ticker loop so the state-by-chain matrix is
// testable directly.
func (ddp *DirectDealsProvider) checkSealingOnce(ctx context.Context, entry *types.DirectDeal) sealingWatchStep {
	si, err := ddp.sps.SectorsStatus(ctx, entry.SectorID, false)
	if err != nil {
		// A status read that fails is not a verdict; keep polling.
		log.Warnw("getting sector sealing state", "sector", entry.SectorID, "err", err.Error())
		return sealingWatchStep{}
	}

	step := sealingWatchStep{state: si.State, observed: true}

	switch sealingStateGateFor(si.State) {
	case sealingStateFailed:
		// An unrecoverable failure is the one verdict the sealer alone can give.
		step.done = true
		step.err = &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("%w: sector %d reached sealing state %s",
				ErrSectorSealingFailed, entry.SectorID, si.State),
		}
		return step
	case sealingStateRemoved:
		// The sector was deleted by the sealer, so there is nothing on chain to
		// read and nothing to wait for.
		step.done = true
		step.err = &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("%w: sector %d was removed by the sealer (state %s)",
				ErrSectorRemovedByProvider, entry.SectorID, si.State),
		}
		return step
	case sealingStatePending:
		// Still sealing; nothing on chain can be concluded yet.
		return step
	}

	// The sealer reports a terminal state only once the sector is on chain, so
	// confirm from the chain's own record that this deal's data landed.
	sc, err := ddp.fullnodeApi.StateSectorGetInfo(ctx, ddp.Address, entry.SectorID, ltypes.EmptyTSK)
	if err != nil {
		log.Warnw("getting sector from chain", "sector", entry.SectorID, "err", err.Error())
		return step
	}
	// sc may be nil -- this node's chain does not show the sector yet. That is
	// node lag, not a verdict, but the claim is still read: a claim that points
	// elsewhere settles the deal without ever seeing this sector.
	claim, err := ddp.claimForAllocation(entry)
	if err != nil {
		log.Warnw("getting claim for allocation", "allocation", entry.AllocationID, "err", err.Error())
		return step
	}

	read := sealedSectorRead{
		sector:      sc,
		claim:       claim,
		pieces:      si.Pieces,
		pieceCID:    entry.PieceCID,
		pieceSize:   entry.PieceSize,
		offset:      entry.Offset,
		piecesKnown: sectorPiecesKnown(si),
	}

	scase, verdict := verdictForSealedSector(read, entry.SectorID)
	step.caseID = scase
	switch verdict {
	case sealingDone:
		step.done = true
		return step
	case sealingClaimElsewhere:
		step.done = true
		step.err = &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("sector mismatch for claim: allocation %d is claimed by a sector other than %d",
				entry.AllocationID, entry.SectorID),
		}
		return step
	case sealingRolledBack:
		step.done = true
		step.err = &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("%w: sector %d carries no data and the piece is not in it -- the snap fell back to the original CC",
				ErrSectorSealingFailed, entry.SectorID),
		}
		return step
	case sealingPieceMissing:
		step.done = true
		step.err = &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("%w: sector %d carries another piece, not this deal's %s (size %d, offset %d)",
				ErrSectorSealingFailed, entry.SectorID, entry.PieceCID, entry.PieceSize, entry.Offset),
		}
		return step
	}

	// sealingNoEvidence: keep polling -- the snap may not have settled, or a
	// piece onboarded before nv29 may still have a claim to appear.
	return step
}

// sealingStateGate is the sealer's own verdict on a sector, taken before any
// chain read. It is the first rung of the evidence ladder.
type sealingStateGate int

const (
	// sealingStatePending: the sealer is still working; nothing on chain can be
	// concluded yet.
	sealingStatePending sealingStateGate = iota
	// sealingStateFailed: unrecoverable. Terminal, and a failure.
	sealingStateFailed
	// sealingStateSettled: the sealer is done; whether the data landed is the
	// chain's question now.
	sealingStateSettled
	// sealingStateRemoved: the sealer deleted the sector, so it will not become
	// where this deal's data lives. A sector removed before activation never
	// appears on chain, so there is nothing to wait for.
	sealingStateRemoved
)

// sealingStateGateFor maps any sealer state onto the gate. Every state in
// sealing.ExistSectorStateList lands on exactly one arm, and the test asserts it
// against the list, so a state added by a future lotus cannot fall through to a
// default. FailedUnrecoverable is checked first: it is also in
// IsFinalSealingState -- terminal, but terminal-and-failed, a verdict rather
// than a hand-off.
func sealingStateGateFor(state lapi.SectorState) sealingStateGate {
	if sealing.SectorState(state) == sealing.FailedUnrecoverable {
		return sealingStateFailed
	}
	if isSectorRemoved(sealing.SectorState(state)) {
		return sealingStateRemoved
	}
	if IsFinalSealingState(state) {
		return sealingStateSettled
	}
	return sealingStatePending
}

// isSectorRemoved reports whether the sealer has deleted this sector: the one
// shape that cannot resolve itself, since a deleted sector never appears on
// chain and waiting for its data waits for something that cannot happen.
//
// Deliberately watcher-local: IsFinalSealingState is shared with the market-deal
// path (ten call sites), where "final" means the pipeline is done, not that the
// data is going away. The rest of the termination group (Removing, Terminating,
// TerminateWait, TerminateFinality, TerminateFailed) is left settled -- those
// sectors are still on chain carrying weight, so the chain decides them.
func isSectorRemoved(state sealing.SectorState) bool {
	return state == sealing.Removed
}

// directDealSealingVerdict is the action the decision point takes for a deal
// whose sealer has reported a terminal sealing state.
type directDealSealingVerdict int

const (
	// sealingNoEvidence: the reads do not (yet) prove the data landed. Not a
	// verdict: the snap may still be settling, or this node may be behind, so
	// the watch keeps polling rather than failing the deal.
	sealingNoEvidence directDealSealingVerdict = iota
	// sealingDone: the deal's data is proven to be in the sector.
	sealingDone
	// sealingClaimElsewhere: a claim for the allocation points at another
	// sector, so this sector does not hold the deal. Terminal.
	sealingClaimElsewhere
	// sealingRolledBack: the sealer is settled in a non-upgrade state, the
	// sector carries no weight and no claim, and our piece is absent from its
	// piece list -- the snap fell back to the original CC. Terminal.
	sealingRolledBack
	// sealingPieceMissing: the sector carries data and its piece list -- one the
	// sealer really reports -- does not hold our piece at the placement we
	// recorded. Another piece took the sector. Terminal.
	sealingPieceMissing
)

// sealingCase names which of the flow's states the reads identify. It is
// derived only from the reads, never from an epoch, and exists for logs and
// tests -- the decision point does not branch on it. The boundary (CC -> snap)
// and a post-nv29 deal are identical on chain, both casePostSealedSector; the
// two pure-CC eras collapse into caseEmptyPureCC. See
// docs/en/nv29-direct-deals-sealing-flow.md.
type sealingCase int

const (
	// caseIndeterminate: this node cannot see enough to decide. Keep polling.
	caseIndeterminate sealingCase = iota
	// caseNv28SealedSector: the pre-nv29 world -- data, or a claim on this sector.
	caseNv28SealedSector
	// casePostSealedSector: weight with no claim -- a CC snapped at/after nv29, or
	// any post-nv29 deal. Same on chain.
	casePostSealedSector
	// caseEmptyPureCC: zero weight, no claim -- a CC carrying no data, either era.
	caseEmptyPureCC
	// caseClaimElsewhere: a claim for the allocation points at another sector.
	caseClaimElsewhere
	// caseRolledBack: a snap that ran and fell back to the original CC.
	caseRolledBack
	// casePieceMissing: the sector carries another piece's spacetime, not ours.
	casePieceMissing
)

func (c sealingCase) String() string {
	switch c {
	case caseNv28SealedSector:
		return "nv28-sealed"
	case casePostSealedSector:
		return "post-nv29-sealed"
	case caseEmptyPureCC:
		return "empty-pure-cc"
	case caseClaimElsewhere:
		return "claim-elsewhere"
	case caseRolledBack:
		return "rolled-back"
	case casePieceMissing:
		return "piece-missing"
	default:
		return "indeterminate"
	}
}

// sectorCarriesData is sectorstatemgr's predicate, used here so the decision
// path and the sector-state followers cannot drift apart on what "carries data"
// means. See its comment for why the QA power bit and the power-base epoch are
// not consulted.
func sectorCarriesData(si *minertypes.SectorOnChainInfo) bool {
	return sectorstatemgr.SectorCarriesData(si)
}

// sectorPiecesKnown reports whether the piece list in si is the sector's real
// list, i.e. whether an absent piece means the piece is absent. It reads the
// miner API boost already speaks -- sps.SectorsStatus, whose Pieces lotus fills
// from the sector's own list for every state -- and does not branch on which
// sealer is behind it: same call, same field, no Curio path.
//
// An empty answer cannot be taken at face value: a sealed sector's list is never
// empty (the padding is listed too), so empty means "the list was not reported",
// not "the sector holds no pieces", and is treated as unreported -- the deal
// keeps polling. Anything non-empty is judged on its content alone. The
// Curio-specific work lives in the migration (cmd/migrate-curio), where the
// recorded rows are read: no full-node API exposes a sector's pieces.
func sectorPiecesKnown(si lapi.SectorInfo) bool {
	return len(si.Pieces) > 0
}

// pieceInSector reports whether the sealer's piece list holds our piece at our
// offset. It shares one implementation with the migration, which asks the same
// question of Curio's recorded piece list.
func pieceInSector(pieces []lapi.SectorPiece, pieceCID cid.Cid, pieceSize abi.PaddedPieceSize, offset abi.PaddedPieceSize) bool {
	return sectorstatemgr.PieceInSector(pieces, pieceCID, pieceSize, offset)
}

// sealedSectorRead is everything the decision point reads for one poll. It is
// passed whole so verdictForSealedSector stays pure and the flow's full table
// is testable without a node.
type sealedSectorRead struct {
	// sector is the sector as this node's chain sees it; nil means not visible.
	sector *minertypes.SectorOnChainInfo
	// claim is the verifreg claim for the deal's allocation; nil means none.
	claim *verifreg9types.Claim
	// pieces is the sealer's piece list for the sector.
	pieces []lapi.SectorPiece
	// our piece's identity inside the sector, from SectorAddPieceToAny.
	pieceCID  cid.Cid
	pieceSize abi.PaddedPieceSize
	offset    abi.PaddedPieceSize
	// piecesKnown says the sealer actually reports this sector's piece list.
	// When false an absent piece proves nothing: neither a rollback nor a
	// superseded snap is inferred from a list that may simply be empty.
	piecesKnown bool
}

// verdictForSealedSector decides a direct deal from the sector's on-chain record
// and the allocation's claim, without dating anything by epoch: the weight check
// covers pieces onboarded at nv29 or later, where FIP-0118 writes no claim --
// the CC -> snap boundary and a fresh seal whose pre-commit landed before nv29
// but whose prove-commit landed after it; the claim check covers pieces
// onboarded before nv29. Nothing here reads the sector's shape, so a fresh seal
// across the upgrade and a snap across it are the same row: data present with no
// claim. They differ only in how the data got there -- the sealer's business,
// not something the chain records.
//
// The weight is a fact about the sector, not about this deal: a sector can
// carry somebody else's piece, and half a sector's weight is not half a verdict.
// So weight alone completes a deal only where a claim hits this sector; with no
// claim it completes one only together with the sealer's list showing our piece
// at our offset. Weight, no claim and no list establishes nothing and polls.
//
// It returns the verdict and the case the reads identify (for logs).
func verdictForSealedSector(r sealedSectorRead, sectorNum abi.SectorNumber) (sealingCase, directDealSealingVerdict) {
	// A claim pointing at another sector is terminal whatever else the reads say:
	// this sector is not where the data went. Judged before the visibility guard
	// because it does not depend on seeing this sector -- a deal that landed
	// elsewhere while the reported sector never committed would poll forever.
	if r.claim != nil && r.claim.Sector != sectorNum {
		return caseClaimElsewhere, sealingClaimElsewhere
	}

	if r.sector == nil {
		// This node's chain does not show the sector yet: node lag, not a verdict.
		return caseIndeterminate, sealingNoEvidence
	}

	inSector := r.piecesKnown && pieceInSector(r.pieces, r.pieceCID, r.pieceSize, r.offset)
	carries := sectorCarriesData(r.sector)

	switch {
	case carries && inSector:
		// Weight is present and the sealer's list holds our piece at our offset:
		// the spacetime that landed is this deal's. (nv28 shows a claim too;
		// post-nv29 and the boundary show none and read the same.)
		if r.claim != nil {
			return caseNv28SealedSector, sealingDone
		}
		return casePostSealedSector, sealingDone

	case r.claim != nil:
		// A claim hits this sector: the pre-nv29 world, where the claim is the
		// authority.
		return caseNv28SealedSector, sealingDone

	case carries && r.piecesKnown:
		// Weight is present and the sealer's list does not hold our piece at the
		// placement we recorded: the data here is not this deal's. The snap could
		// not have been ours -- an aborted snap whose sector then took another
		// piece reads exactly like this -- so the deal must fail. The weight alone
		// cannot make this call ("something landed here", not "this deal landed
		// here"); only the list separates the two. This arm needs a readable list
		// and sits after the claim arm: before nv29 a claim on this sector settles
		// the deal whether or not the list agrees.
		return casePieceMissing, sealingPieceMissing

	case carries:
		// Weight, no claim, and the sealer does not report the sector's list.
		// The weight says something landed here; nothing says it was ours, and
		// there is no claim to ask either. Keep polling.
		return caseIndeterminate, sealingNoEvidence

	case inSector:
		// Our piece is staged in the sector but the chain shows no weight yet:
		// the data has not landed. Keep polling.
		return caseEmptyPureCC, sealingNoEvidence

	case !r.piecesKnown:
		// Cannot tell an absent piece from an unreported list. Keep polling.
		return caseIndeterminate, sealingNoEvidence

	default:
		// Settled sealer, zero weight, no claim, our piece absent from a known
		// list: the snap ran and fell back -- the one shape the chain cannot tell
		// from an empty CC; only the piece list separates them.
		return caseRolledBack, sealingRolledBack
	}
}

func (ddp *DirectDealsProvider) updateCheckpoint(ctx context.Context, entry *types.DirectDeal, ckpt dealcheckpoints.Checkpoint) *dealMakingError {
	prev := entry.Checkpoint
	entry.Checkpoint = ckpt
	err := ddp.directDealsDB.Update(ctx, entry)
	if err != nil {
		return &dealMakingError{
			retry: types.DealRetryFatal,
			error: fmt.Errorf("failed to persist deal state: %w", err),
		}
	}

	ddp.dealLogger.Infow(entry.ID, "updated deal checkpoint in DB",
		"old checkpoint", prev.String(), "new checkpoint", ckpt.String())

	return nil
}

func (ddp *DirectDealsProvider) saveDealToDB(deal *types.DirectDeal) {
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

func (ddp *DirectDealsProvider) failDeal(deal *types.DirectDeal, err error) {
	// Update state in DB with error
	deal.Checkpoint = dealcheckpoints.Complete
	deal.Retry = types.DealRetryFatal
	deal.Err = err.Error()
	ddp.dealLogger.LogError(deal.ID, "deal failed", err)
	ddp.saveDealToDB(deal)
}

func (ddp *DirectDealsProvider) RetryPausedDeal(ctx context.Context, id uuid.UUID) error {
	deal, err := ddp.directDealsDB.ByID(ctx, id)
	if err != nil {
		return err
	}

	if deal.Checkpoint == dealcheckpoints.Complete {
		return fmt.Errorf("deal %s is already complete", id)
	}

	// Start executing the deal in a go routine
	started := ddp.startDealThread(id)
	if started {
		// If the deal wasn't already running, log a message saying
		// that it was restarted
		ddp.dealLogger.Infow(id, "user initiated deal retry", "checkpoint", deal.Checkpoint.String())
	} else {
		// the deal was already running - log a message saying so
		ddp.dealLogger.Infow(id, "user initiated deal retry but deal is already running", "checkpoint", deal.Checkpoint.String())
	}

	return nil
}

func (ddp *DirectDealsProvider) FailPausedDeal(ctx context.Context, id uuid.UUID) error {
	deal, err := ddp.directDealsDB.ByID(ctx, id)
	if err != nil {
		return err
	}

	if deal.Checkpoint == dealcheckpoints.Complete {
		return fmt.Errorf("deal %s is already complete", id)
	}

	ddp.runningLk.RLock()
	_, isRunning := ddp.running[id]
	ddp.runningLk.RUnlock()

	// Check if the deal is running
	if isRunning {
		return fmt.Errorf("the deal %s is running; cannot fail running deal", id)
	}

	// Update state in DB with error
	deal.Checkpoint = dealcheckpoints.Complete
	deal.Retry = types.DealRetryFatal
	if deal.Err == "" {
		err = errors.New("user manually terminated the deal")
	} else {
		err = errors.New(deal.Err)
	}
	deal.Err = "user manually terminated the deal"
	ddp.dealLogger.LogError(id, deal.Err, err)
	ddp.saveDealToDB(deal)

	if deal.CleanupData {
		_ = os.Remove(deal.InboundFilePath)
	}

	return nil
}

func (ddp *DirectDealsProvider) indexAndAnnounce(ctx context.Context, entry *types.DirectDeal) *dealMakingError {
	// add deal to piece metadata store
	ddp.dealLogger.Infow(entry.ID, "about to add direct deal for piece in LID")
	if err := ddp.pd.AddDealForPiece(ctx, entry.PieceCID, model.DealInfo{
		DealUuid:     entry.ID.String(),
		ChainDealID:  abi.DealID(entry.AllocationID), // Convert the type to avoid migration as underlying types are same
		MinerAddr:    entry.Provider,
		SectorID:     entry.SectorID,
		PieceOffset:  entry.Offset,
		PieceLength:  entry.Length,
		CarLength:    uint64(entry.InboundFileSize),
		IsDirectDeal: true,
	}); err != nil {
		return &dealMakingError{
			retry: types.DealRetryAuto,
			error: fmt.Errorf("failed to add deal to piece metadata store: %w", err),
		}
	}
	ddp.dealLogger.Infow(entry.ID, "direct deal successfully added to LID")

	// if the index provider is enabled
	if ddp.ip.Enabled() {
		if entry.AnnounceToIPNI {
			// announce to the network indexer but do not fail the deal if the announcement fails,
			// just retry the next time boost restarts
			annCid, err := ddp.ip.AnnounceBoostDirectDeal(ctx, entry)
			if err != nil {
				return &dealMakingError{
					retry: types.DealRetryAuto,
					error: fmt.Errorf("failed to announce deal to network indexer: %w", err),
				}
			}
			ddp.dealLogger.Infow(entry.ID, "announced the direct deal to network indexer", "announcement-cid", annCid)
		} else {
			ddp.dealLogger.Infow(entry.ID, "didn't announce the direct deal because the client disabled announcements for this deal")
		}
	} else {
		ddp.dealLogger.Infow(entry.ID, "didn't announce the direct deal because network indexer is disabled")
	}

	if derr := ddp.updateCheckpoint(ctx, entry, dealcheckpoints.IndexedAndAnnounced); derr != nil {
		return derr
	}

	return nil
}

// claimForAllocation reads the verifreg claim for the deal's allocation, if any.
// From nv29 on FIP-0118 writes none, so a nil claim is expected, not an error.
func (ddp *DirectDealsProvider) claimForAllocation(entry *types.DirectDeal) (*verifreg9types.Claim, error) {
	claim, err := ddp.fullnodeApi.StateGetClaim(ddp.ctx, ddp.Address, verifreg9types.ClaimId(entry.AllocationID), ltypes.EmptyTSK)
	if err != nil {
		return nil, fmt.Errorf("getting claim details for allocationID %d: %w", entry.AllocationID, err)
	}
	return claim, nil
}
