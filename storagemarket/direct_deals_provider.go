package storagemarket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
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
	Curio                   bool
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

	sealingPollEvery time.Duration
	sealingClock     func() time.Time

	noSealerPiecesOnce sync.Once

	pd *piecedirectory.PieceDirectory
	ip *indexprovider.Wrapper
}

func (ddp *DirectDealsProvider) pollEvery() time.Duration {
	if ddp.sealingPollEvery > 0 {
		return ddp.sealingPollEvery
	}
	return sealingPollInterval
}

func (ddp *DirectDealsProvider) now() time.Time {
	if ddp.sealingClock != nil {
		return ddp.sealingClock()
	}
	return time.Now()
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

	v, err := ddp.sps.Version(ctx)
	if err != nil {
		return err
	}

	if strings.Contains(v.String(), "curio") {
		ddp.config.Curio = true
	}

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

// DirectDealRejectionAtNv29 is the reason a direct deal is turned away from nv29 on.
const DirectDealRejectionAtNv29 = "DDO (FIL+ verified deals) is no longer supported at network version 29+: datacap was deprecated by FIP-0118"

// RejectedAtNv29 reports whether a new deal has to be turned away at this network
// version; for a sealed sector ask PieceOnboardedAtOrAfterNv29 instead.
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

// nv29UpgradeHeight returns the epoch nv29 activates at, off the node's own fork
// schedule. A zero cannot be used: the node predates the field.
func nv29UpgradeHeight(ctx context.Context, api v1api.FullNode) (abi.ChainEpoch, error) {
	params, err := api.StateGetNetworkParams(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting network params: %w", err)
	}

	height := params.ForkUpgradeParams.UpgradeSolsticeHeight
	if height == 0 {
		return 0, errors.New("the full node reported no nv29 upgrade height: too old to know it")
	}
	return height, nil
}

// Nv29Boundary is where nv29 sits on the chain a node is synced to.
type Nv29Boundary struct {
	Reached bool
	Height  abi.ChainEpoch
}

// ResolveNv29Boundary asks the chain where nv29 sits on it.
func ResolveNv29Boundary(ctx context.Context, api v1api.FullNode) (Nv29Boundary, error) {
	nv, err := api.StateNetworkVersion(ctx, ltypes.EmptyTSK)
	if err != nil {
		return Nv29Boundary{}, fmt.Errorf("getting network version: %w", err)
	}
	if !RejectedAtNv29(nv) {
		return Nv29Boundary{}, nil
	}

	height, err := nv29UpgradeHeight(ctx, api)
	if err != nil {
		return Nv29Boundary{}, err
	}
	return Nv29Boundary{Reached: true, Height: height}, nil
}

// PieceOnboardedAtOrAfterNv29 reports whether a sector holds piece data that entered
// it at or after nv29, where FIP-0118 writes no claim for it.
//
// The sector's own history decides this, not the chain's current version, which would
// excuse a claim that really did go missing.
func PieceOnboardedAtOrAfterNv29(ctx context.Context, api v1api.FullNode, miner address.Address, sector abi.SectorNumber) (bool, error) {
	boundary, err := ResolveNv29Boundary(ctx, api)
	if err != nil {
		return false, err
	}
	return boundary.PieceOnboarded(ctx, api, miner, sector)
}

func (b Nv29Boundary) PieceOnboarded(ctx context.Context, api v1api.FullNode, miner address.Address, sector abi.SectorNumber) (bool, error) {
	if !b.Reached {
		return false, nil
	}

	si, err := api.StateSectorGetInfo(ctx, miner, sector, ltypes.EmptyTSK)
	if err != nil {
		return false, fmt.Errorf("getting sector info: %w", err)
	}
	return b.SectorOnboarded(si), nil
}

// SectorOnboarded is PieceOnboarded for sector info already in hand, so the
// migration, holding every sector from one StateMinerSectors call, reads no chain.
func (b Nv29Boundary) SectorOnboarded(si *minertypes.SectorOnChainInfo) bool {
	if !b.Reached || si == nil {
		return false
	}

	// No piece spacetime: committed capacity, or a snap that never landed.
	if !sectorstatemgr.SectorCarriesData(si.DealWeight) && !sectorstatemgr.SectorCarriesData(si.VerifiedDealWeight) {
		return false
	}

	// Activation only records when the sector was committed, so a snapped sector is
	// dated from PowerBaseEpoch, which the first ReplicaUpdate moves.
	onboarded := si.Activation
	if si.SectorKeyCID != nil {
		onboarded = si.PowerBaseEpoch
	}
	return onboarded >= b.Height
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

// ErrNoClaimFound is the failure a direct deal records when its sector finished
// sealing but the allocation was never claimed and the data cannot be shown to have
// been onboarded past nv29. The migration to Curio excuses this one fatal error.
var ErrNoClaimFound = errors.New("no claim found")

// ErrPieceNotOnboarded is the failure a direct deal records when the chain shows its
// sector's data landed but the sector does not hold the deal's piece. Deliberately
// not ErrNoClaimFound, which the migration excuses.
var ErrPieceNotOnboarded = errors.New("piece not onboarded")

// ErrPieceUnverifiable is the failure a direct deal records when the chain shows its
// sector's data landed past nv29 but the sealer gave no piece list to check the deal's
// piece against. Deliberately not ErrNoClaimFound either.
var ErrPieceUnverifiable = errors.New("piece unverifiable")

const claimWaitLimit = 10 * time.Minute

const sealingPollInterval = 10 * time.Second

// claimWait measures that wait. Its clock starts at the first final sealing state, not
// when the deal began being watched: a sector can spend hours collecting its remaining
// pieces, so an earlier budget would be gone before any answer.
type claimWait struct {
	since time.Time
}

// start begins the wait if it has not begun, so returning to a final state gives no
// fresh budget.
func (w *claimWait) start(now time.Time) {
	if w.since.IsZero() {
		w.since = now
	}
}

// expired reports whether the wait has run out; a wait that never began has not.
func (w *claimWait) expired(now time.Time) bool {
	return !w.since.IsZero() && now.Sub(w.since) > claimWaitLimit
}

type claimOutcome int

const (
	claimPending claimOutcome = iota
	// claimUnverifiable: keep waiting, but a timeout means the sealer never
	// established the outcome.
	claimUnverifiable
	claimDone
)

func fatal(err error) *dealMakingError {
	return &dealMakingError{retry: types.DealRetryFatal, error: err}
}

func retryable(err error) *dealMakingError {
	return &dealMakingError{retry: types.DealRetryAuto, error: err}
}

// watchSealingUpdates periodically checks the sealing status of the deal,
// and returns once the deal is active (or boost is shutdown)
func (ddp *DirectDealsProvider) watchSealingUpdates(entry *types.DirectDeal) *dealMakingError {
	var lastSealingState lapi.SectorState
	finalSealingState := func() lapi.SectorState {
		si, err := ddp.sps.SectorsStatus(ddp.ctx, entry.SectorID, false)
		if err != nil {
			log.Warnw("getting sector sealing state", "sector", entry.SectorID, "err", err.Error())
			return ""
		}

		if si.State != lastSealingState {
			lastSealingState = si.State
			ddp.dealLogger.Infow(entry.ID, "current sealing state", "state", si.State)
		}

		if !IsFinalSealingState(si.State) {
			return ""
		}
		return si.State
	}

	var wait claimWait

	check := func() (done bool, derr *dealMakingError) {
		state := finalSealingState()
		if state == "" {
			return false, nil
		}
		wait.start(ddp.now())

		outcome, derr := ddp.settle(entry, state)
		if derr != nil {
			return true, derr
		}
		if outcome == claimDone {
			return true, nil
		}
		if wait.expired(ddp.now()) {
			return true, timeoutError(outcome)
		}
		return false, nil
	}

	if done, derr := check(); done {
		return derr
	}

	ddp.dealLogger.Infow(entry.ID, "watching deal sealing state changes")
	ticker := time.NewTicker(ddp.pollEvery())
	defer ticker.Stop()
	for {
		select {
		case <-ddp.ctx.Done():
			return retryable(ddp.ctx.Err())
		case <-ticker.C:
			if done, derr := check(); done {
				return derr
			}
		}
	}
}

// settle reports what the chain says about a deal whose sector has reached a final
// sealing state. The claim is looked up first, as the pre-nv29 code did: it settles the
// question on its own. Only with no claim does the sealing state end the deal.
func (ddp *DirectDealsProvider) settle(entry *types.DirectDeal, state lapi.SectorState) (claimOutcome, *dealMakingError) {
	isClaimed, found, err := ddp.confirmClaim(ddp.ctx, entry.AllocationID, entry.SectorID)
	if err != nil {
		return claimPending, retryable(err)
	}
	if found {
		if !isClaimed {
			return claimPending, fatal(errors.New("sector mismatch for claim"))
		}
		return claimDone, nil
	}

	// Past nv29 no claim is written, so the sector is the whole of the outcome, and that
	// takes both halves: dated past the upgrade, and holding this deal's piece.
	outcome := claimPending
	onboarded, err := PieceOnboardedAtOrAfterNv29(ddp.ctx, ddp.fullnodeApi, ddp.Address, entry.SectorID)
	if err != nil {
		return claimPending, retryable(err)
	}
	if onboarded {
		// No on-chain record names the sector's piece CIDs past nv29, so the piece is checked
		// against the sealer's.
		si, err := ddp.sps.SectorsStatus(ddp.ctx, entry.SectorID, false)
		if err != nil {
			return claimPending, retryable(fmt.Errorf("getting sector status: %w", err))
		}
		held, reported := SectorHoldsPiece(si, entry.PieceCID)
		switch {
		case held:
			ddp.dealLogger.Infow(entry.ID,
				"piece onboarded at network version 29 or above, where FIP-0118 writes no claim for it; the deal is complete")
			return claimDone, nil
		case !reported:
			// Nothing names the piece either way: keep looking, and let the timeout record it.
			ddp.warnSealerListsNoPieces(entry)
			outcome = claimUnverifiable
		default:
			// Waiting cannot change this: fail rather than let the timeout excuse it.
			return claimPending, fatal(fmt.Errorf("%w: sector %d carries data from network version 29 or above without the deal's piece %s",
				ErrPieceNotOnboarded, entry.SectorID, entry.PieceCID))
		}
	}

	if IsFailedSealingState(state) {
		return claimPending, fatal(fmt.Errorf("%w: sector %d reached sealing state %s", ErrSectorSealingFailed, entry.SectorID, state))
	}
	return outcome, nil
}

// timeoutError names the failure a deal ends on when its wait runs out: ErrNoClaimFound,
// whose text the migration matches on to carry such a deal over.
func timeoutError(outcome claimOutcome) *dealMakingError {
	if outcome == claimUnverifiable {
		return fatal(ErrPieceUnverifiable)
	}
	return fatal(ErrNoClaimFound)
}

// warnSealerListsNoPieces tells the operator once per process that the sealer does not
// report which pieces a sector holds.
func (ddp *DirectDealsProvider) warnSealerListsNoPieces(entry *types.DirectDeal) {
	ddp.noSealerPiecesOnce.Do(func() {
		ddp.dealLogger.Warnw(entry.ID,
			"sector carries data from network version 29 or above but the sealer reported no pieces to check the deal's piece against; post-nv29 direct deals cannot be verified against this sealer and will be reported as unverifiable when they time out",
			"sector", entry.SectorID, "piece", entry.PieceCID)
	})
}

// SectorHoldsPiece reports whether a sealer's record holds a piece, and whether it
// listed any pieces at all.
func SectorHoldsPiece(si lapi.SectorInfo, pieceCID cid.Cid) (held bool, reported bool) {
	for _, p := range si.Pieces {
		if p.Piece.PieceCID.Equals(pieceCID) {
			return true, true
		}
	}
	return false, len(si.Pieces) > 0
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
	// If this is Curio sealer then we should wait till sector finishes sealing
	if ddp.config.Curio {
		// Wait for sector to finish sealing
		err := ddp.trackCurioSealing(entry.SectorID)
		if err != nil {
			return err
		}
	}

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

func (ddp *DirectDealsProvider) confirmClaim(ctx context.Context, allocId verifreg9types.AllocationId, sectorNum abi.SectorNumber) (bool, bool, error) {
	claim, err := ddp.fullnodeApi.StateGetClaim(ctx, ddp.Address, verifreg9types.ClaimId(allocId), ltypes.EmptyTSK)
	if err != nil {
		return false, false, fmt.Errorf("getting claim details for allocationID %d: %s", allocId, err)
	}
	if claim == nil {
		return false, false, nil
	}
	if claim.Sector != sectorNum {
		return false, true, nil
	}
	return true, true, nil
}

func (ddp *DirectDealsProvider) trackCurioSealing(sectorNum abi.SectorNumber) *dealMakingError {
	var lastSealingState lapi.SectorState
	checkStatus := func() lapi.SectorInfo {
		// Get the sector status
		si, err := ddp.sps.SectorsStatus(ddp.ctx, sectorNum, false)
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
		case <-ddp.ctx.Done():
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
