package storagemarket

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/db/migrations"
	"github.com/filecoin-project/boost/extern/boostd-data/shared/tracing"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/piecedirectory"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/types"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/storagemarket/types/legacytypes"
	"github.com/filecoin-project/boost/transport"
	"github.com/filecoin-project/dagstore"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v1api"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	"github.com/filecoin-project/lotus/storage/pipeline/piece"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/attribute"
)

var (
	ErrDealNotFound        = fmt.Errorf("deal not found")
	ErrDealHandlerNotFound = errors.New("deal handler not found")
	ErrDealNotInSector     = errors.New("storage failed - deal not found in sector")
	ErrSectorSealingFailed = errors.New("storage failed - sector failed to seal")
)

var (
	addPieceRetryWait    = 5 * time.Minute
	addPieceRetryTimeout = 6 * time.Hour
)

type SealingPipelineCache struct {
	Status     sealingpipeline.Status
	CacheTime  time.Time
	CacheError error
}

// PackingResult returns information about how a deal was put into a sector
type PackingResult struct {
	SectorNumber abi.SectorNumber
	Offset       abi.PaddedPieceSize
	Size         abi.PaddedPieceSize
}

// DagstoreShardRegistry provides the one method from the Dagstore that we use
// in deal execution: registering a shard
type DagstoreShardRegistry interface {
	RegisterShard(ctx context.Context, pieceCid cid.Cid, carPath string, eagerInit bool, resch chan dagstore.ShardResult) error
}

type Config struct {
	// The maximum amount of time a transfer can take before it fails
	MaxTransferDuration time.Duration
	// Whether to do commp on the Boost node (local) or the sealing node (remote)
	RemoteCommp     bool
	TransferLimiter TransferLimiterConfig
	// Cleanup deal logs from DB older than this many number of days
	DealLogDurationDays int
	// Cache timeout for Sealing Pipeline status
	SealingPipelineCacheTimeout time.Duration
	StorageFilter               string
	Curio                       bool
}

var log = logging.Logger("boost-provider")

type Provider struct {
	config Config
	// Address of the provider on chain.
	Address address.Address

	ctx       context.Context
	cancel    context.CancelFunc
	closeSync sync.Once
	runWG     sync.WaitGroup

	newDealPS *newDealPS

	// channels used to pass messages to run loop
	acceptDealChan       chan acceptDealReq
	finishedDealChan     chan finishedDealReq
	publishedDealChan    chan publishDealReq
	updateRetryStateChan chan updateRetryStateReq
	storageSpaceChan     chan storageSpaceDealReq
	processedDealChan    chan processedDealReq

	// Sealing Pipeline API
	sps      sealingpipeline.API
	spsCache SealingPipelineCache

	// Boost deal filter
	df dtypes.StorageDealFilter

	// Database API
	db        *sql.DB
	dealsDB   *db.DealsDB
	logsSqlDB *sql.DB
	logsDB    *db.LogsDB

	Transport      transport.Transport
	xferLimiter    *transferLimiter
	fundManager    *fundmanager.FundManager
	storageManager *storagemanager.StorageManager
	dealPublisher  types.DealPublisher
	transfers      *dealTransfers

	pieceAdder                  types.PieceAdder
	commpThrottle               CommpThrottle
	commpCalc                   smtypes.CommpCalculator
	maxDealCollateralMultiplier uint64
	chainDealManager            types.ChainDealManager

	fullnodeApi v1api.FullNode

	dhsMu sync.RWMutex
	dhs   map[uuid.UUID]*dealHandler // Map of deal handlers indexed by deal uuid.

	dealLogger *logs.DealLogger

	piecedirectory *piecedirectory.PieceDirectory
	ip             types.IndexProvider
	askGetter      types.AskGetter
	sigVerifier    types.SignatureVerifier
}

func NewProvider(cfg Config, sqldb *sql.DB, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager,
	fullnodeApi v1api.FullNode, dp types.DealPublisher, addr address.Address, pa types.PieceAdder, commpCalc smtypes.CommpCalculator, commpThrottle CommpThrottle,
	sps sealingpipeline.API, cm types.ChainDealManager, df dtypes.StorageDealFilter, logsSqlDB *sql.DB, logsDB *db.LogsDB,
	piecedirectory *piecedirectory.PieceDirectory, ip types.IndexProvider, askGetter types.AskGetter,
	sigVerifier types.SignatureVerifier, dl *logs.DealLogger, tspt transport.Transport) (*Provider, error) {

	xferLimiter, err := newTransferLimiter(cfg.TransferLimiter)
	if err != nil {
		return nil, err
	}

	v, err := sps.Version(context.Background())
	if err != nil {
		return nil, err
	}

	if strings.Contains(v.String(), "curio") {
		cfg.Curio = true
	}

	newDealPS, err := newDealPubsub()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())

	if cfg.SealingPipelineCacheTimeout < 0 {
		cfg.SealingPipelineCacheTimeout = 30 * time.Second
	}

	return &Provider{
		ctx:       ctx,
		cancel:    cancel,
		config:    cfg,
		Address:   addr,
		newDealPS: newDealPS,
		db:        sqldb,
		dealsDB:   dealsDB,
		logsSqlDB: logsSqlDB,
		sps:       sps,
		spsCache:  SealingPipelineCache{},
		df:        df,

		acceptDealChan:       make(chan acceptDealReq),
		finishedDealChan:     make(chan finishedDealReq),
		publishedDealChan:    make(chan publishDealReq),
		updateRetryStateChan: make(chan updateRetryStateReq),
		storageSpaceChan:     make(chan storageSpaceDealReq),
		processedDealChan:    make(chan processedDealReq),

		Transport:      tspt,
		xferLimiter:    xferLimiter,
		fundManager:    fundMgr,
		storageManager: storageMgr,

		dealPublisher:               dp,
		fullnodeApi:                 fullnodeApi,
		pieceAdder:                  pa,
		commpThrottle:               commpThrottle,
		commpCalc:                   commpCalc,
		chainDealManager:            cm,
		maxDealCollateralMultiplier: 2,
		transfers:                   newDealTransfers(),

		dhs:        make(map[uuid.UUID]*dealHandler),
		dealLogger: dl,
		logsDB:     logsDB,

		piecedirectory: piecedirectory,
		ip:             ip,
		askGetter:      askGetter,
		sigVerifier:    sigVerifier,
	}, nil
}

func (p *Provider) Deal(ctx context.Context, dealUuid uuid.UUID) (*types.ProviderDealState, error) {
	ctx, span := tracing.Tracer.Start(ctx, "Provider.Deal")
	defer span.End()
	span.SetAttributes(attribute.String("dealUuid", dealUuid.String())) // Example of adding additional attributes

	deal, err := p.dealsDB.ByID(ctx, dealUuid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("getting deal %s: %w", dealUuid, ErrDealNotFound)
	}
	return deal, nil
}

func (p *Provider) DealBySignedProposalCid(ctx context.Context, propCid cid.Cid) (*types.ProviderDealState, error) {
	deal, err := p.dealsDB.BySignedProposalCID(ctx, propCid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("getting deal %s: %w", propCid, ErrDealNotFound)
	}
	return deal, nil
}

func (p *Provider) GetAsk() *legacytypes.SignedStorageAsk {
	return p.askGetter.GetAsk(p.Address)
}

// ImportOfflineDealData is called when the Storage Provider imports data for
// an offline deal (the deal must already have been proposed by the client)
func (p *Provider) ImportOfflineDealData(ctx context.Context, dealUuid uuid.UUID, filePath string, delAfterImport bool) (pi *api.ProviderDealRejectionInfo, err error) {
	p.dealLogger.Infow(dealUuid, "import data for offline deal", "filepath", filePath, "delete after import", delAfterImport)

	// db should already have a deal with this uuid as the deal proposal should have been made beforehand
	ds, err := p.dealsDB.ByID(p.ctx, dealUuid)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("no pre-existing deal proposal for offline deal %s: %w", dealUuid, err)
		}
		return nil, fmt.Errorf("getting offline deal %s: %w", dealUuid, err)
	}
	if !ds.IsOffline {
		return nil, fmt.Errorf("deal %s is not an offline deal", dealUuid)
	}
	if ds.Checkpoint > dealcheckpoints.Accepted {
		return nil, fmt.Errorf("deal %s has already been imported and reached checkpoint %s", dealUuid, ds.Checkpoint)
	}

	ds.InboundFilePath = filePath
	ds.CleanupData = delAfterImport

	resp, err := p.checkForDealAcceptance(ctx, ds, true)
	if err != nil {
		p.dealLogger.LogError(dealUuid, "failed to send deal for acceptance", err)
		return nil, fmt.Errorf("failed to send deal for acceptance: %w", err)
	}

	// if there was an error, we just return the error message (there is no rejection reason)
	if resp.err != nil {
		return nil, fmt.Errorf("failed to accept deal: %w", resp.err)
	}

	// return rejection reason as provider has rejected the deal
	if !resp.ri.Accepted {
		p.dealLogger.Infow(dealUuid, "deal execution rejected by provider", "reason", resp.ri.Reason)
		return resp.ri, nil
	}

	p.dealLogger.Infow(dealUuid, "offline deal data imported and deal scheduled for execution")
	return resp.ri, nil
}

// ExecuteDeal is called when the Storage Provider receives a deal proposal
// from the network
func (p *Provider) ExecuteDeal(ctx context.Context, dp *types.DealParams, clientPeer peer.ID) (*api.ProviderDealRejectionInfo, error) {
	ctx, span := tracing.Tracer.Start(ctx, "Provider.ExecuteLibp2pDeal")
	defer span.End()

	span.SetAttributes(attribute.String("dealUuid", dp.DealUUID.String())) // Example of adding additional attributes

	p.dealLogger.Infow(dp.DealUUID, "executing deal proposal received from network", "peer", clientPeer)

	ds := types.ProviderDealState{
		DealUuid:           dp.DealUUID,
		ClientDealProposal: dp.ClientDealProposal,
		ClientPeerID:       clientPeer,
		DealDataRoot:       dp.DealDataRoot,
		Transfer:           dp.Transfer,
		IsOffline:          dp.IsOffline,
		CleanupData:        !dp.IsOffline,
		Retry:              smtypes.DealRetryAuto,
		FastRetrieval:      !dp.RemoveUnsealedCopy,
		AnnounceToIPNI:     !dp.SkipIPNIAnnounce,
	}

	// Validate the deal proposal
	if err := p.validateDealProposal(ds); err != nil {
		// Send the client a reason for the rejection that doesn't reveal the
		// internal error message
		reason := err.reason
		if reason == "" {
			reason = err.Error()
		}

		// Log the internal error message
		p.dealLogger.Infow(dp.DealUUID, "deal proposal failed validation", "err", err.Error(), "reason", reason)
		return &api.ProviderDealRejectionInfo{
			Reason: fmt.Sprintf("failed validation: %s", reason),
		}, nil
	}

	return p.executeDeal(ctx, ds)
}

// executeDeal sends the deal to the main provider run loop for execution
func (p *Provider) executeDeal(ctx context.Context, ds smtypes.ProviderDealState) (*api.ProviderDealRejectionInfo, error) {
	ctx, span := tracing.Tracer.Start(ctx, "Provider.executeDeal")
	defer span.End()

	ri, err := func() (*api.ProviderDealRejectionInfo, error) {
		// send the deal to the main provider loop for execution
		resp, err := p.checkForDealAcceptance(ctx, &ds, false)
		if err != nil {
			p.dealLogger.LogError(ds.DealUuid, "failed to send deal for acceptance", err)
			return nil, fmt.Errorf("failed to send deal for acceptance: %w", err)
		}

		// if there was an error, we don't return a rejection reason, just the error.
		if resp.err != nil {
			return nil, fmt.Errorf("failed to accept deal: %w", resp.err)
		}

		// log rejection reason as provider has rejected the deal.
		if !resp.ri.Accepted {
			p.dealLogger.Infow(ds.DealUuid, "deal rejected by provider", "reason", resp.ri.Reason)
		}

		return resp.ri, nil
	}()
	if err != nil || ri == nil || !ri.Accepted {
		// if there was an error processing the deal, or the deal was rejected, return
		return ri, err
	}

	if ds.IsOffline {
		p.dealLogger.Infow(ds.DealUuid, "offline deal accepted, waiting for data import")
	} else {
		p.dealLogger.Infow(ds.DealUuid, "deal accepted and scheduled for execution")
	}

	return ri, nil
}

func (p *Provider) checkForDealAcceptance(ctx context.Context, ds *types.ProviderDealState, isImport bool) (acceptDealResp, error) {
	_, span := tracing.Tracer.Start(ctx, "Provider.checkForDealAcceptance")
	defer span.End()

	// send message to run loop to run the deal through the acceptance filter and reserve the required resources
	// then wait for a response and return the response to the client.
	respChan := make(chan acceptDealResp, 1)
	select {
	case p.acceptDealChan <- acceptDealReq{rsp: respChan, deal: ds, isImport: isImport}:
	case <-p.ctx.Done():
		return acceptDealResp{}, p.ctx.Err()
	}

	var resp acceptDealResp
	select {
	case resp = <-respChan:
	case <-p.ctx.Done():
		return acceptDealResp{}, p.ctx.Err()
	}

	return resp, nil
}

func (p *Provider) Start() error {
	log.Infow("storage provider: starting")

	// initialize the database
	log.Infow("db: creating tables")
	err := db.CreateAllBoostTables(p.ctx, p.db, p.logsSqlDB)
	if err != nil {
		return fmt.Errorf("failed to init db: %w", err)
	}

	log.Infow("db: performing migrations")
	err = migrations.Migrate(p.db)
	if err != nil {
		return fmt.Errorf("failed to migrate db: %w", err)
	}

	// De-fragment the logs DB
	_, err = p.logsSqlDB.Exec("Vacuum")
	if err != nil {
		log.Errorf("failed to de-fragment the logs db: %w", err)
	}

	log.Infow("db: initialized")

	// cleanup all completed deals in case Boost resumed before they were cleanedup
	finished, err := p.dealsDB.ListCompleted(p.ctx)
	if err != nil {
		return fmt.Errorf("failed to list completed deals: %w", err)
	}
	if len(finished) > 0 {
		log.Infof("cleaning up %d completed deals", len(finished))
	}
	for i := range finished {
		p.cleanupDealOnRestart(finished[i])
	}
	if len(finished) > 0 {
		log.Infof("finished cleaning up %d completed deals", len(finished))
	}

	// restart all active deals
	activeDeals, err := p.dealsDB.ListActive(p.ctx)
	if err != nil {
		return fmt.Errorf("failed to list active deals: %w", err)
	}

	// cleanup all deals that have finished successfully
	for _, deal := range activeDeals {
		// Make sure that deals that have reached the IndexedAndAnnounced stage
		// have their resources untagged
		// TODO Update this once we start listening for expired/slashed deals etc
		if deal.Checkpoint >= dealcheckpoints.IndexedAndAnnounced {
			// cleanup if cleanup didn't finish before we restarted
			p.cleanupDealOnRestart(deal)
		}
	}

	// Restart active deals
	for _, deal := range activeDeals {
		// Check if deal is already proving
		if deal.Checkpoint >= dealcheckpoints.IndexedAndAnnounced {
			si, err := p.sps.SectorsStatus(p.ctx, deal.SectorID, false)
			if err != nil || IsFinalSealingState(si.State) {
				continue
			}
		}

		// Set up a deal handler so that clients can subscribe to update
		// events about the deal
		dh, err := p.mkAndInsertDealHandler(deal.DealUuid)
		if err != nil {
			p.dealLogger.LogError(deal.DealUuid, "failed to restart deal", err)
			continue
		}

		// Fail deals if start epoch has passed and deal has still not been added to a sector
		if deal.Checkpoint < dealcheckpoints.AddedPiece {
			if serr := p.checkDealProposalStartEpoch(deal); serr != nil {
				go p.failDeal(dh.Publisher, deal, serr, false)
				continue
			}
		}

		// If it's an offline deal, and the deal data hasn't yet been
		// imported, just wait for the SP operator to import the data
		if deal.IsOffline && deal.InboundFilePath == "" {
			p.dealLogger.Infow(deal.DealUuid, "restarted deal: waiting for offline deal data import")
			continue
		}

		// Check if the deal can be restarted automatically.
		// Note that if the retry type is "fatal" then the deal should already
		// have been marked as complete (and therefore not returned by ListActive).
		if deal.Retry != smtypes.DealRetryAuto {
			p.dealLogger.Infow(deal.DealUuid, "deal must be manually restarted: waiting for manual restart")
			continue
		}

		// Restart deal
		p.dealLogger.Infow(deal.DealUuid, "resuming deal on boost restart", "checkpoint", deal.Checkpoint.String())
		_, err = p.startDealThread(dh, deal)
		if err != nil {
			p.dealLogger.LogError(deal.DealUuid, "failed to restart deal", err)
		}
	}

	// Start provider run loop
	go p.run()

	// Start sampling transfer data rate
	go p.transfers.start(p.ctx)

	// Start the transfer limiter
	go p.xferLimiter.run(p.ctx)

	// Start hourly deal and funds log cleanup
	if p.config.DealLogDurationDays > 0 {
		go p.dealLogger.LogCleanup(p.ctx, p.config.DealLogDurationDays)
		go p.fundManager.LogCleanup(p.ctx, p.config.DealLogDurationDays)
	}

	log.Infow("storage provider: started")
	return nil
}

func (p *Provider) cleanupDealOnRestart(deal *types.ProviderDealState) {
	// remove the temp file created for inbound deal data if it is not an offline deal
	if !deal.IsOffline {
		_ = os.Remove(deal.InboundFilePath)
	}

	// untag storage space
	errs := p.storageManager.Untag(p.ctx, deal.DealUuid)
	if errs == nil {
		p.dealLogger.Infow(deal.DealUuid, "untagged storage space")
	}

	// untag funds
	collat, pub, errf := p.fundManager.UntagFunds(p.ctx, deal.DealUuid)
	if errf == nil {
		p.dealLogger.Infow(deal.DealUuid, "untagged funds for deal as deal has finished", "untagged publish", pub, "untagged collateral", collat)
	}
}

func (p *Provider) Stop() {
	p.closeSync.Do(func() {
		log.Infow("storage provider: shutdown")

		deals, err := p.dealsDB.ListActive(p.ctx)
		if err == nil {
			for i := range deals {
				dl := deals[i]
				if dl.Checkpoint < dealcheckpoints.AddedPiece {
					log.Infow("shutting down running deal", "id", dl.DealUuid.String(), "ckp", dl.Checkpoint.String())
				}
			}
		}

		log.Infow("storage provider: stop run loop")
		p.cancel()
		p.runWG.Wait()
		log.Info("storage provider: shutdown complete")
	})
}

// SubscribeNewDeals subscribes to "new deal" events
func (p *Provider) SubscribeNewDeals() (event.Subscription, error) {
	return p.newDealPS.subscribe()
}

// SubscribeDealUpdates subscribes to updates to a deal
func (p *Provider) SubscribeDealUpdates(dealUuid uuid.UUID) (event.Subscription, error) {
	dh := p.getDealHandler(dealUuid)
	if dh == nil {
		return nil, ErrDealHandlerNotFound
	}

	return dh.subscribeUpdates()
}

// RetryPausedDeal starts execution of a deal from the point at which it stopped
func (p *Provider) RetryPausedDeal(dealUuid uuid.UUID) error {
	return p.updateRetryState(dealUuid, true)
}

// FailPausedDeal moves a deal from the paused state to the failed state
func (p *Provider) FailPausedDeal(dealUuid uuid.UUID) error {
	return p.updateRetryState(dealUuid, false)
}

// CancelOfflineDealAwaitingImport moves an offline deal from waiting for data state to the failed state
func (p *Provider) CancelOfflineDealAwaitingImport(dealUuid uuid.UUID) error {
	pds, err := p.dealsDB.ByID(p.ctx, dealUuid)
	if err != nil {
		return fmt.Errorf("failed to lookup deal in DB: %w", err)
	}
	if !pds.IsOffline {
		return errors.New("cannot cancel an online deal")
	}

	if pds.InboundFilePath != "" {
		return errors.New("deal has already started importing data")
	}

	dh := p.getDealHandler(dealUuid)
	if dh == nil {
		return ErrDealHandlerNotFound
	}

	if !dh.isRunning() {
		return p.updateRetryState(dealUuid, false)
	}

	return errors.New("deal is already running")
}

// updateRetryState either retries the deal or terminates the deal
// (depending on the value of retry)
func (p *Provider) updateRetryState(dealUuid uuid.UUID, retry bool) error {
	resp := make(chan error, 1)
	select {
	case p.updateRetryStateChan <- updateRetryStateReq{dealUuid: dealUuid, retry: retry, done: resp}:
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
	select {
	case err := <-resp:
		return err
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

func (p *Provider) CancelDealDataTransfer(dealUuid uuid.UUID) error {
	// Ideally, the UI should never show the cancel data transfer button for an offline deal
	pds, err := p.dealsDB.ByID(p.ctx, dealUuid)
	if err != nil {
		return fmt.Errorf("failed to lookup deal in DB: %w", err)
	}
	if pds.IsOffline {
		return errors.New("cannot cancel data transfer for an offline deal")
	}

	dh := p.getDealHandler(dealUuid)
	if dh == nil {
		return ErrDealHandlerNotFound
	}

	err = dh.cancelTransfer()
	if err == nil {
		p.dealLogger.Infow(dealUuid, "deal data transfer cancelled by user")
	} else {
		p.dealLogger.Warnw(dealUuid, "error when user tried to cancel deal data transfer", "err", err)
	}
	return err
}

func (p *Provider) AddPieceToSector(ctx context.Context, deal smtypes.ProviderDealState, pieceData io.Reader) (*PackingResult, error) {
	// Sanity check - we must have published the deal before handing it off
	// to the sealing subsystem
	if deal.PublishCID == nil {
		return nil, fmt.Errorf("deal.PublishCid can't be nil")
	}

	sdInfo := piece.PieceDealInfo{
		DealID:       deal.ChainDealID,
		DealProposal: &deal.ClientDealProposal.Proposal,
		PublishCid:   deal.PublishCID,
		DealSchedule: piece.DealSchedule{
			StartEpoch: deal.ClientDealProposal.Proposal.StartEpoch,
			EndEpoch:   deal.ClientDealProposal.Proposal.EndEpoch,
		},
		KeepUnsealed: deal.FastRetrieval,
	}

	// Attempt to add the piece to a sector (repeatedly if necessary)
	pieceSize := deal.ClientDealProposal.Proposal.PieceSize.Unpadded()
	sectorNum, offset, err := addPieceWithRetry(ctx, p.pieceAdder, pieceSize, pieceData, sdInfo)
	if err != nil {
		return nil, fmt.Errorf("AddPiece failed: %w", err)
	}
	p.dealLogger.Infow(deal.DealUuid, "added new deal to sector", "sector", sectorNum.String())

	return &PackingResult{
		SectorNumber: sectorNum,
		Offset:       offset,
		Size:         pieceSize.Padded(),
	}, nil
}

func addPieceWithRetry(ctx context.Context, pieceAdder smtypes.PieceAdder, pieceSize abi.UnpaddedPieceSize, pieceData io.Reader, sdInfo piece.PieceDealInfo) (abi.SectorNumber, abi.PaddedPieceSize, error) {
	info, err := pieceAdder.SectorAddPieceToAny(ctx, pieceSize, pieceData, sdInfo)
	curTime := build.Clock.Now()
	for err != nil && build.Clock.Since(curTime) < addPieceRetryTimeout {
		// Check if the error was because there are too many sectors sealing
		if !errors.Is(err, sealing.ErrTooManySectorsSealing) {
			// There was some other error, return it
			return 0, 0, err
		}

		// There are too many sectors sealing, back off for a while then try again
		select {
		case <-build.Clock.After(addPieceRetryWait):
			info, err = pieceAdder.SectorAddPieceToAny(ctx, pieceSize, pieceData, sdInfo)
		case <-ctx.Done():
			return 0, 0, fmt.Errorf("shutdown while adding piece")
		}
	}
	return info.Sector, info.Offset, err
}
