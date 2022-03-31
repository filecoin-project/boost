package storagemarket

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/build"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/filestore"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/node/modules/dtypes"
	"github.com/filecoin-project/boost/sealingpipeline"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket/logs"
	"github.com/filecoin-project/boost/storagemarket/types"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/boost/transport"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/piecestore"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-fil-markets/stores"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	ctypes "github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/extern/storage-sealing"
	"github.com/filecoin-project/lotus/lib/sigs"
	"github.com/filecoin-project/lotus/markets/utils"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/xerrors"
)

var (
	ErrDealNotFound        = fmt.Errorf("deal not found")
	ErrDealHandlerNotFound = errors.New("deal handler not found")
)

var (
	addPieceRetryWait    = 5 * time.Minute
	addPieceRetryTimeout = 6 * time.Hour
)

type Config struct {
	MaxTransferDuration time.Duration
}

var log = logging.Logger("boost-provider")

type Provider struct {
	testMode bool

	config Config
	// Address of the provider on chain.
	Address address.Address

	ctx       context.Context
	cancel    context.CancelFunc
	closeSync sync.Once
	wg        sync.WaitGroup

	newDealPS *newDealPS

	// filestore for manipulating files on disk.
	fs filestore.FileStore

	// event loop
	acceptDealChan    chan acceptDealReq
	finishedDealChan  chan finishedDealReq
	publishedDealChan chan publishDealReq
	storageSpaceChan  chan storageSpaceDealReq

	// Sealing Pipeline API
	sps sealingpipeline.API

	// Boost deal filter
	df dtypes.StorageDealFilter

	// Database API
	db        *sql.DB
	dealsDB   *db.DealsDB
	logsSqlDB *sql.DB
	logsDB    *db.LogsDB

	Transport      transport.Transport
	fundManager    *fundmanager.FundManager
	storageManager *storagemanager.StorageManager
	dealPublisher  types.DealPublisher
	transfers      *dealTransfers

	pieceAdder                  types.PieceAdder
	maxDealCollateralMultiplier uint64
	chainDealManager            types.ChainDealManager

	fullnodeApi v1api.FullNode

	dhsMu sync.RWMutex
	dhs   map[uuid.UUID]*dealHandler

	dealLogger *logs.DealLogger

	dagst stores.DAGStoreWrapper
	ps    piecestore.PieceStore

	ip types.IndexProvider
}

func NewProvider(repoRoot string, h host.Host, sqldb *sql.DB, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, fullnodeApi v1api.FullNode, dp types.DealPublisher, addr address.Address, pa types.PieceAdder,
	sps sealingpipeline.API, cm types.ChainDealManager, df dtypes.StorageDealFilter, logsSqlDB *sql.DB, logsDB *db.LogsDB,
	dagst stores.DAGStoreWrapper, ps piecestore.PieceStore, ip types.IndexProvider, httpOpts ...httptransport.Option) (*Provider, error) {
	fspath := path.Join(repoRoot, storagemanager.StagingAreaDirName)
	err := os.MkdirAll(fspath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	fs, err := filestore.NewLocalFileStore(filestore.OsPath(fspath))
	if err != nil {
		return nil, err
	}

	newDealPS, err := newDealPubsub()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	dl := logs.NewDealLogger(logsDB)

	return &Provider{
		ctx:    ctx,
		cancel: cancel,
		// TODO Make this configurable
		config:    Config{MaxTransferDuration: 24 * 3600 * time.Second},
		Address:   addr,
		newDealPS: newDealPS,
		fs:        fs,
		db:        sqldb,
		dealsDB:   dealsDB,
		logsSqlDB: logsSqlDB,
		sps:       sps,
		df:        df,

		acceptDealChan:    make(chan acceptDealReq),
		finishedDealChan:  make(chan finishedDealReq),
		publishedDealChan: make(chan publishDealReq),
		storageSpaceChan:  make(chan storageSpaceDealReq),

		Transport:      httptransport.New(h, dl, httpOpts...),
		fundManager:    fundMgr,
		storageManager: storageMgr,

		dealPublisher:               dp,
		fullnodeApi:                 fullnodeApi,
		pieceAdder:                  pa,
		chainDealManager:            cm,
		maxDealCollateralMultiplier: 2,
		transfers:                   newDealTransfers(),

		dhs:        make(map[uuid.UUID]*dealHandler),
		dealLogger: dl,
		logsDB:     logsDB,

		dagst: dagst,
		ps:    ps,

		ip: ip,
	}, nil
}

func (p *Provider) Deal(ctx context.Context, dealUuid uuid.UUID) (*types.ProviderDealState, error) {
	deal, err := p.dealsDB.ByID(ctx, dealUuid)
	if xerrors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("getting deal %s: %w", dealUuid, ErrDealNotFound)
	}
	return deal, nil
}

func (p *Provider) NBytesReceived(dealUuid uuid.UUID) uint64 {
	return p.transfers.getBytes(dealUuid)
}

func (p *Provider) GetAsk() *storagemarket.StorageAsk {
	return &storagemarket.StorageAsk{
		Price:         abi.NewTokenAmount(0),
		VerifiedPrice: abi.NewTokenAmount(0),
		MinPieceSize:  0,
		MaxPieceSize:  64 * 1024 * 1024 * 1024,
		Miner:         p.Address,
	}
}

// MakeOfflineDealWithData is called when the Storage Provider imports data for
// an offline deal (the deal must already have been proposed by the client)
func (p *Provider) MakeOfflineDealWithData(dealUuid uuid.UUID, filePath string) (pi *api.ProviderDealRejectionInfo, handler *dealHandler, err error) {
	p.dealLogger.Infow(dealUuid, "import data for offline deal", "filepath", filePath)

	// db should already have a deal with this uuid as the deal proposal should have been agreed before hand
	ds, err := p.dealsDB.ByID(p.ctx, dealUuid)
	if err != nil {
		if xerrors.Is(err, sql.ErrNoRows) {
			return nil, nil, fmt.Errorf("no pre-existing deal proposal for offline deal %s: %w", dealUuid, err)
		}
		return nil, nil, fmt.Errorf("getting offline deal %s: %w", dealUuid, err)
	}
	if !ds.IsOffline {
		return nil, nil, fmt.Errorf("deal %s is not an offline deal", dealUuid)
	}
	if ds.Checkpoint > dealcheckpoints.Accepted {
		return nil, nil, fmt.Errorf("deal %s has already been imported and reached checkpoint %s", dealUuid, ds.Checkpoint)
	}

	ds.InboundFilePath = filePath

	// get the deal handler for the deal
	dh := p.getDealHandler(dealUuid)
	if dh == nil {
		// deal handlers are created for each active deal on startup, so if
		// there is no deal handler then the deal is not active
		return nil, nil, fmt.Errorf("deal %s is no longer active (deal is at checkpoint %s)", dealUuid, ds.Checkpoint)
	}

	// setup clean-up code
	cleanup := func() {
		dh.close()
		p.delDealHandler(dealUuid)
	}

	resp, err := p.checkForDealAcceptance(ds, dh)
	if err != nil {
		cleanup()
		p.dealLogger.LogError(dealUuid, "failed to send deal for acceptance", err)
		return nil, nil, fmt.Errorf("failed to send deal for acceptance: %w", err)
	}

	// if there was an error, we return no rejection reason as well.
	if resp.err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("failed to accept deal: %w", resp.err)
	}

	// return rejection reason as provider has rejected the deal.
	if !resp.ri.Accepted {
		cleanup()
		p.dealLogger.Infow(dealUuid, "deal execution rejected by provider", "reason", resp.ri.Reason)
		return resp.ri, nil, nil
	}

	p.dealLogger.Infow(dealUuid, "deal accepted and scheduled for execution")
	return resp.ri, dh, nil
}

// ExecuteDeal is called when the Storage Provider receives a deal proposal
// from the network
func (p *Provider) ExecuteDeal(dp *types.DealParams, clientPeer peer.ID) (*api.ProviderDealRejectionInfo, *dealHandler, error) {
	p.dealLogger.Infow(dp.DealUUID, "executing deal proposal received from network", "peer", clientPeer)

	ds := types.ProviderDealState{
		DealUuid:           dp.DealUUID,
		ClientDealProposal: dp.ClientDealProposal,
		ClientPeerID:       clientPeer,
		DealDataRoot:       dp.DealDataRoot,
		Transfer:           dp.Transfer,
		IsOffline:          dp.IsOffline,
	}
	// validate the deal proposal
	if !p.testMode {
		if err := p.validateDealProposal(ds); err != nil {
			p.dealLogger.Infow(dp.DealUUID, "deal proposal failed validation", "err", err.Error())

			return &api.ProviderDealRejectionInfo{
				Reason: fmt.Sprintf("failed validation: %s", err),
			}, nil, nil
		}
	}

	if dp.IsOffline {
		return p.processOfflineDealProposal(ds)
	}

	return p.executeOnlineDeal(ds)
}

// processOfflineDealProposal saves the deal to the database and then stops execution.
// Execution resumes when MakeOfflineDealWithData is called.
func (p *Provider) processOfflineDealProposal(ds smtypes.ProviderDealState) (*api.ProviderDealRejectionInfo, *dealHandler, error) {
	// Save deal to DB
	ds.CreatedAt = time.Now()
	ds.Checkpoint = dealcheckpoints.Accepted
	if err := p.dealsDB.Insert(p.ctx, &ds); err != nil {
		return nil, nil, fmt.Errorf("failed to insert deal in db: %w", err)
	}

	// Set up pubsub for deal updates
	dh := p.mkAndInsertDealHandler(ds.DealUuid)
	pub, err := dh.bus.Emitter(&types.ProviderDealState{}, eventbus.Stateful)
	if err != nil {
		err = fmt.Errorf("failed to create event emitter: %w", err)
		p.failDeal(pub, &ds, err)
		p.cleanupDealLogged(&ds)
		return nil, nil, err
	}

	// publish "new deal" event
	p.fireEventDealNew(&ds)
	// publish an event with the current state of the deal
	p.fireEventDealUpdate(pub, &ds)

	p.dealLogger.Infow(ds.DealUuid, "offline deal accepted, waiting for data import")

	return &api.ProviderDealRejectionInfo{Accepted: true}, nil, nil
}

// executeOnlineDeal sets up a download location for the deal, then sends the
// deal to the main provider loop for execution
func (p *Provider) executeOnlineDeal(ds smtypes.ProviderDealState) (*api.ProviderDealRejectionInfo, *dealHandler, error) {
	dh := p.mkAndInsertDealHandler(ds.DealUuid)
	tmpFile, err := p.fs.CreateTemp()
	ri, err := func() (*api.ProviderDealRejectionInfo, error) {
		// create a temp file where we will hold the deal data.
		if err != nil {
			p.dealLogger.LogError(ds.DealUuid, "failed to create temp file for inbound data transfer", err)
			return nil, fmt.Errorf("failed to create temp file: %w", err)
		}
		if err := tmpFile.Close(); err != nil {
			p.dealLogger.LogError(ds.DealUuid, "failed to close temp file created for inbound data transfer", err)
			return nil, fmt.Errorf("failed to close temp file: %w", err)
		}

		// send the deal to the main provider loop for execution
		ds.InboundFilePath = string(tmpFile.OsPath())
		resp, err := p.checkForDealAcceptance(&ds, dh)
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
		// if there was an error processing the deal, or the deal was rejected,
		// clean up the tmp file and deal handler
		if tmpFile != nil {
			_ = os.Remove(string(tmpFile.OsPath()))
		}
		dh.close()
		p.delDealHandler(ds.DealUuid)
		return ri, nil, err
	}

	p.dealLogger.Infow(ds.DealUuid, "deal accepted and scheduled for execution")
	return ri, dh, nil
}

func (p *Provider) checkForDealAcceptance(ds *types.ProviderDealState, dh *dealHandler) (acceptDealResp, error) {
	// send message to event loop to run the deal through the acceptance filter and reserve the required resources
	// then wait for a response and return the response to the client.
	respChan := make(chan acceptDealResp, 1)
	select {
	case p.acceptDealChan <- acceptDealReq{rsp: respChan, deal: ds, dh: dh}:
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

func (p *Provider) mkAndInsertDealHandler(dealUuid uuid.UUID) *dealHandler {
	// Create a deal handler
	bus := eventbus.NewBus()

	transferCtx, cancel := context.WithCancel(p.ctx)
	dh := &dealHandler{
		providerCtx: p.ctx,
		dealUuid:    dealUuid,
		bus:         bus,

		transferCtx:    transferCtx,
		transferCancel: cancel,
		transferDone:   make(chan error, 1),
	}

	p.dhsMu.Lock()
	defer p.dhsMu.Unlock()
	p.dhs[dealUuid] = dh
	return dh
}

func (p *Provider) Start() ([]*dealHandler, error) {
	log.Infow("storage provider: starting")

	// initialize the database
	err := db.CreateAllBoostTables(p.ctx, p.db, p.logsSqlDB)
	if err != nil {
		return nil, fmt.Errorf("failed to init db: %w", err)
	}
	log.Infow("db initialized")

	// cleanup all completed deals in case Boost resumed before they were cleanedup
	finished, err := p.dealsDB.ListCompleted(p.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list completed deals: %w", err)
	}
	for i := range finished {
		p.cleanupDealOnRestart(finished[i])
	}
	log.Info("finished cleaning up completed deals")

	// restart all active deals
	pds, err := p.dealsDB.ListActive(p.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list active deals: %w", err)
	}

	// cleanup all deals that have finished successfully
	for i := range pds {
		deal := pds[i]
		// TODO Update this once we start listening for expired/slashed deals etc
		if deal.Checkpoint >= dealcheckpoints.IndexedAndAnnounced {
			// cleanup if cleanup didn't finish before we restarted
			p.cleanupDealOnRestart(deal)
		}
	}

	// resume all in-progress deals
	var dhs []*dealHandler
	for _, d := range pds {
		d := d
		dh := p.mkAndInsertDealHandler(d.DealUuid)
		p.wg.Add(1)
		dhs = append(dhs, dh)

		go func() {
			defer p.wg.Done()

			// If it's an offline deal, and the deal data hasn't yet been
			// imported, just wait for the SP operator to import the data
			if d.IsOffline && d.InboundFilePath == "" {
				p.dealLogger.Infow(d.DealUuid, "restarted deal: waiting for offline deal data import")
				return
			}

			// Check if deal is already proving
			if d.Checkpoint >= dealcheckpoints.IndexedAndAnnounced {
				si, err := p.sps.SectorsStatus(p.ctx, d.SectorID, false)
				if err != nil || isFinalSealingState(si.State) {
					return
				}
			}

			p.dealLogger.Infow(d.DealUuid, "resuming deal on boost restart", "checkpoint on resumption", d.Checkpoint.String())
			p.doDeal(d, dh)
			log.Infow("finished running deal", "id", d.DealUuid)
		}()
	}

	p.wg.Add(1)
	go p.loop()
	go p.transfers.start(p.ctx)

	log.Infow("storage provider: started")
	return dhs, nil
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
		p.dealLogger.Infow(deal.DealUuid, "untagged funds for deal as deal finished", "untagged publish", pub, "untagged collateral", collat,
			"err", errf)
	}
}

func (p *Provider) Stop() {
	p.closeSync.Do(func() {
		log.Infow("storage provider: stopping")

		deals, err := p.dealsDB.ListActive(p.ctx)
		if err == nil {
			for i := range deals {
				dl := deals[i]
				if dl.Checkpoint < dealcheckpoints.AddedPiece {
					log.Infow("shutting down running deal", "id", dl.DealUuid.String(), "ckp", dl.Checkpoint.String())
				}
			}
		}

		p.cancel()
		log.Infow("stopping provider event loop")
		p.wg.Wait()
		log.Info("provider shutdown complete")
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

func (p *Provider) getDealHandler(id uuid.UUID) *dealHandler {
	p.dhsMu.RLock()
	defer p.dhsMu.RUnlock()

	return p.dhs[id]
}

func (p *Provider) delDealHandler(dealUuid uuid.UUID) {
	p.dhsMu.Lock()
	delete(p.dhs, dealUuid)
	p.dhsMu.Unlock()
}

func (p *Provider) AddPieceToSector(ctx context.Context, deal smtypes.ProviderDealState, pieceData io.Reader) (*storagemarket.PackingResult, error) {
	// Sanity check - we must have published the deal before handing it off
	// to the sealing subsystem
	if deal.PublishCID == nil {
		return nil, xerrors.Errorf("deal.PublishCid can't be nil")
	}

	sdInfo := lapi.PieceDealInfo{
		DealID:       deal.ChainDealID,
		DealProposal: &deal.ClientDealProposal.Proposal,
		PublishCid:   deal.PublishCID,
		DealSchedule: lapi.DealSchedule{
			StartEpoch: deal.ClientDealProposal.Proposal.StartEpoch,
			EndEpoch:   deal.ClientDealProposal.Proposal.EndEpoch,
		},
		// Assume that it doesn't make sense for a miner not to keep an
		// unsealed copy. TODO: Check that's a valid assumption.
		//KeepUnsealed: deal.FastRetrieval,
		KeepUnsealed: true,
	}

	// Attempt to add the piece to a sector (repeatedly if necessary)
	pieceSize := deal.ClientDealProposal.Proposal.PieceSize.Unpadded()
	sectorNum, offset, err := p.pieceAdder.AddPiece(ctx, pieceSize, pieceData, sdInfo)
	curTime := build.Clock.Now()

	for build.Clock.Since(curTime) < addPieceRetryTimeout {
		if !xerrors.Is(err, sealing.ErrTooManySectorsSealing) {
			if err != nil {
				p.dealLogger.Warnw(deal.DealUuid, "failed to addPiece for deal, will-retry", "err", err.Error())
			}
			break
		}
		select {
		case <-build.Clock.After(addPieceRetryWait):
			sectorNum, offset, err = p.pieceAdder.AddPiece(ctx, pieceSize, pieceData, sdInfo)
		case <-ctx.Done():
			return nil, fmt.Errorf("error while waiting to retry AddPiece: %w", ctx.Err())
		}
	}

	if err != nil {
		return nil, fmt.Errorf("AddPiece failed: %w", err)
	}
	p.dealLogger.Infow(deal.DealUuid, "added new deal to sector", "sector", sectorNum.String())

	return &storagemarket.PackingResult{
		SectorNumber: sectorNum,
		Offset:       offset,
		Size:         pieceSize.Padded(),
	}, nil
}

func (p *Provider) VerifySignature(ctx context.Context, sig crypto.Signature, addr address.Address, input []byte, encodedTs shared.TipSetToken) (bool, error) {
	addr, err := p.fullnodeApi.StateAccountKey(ctx, addr, ctypes.EmptyTSK)
	if err != nil {
		return false, err
	}

	err = sigs.Verify(&sig, addr, input)
	return err == nil, err
}

func (p *Provider) GetBalance(ctx context.Context, addr address.Address, encodedTs shared.TipSetToken) (storagemarket.Balance, error) {
	tsk, err := ctypes.TipSetKeyFromBytes(encodedTs)
	if err != nil {
		return storagemarket.Balance{}, err
	}

	bal, err := p.fullnodeApi.StateMarketBalance(ctx, addr, tsk)
	if err != nil {
		return storagemarket.Balance{}, err
	}

	return utils.ToSharedBalance(bal), nil
}

type CurrentDealInfo struct {
	DealID           abi.DealID
	MarketDeal       *lapi.MarketDeal
	PublishMsgTipSet ctypes.TipSetKey
}
