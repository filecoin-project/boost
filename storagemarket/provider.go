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
	"github.com/filecoin-project/boost/sealingpipeline"
	"github.com/filecoin-project/boost/storagemanager"
	"github.com/filecoin-project/boost/storagemarket/types"
	smtypes "github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/transport"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-fil-markets/storagemarket"
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
	log                    = logging.Logger("boost-provider")
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

	// Sealing Pipeline API
	sps sealingpipeline.State

	// Database API
	db      *sql.DB
	dealsDB *db.DealsDB

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
}

func NewProvider(repoRoot string, h host.Host, sqldb *sql.DB, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, fullnodeApi v1api.FullNode,
	dp types.DealPublisher, addr address.Address, pa types.PieceAdder, sps sealingpipeline.State,
	cm types.ChainDealManager, httpOpts ...httptransport.Option) (*Provider, error) {
	fspath := path.Join(repoRoot, "incoming")
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

	return &Provider{
		config:    Config{MaxTransferDuration: 24 * 3600 * time.Second},
		Address:   addr,
		newDealPS: newDealPS,
		fs:        fs,
		db:        sqldb,
		dealsDB:   dealsDB,
		sps:       sps,

		acceptDealChan:    make(chan acceptDealReq),
		finishedDealChan:  make(chan finishedDealReq),
		publishedDealChan: make(chan publishDealReq),

		Transport:      httptransport.New(h, httpOpts...),
		fundManager:    fundMgr,
		storageManager: storageMgr,

		dealPublisher:               dp,
		fullnodeApi:                 fullnodeApi,
		pieceAdder:                  pa,
		chainDealManager:            cm,
		maxDealCollateralMultiplier: 2,
		transfers:                   newDealTransfers(),

		dhs: make(map[uuid.UUID]*dealHandler),
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

func (p *Provider) GetAsk() *types.StorageAsk {
	return &types.StorageAsk{
		Price:         abi.NewTokenAmount(1),
		VerifiedPrice: abi.NewTokenAmount(1),
		MinPieceSize:  0,
		MaxPieceSize:  64 * 1024 * 1024 * 1024,
		Miner:         p.Address,
	}
}

func (p *Provider) ExecuteDeal(dp *types.DealParams, clientPeer peer.ID) (pi *api.ProviderDealRejectionInfo, err error) {
	log.Infow("execute deal", "uuid", dp.DealUUID)

	ds := types.ProviderDealState{
		DealUuid:           dp.DealUUID,
		ClientDealProposal: dp.ClientDealProposal,
		ClientPeerID:       clientPeer,
		DealDataRoot:       dp.DealDataRoot,
		Transfer:           dp.Transfer,
	}
	// validate the deal proposal
	if !p.testMode {
		if err := p.validateDealProposal(ds); err != nil {
			return &api.ProviderDealRejectionInfo{
				Reason: fmt.Sprintf("failed validation: %s", err),
			}, nil
		}
	}

	// setup clean-up code
	var tmpFile filestore.File
	var dh *dealHandler
	cleanup := func() {
		if tmpFile != nil {
			_ = os.Remove(string(tmpFile.OsPath()))
		}
		if dh != nil {
			dh.close()
			p.delDealHandler(dp.DealUUID)
		}
	}

	// create a temp file where we will hold the deal data.
	tmpFile, err = p.fs.CreateTemp()
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		cleanup()
		return nil, fmt.Errorf("failed to close temp file: %w", err)
	}

	ds.InboundFilePath = string(tmpFile.OsPath())
	dh = p.mkAndInsertDealHandler(dp.DealUUID)
	resp, err := p.checkForDealAcceptance(&ds, dh)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("failed to send deal for acceptance: %w", err)
	}

	// if there was an error, we return no rejection reason as well.
	if resp.err != nil {
		cleanup()
		return nil, fmt.Errorf("failed to accept deal: %w", resp.err)
	}
	// return rejection reason as provider has rejected the deal.
	if !resp.ri.Accepted {
		cleanup()
		log.Infow("rejected deal: "+resp.ri.Reason, "id", dp.DealUUID)
		return resp.ri, nil
	}

	log.Infow("scheduled deal for execution", "id", dp.DealUUID)
	return resp.ri, nil
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

func (p *Provider) Start(ctx context.Context) error {
	log.Infow("storage provider: starting")

	p.ctx, p.cancel = context.WithCancel(ctx)

	// initialize the database
	err := db.CreateTables(p.ctx, p.db)
	if err != nil {
		return fmt.Errorf("failed to init db: %w", err)
	}
	log.Infow("db initialized")

	// restart all active deals
	pds, err := p.dealsDB.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("failed to list active deals: %w", err)
	}
	for _, ds := range pds {
		d := ds
		dh := p.mkAndInsertDealHandler(d.DealUuid)
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.doDeal(d, dh)
		}()
	}

	p.wg.Add(1)
	go p.loop()
	go p.transfers.start(p.ctx)

	log.Infow("storage provider: started")
	return nil
}

func (p *Provider) Stop() {
	p.closeSync.Do(func() {
		log.Infow("storage provider: stopping")

		p.cancel()
		p.wg.Wait()
	})
}

// SubscribeNewDeals subscribes to "new deal" events
func (p *Provider) SubscribeNewDeals() (event.Subscription, error) {
	return p.newDealPS.subscribe()
}

// SubscribeNewDeals subscribes to updates to a deal
func (p *Provider) SubscribeDealUpdates(dealUuid uuid.UUID) (event.Subscription, error) {
	dh := p.getDealHandler(dealUuid)
	if dh == nil {
		return nil, ErrDealHandlerNotFound
	}

	return dh.subscribeUpdates()
}

func (p *Provider) CancelDeal(dealUuid uuid.UUID) error {
	dh := p.getDealHandler(dealUuid)
	if dh == nil {
		return ErrDealHandlerNotFound
	}

	return dh.cancel()
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
				log.Errorw("failed to addPiece for deal", "id", deal.DealUuid, "err", err)
			}
			break
		}
		select {
		case <-build.Clock.After(addPieceRetryWait):
			sectorNum, offset, err = p.pieceAdder.AddPiece(ctx, pieceSize, pieceData, sdInfo)
		case <-ctx.Done():
			return nil, xerrors.New("context expired while waiting to retry AddPiece")
		}
	}

	if err != nil {
		return nil, xerrors.Errorf("AddPiece failed: %s", err)
	}
	log.Infow("Added new deal to sector", "id", deal.DealUuid, "sector", p)

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
