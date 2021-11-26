package storagemarket

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/filestore"
	"github.com/filecoin-project/boost/fundmanager"
	"github.com/filecoin-project/boost/storage/sectorblocks"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/transport"
	"github.com/filecoin-project/boost/transport/httptransport"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/event"
	"golang.org/x/xerrors"
)

var log = logging.Logger("boost-provider")

var ErrDealNotFound = fmt.Errorf("deal not found")

type Config struct {
	MaxTransferDuration time.Duration
}

type Provider struct {
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
	acceptDealsChan  chan acceptDealReq
	failedDealsChan  chan failedDealReq
	restartDealsChan chan restartReq

	// Database API
	db      *sql.DB
	dealsDB *db.DealsDB

	Transport     transport.Transport
	fundManager   *fundmanager.FundManager
	dealPublisher *DealPublisher
	adapter       *Adapter

	dealHandlers *dealHandlers
}

func NewProvider(repoRoot string, sqldb *sql.DB, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, fullnodeApi v1api.FullNode, dealPublisher *DealPublisher, addr address.Address, secb *sectorblocks.SectorBlocks) (*Provider, error) {
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
		config:    Config{MaxTransferDuration: 30 * time.Second},
		Address:   addr,
		newDealPS: newDealPS,
		fs:        fs,
		db:        sqldb,
		dealsDB:   dealsDB,

		acceptDealsChan:  make(chan acceptDealReq),
		failedDealsChan:  make(chan failedDealReq),
		restartDealsChan: make(chan restartReq),

		Transport:   httptransport.New(),
		fundManager: fundMgr,

		dealPublisher: dealPublisher,
		adapter: &Adapter{
			FullNode: fullnodeApi,
			secb:     secb,
		},

		dealHandlers: newDealHandlers(),
	}, nil
}

func (p *Provider) Deal(ctx context.Context, dealUuid uuid.UUID) (*types.ProviderDealState, error) {
	deal, err := p.dealsDB.ByID(ctx, dealUuid)
	if xerrors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("getting deal %s: %w", dealUuid, ErrDealNotFound)
	}
	return deal, nil
}

func (p *Provider) NBytesReceived(deal *types.ProviderDealState) (int64, error) {
	fi, err := os.Stat(deal.InboundFilePath)
	if err != nil {
		return 0, nil
	}
	return fi.Size(), nil
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

func (p *Provider) ExecuteDeal(dp *types.ClientDealParams) (pi *api.ProviderDealRejectionInfo, err error) {
	log.Infow("execute deal", "id", dp.DealUuid)

	ds := types.ProviderDealState{
		DealUuid:           dp.DealUuid,
		ClientDealProposal: dp.ClientDealProposal,
		SelfPeerID:         dp.MinerPeerID,
		ClientPeerID:       dp.ClientPeerID,
		DealDataRoot:       dp.DealDataRoot,
		Transfer:           dp.Transfer,
	}

	// validate the deal proposal
	if err := p.validateDealProposal(ds); err != nil {
		return &api.ProviderDealRejectionInfo{
			Reason: fmt.Sprintf("failed validation: %s", err),
		}, nil
	}

	// create a temp file where we will hold the deal data.
	tmp, err := p.fs.CreateTemp()
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(string(tmp.OsPath()))
		return nil, fmt.Errorf("failed to close temp file: %w", err)
	}
	ds.InboundFilePath = string(tmp.OsPath())
	// make sure to remove the temp file if something goes wrong from here on.
	defer func() {
		if pi != nil || err != nil {
			_ = os.Remove(ds.InboundFilePath)
		}
	}()

	resp, err := p.checkForDealAcceptance(&ds)
	if err != nil {
		return nil, fmt.Errorf("failed to send deal for acceptance: %w", err)
	}

	// if there was an error, we return no rejection reason as well.
	if resp.err != nil {
		return nil, fmt.Errorf("failed to accept deal: %w", resp.err)
	}
	// return rejection reason as provider has rejected a valid deal.
	if !resp.accepted {
		log.Infow("rejected deal: "+resp.ri.Reason, "id", dp.DealUuid)
		return resp.ri, nil
	}

	log.Infow("scheduled deal for execution", "id", dp.DealUuid)
	return nil, nil
}

func (p *Provider) checkForDealAcceptance(ds *types.ProviderDealState) (acceptDealResp, error) {
	// send message to event loop to run the deal through the acceptance filter and reserve the required resources
	// then wait for a response and return the response to the client.
	respChan := make(chan acceptDealResp, 1)
	select {
	case p.acceptDealsChan <- acceptDealReq{rsp: respChan, deal: ds}:
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

func (p *Provider) Start(ctx context.Context) error {
	log.Infow("storage provider: starting")

	p.ctx, p.cancel = context.WithCancel(ctx)

	// initialize the database
	err := db.CreateTables(p.ctx, p.db)
	if err != nil {
		return fmt.Errorf("failed to init db: %w", err)
	}
	log.Infow("db initialized")

	// restart all existing deals
	deals, err := p.dealsDB.ListActive(p.ctx)
	if err != nil {
		return fmt.Errorf("getting active deals: %w", err)
	}

	var restartWg sync.WaitGroup
	for _, deal := range deals {
		ds := deal
		restartWg.Add(1)
		go func() {
			defer restartWg.Done()

			select {
			case p.restartDealsChan <- restartReq{deal: ds}:
			case <-p.ctx.Done():
				log.Errorw("timeout when restarting deal", "err", p.ctx.Err(), "id", ds.DealUuid)
			}
		}()
	}

	p.wg.Add(1)
	go p.loop()

	// wait for all deals to be restarted before returning so we know new deals will be processed
	// after all existing deals have restarted and accounted for their resources.
	restartWg.Wait()

	log.Infow("storage provider: started")
	return nil
}

func (p *Provider) Close() error {
	p.closeSync.Do(func() {
		p.cancel()
		p.wg.Wait()
	})
	return nil
}

// SubscribeNewDeals subscribes to "new deal" events
func (p *Provider) SubscribeNewDeals() (event.Subscription, error) {
	return p.newDealPS.subscribe()
}

// SubscribeNewDeals subscribes to updates to a deal
func (p *Provider) SubscribeDealUpdates(dealUuid uuid.UUID) (event.Subscription, error) {
	dh, err := p.dealHandlers.get(dealUuid)
	if err != nil {
		return nil, err
	}
	return dh.subscribeUpdates()
}

// CancelDeal cancels a deal and any associated data transfer
func (p *Provider) CancelDeal(ctx context.Context, dealUuid uuid.UUID) error {
	dh, err := p.dealHandlers.get(dealUuid)
	if err != nil {
		if xerrors.Is(err, ErrDealHandlerFound) {
			return nil
		}
		return err
	}
	dh.cancel(ctx)
	return nil
}
