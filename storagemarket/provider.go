package storagemarket

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/filecoin-project/boost/transport"
	"github.com/filecoin-project/boost/transport/httptransport"

	"github.com/filecoin-project/boost/api"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/filestore"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/event"
)

var log = logging.Logger("provider")

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
	db *db.DealsDB

	Transport transport.Transport

	dealPublisher *DealPublisher
	adapter       *Adapter

	dealHandlers *dealHandlers
}

func NewProvider(repoRoot string, dealsDB *db.DealsDB, fullnodeApi v1api.FullNode, dealPublisher *DealPublisher, addr address.Address) (*Provider, error) {
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
		// TODO: pass max transfer duration as a param
		config:    Config{MaxTransferDuration: 30 * time.Second},
		Address:   addr,
		newDealPS: newDealPS,
		fs:        fs,
		db:        dealsDB,

		acceptDealsChan:  make(chan acceptDealReq),
		failedDealsChan:  make(chan failedDealReq),
		restartDealsChan: make(chan restartReq),

		Transport: httptransport.New(),

		dealPublisher: dealPublisher,
		adapter: &Adapter{
			FullNode: fullnodeApi,
		},

		dealHandlers: newDealHandlers(),
	}, nil
}

func (p *Provider) NBytesReceived(deal *types.ProviderDealState) (int64, error) {
	fi, err := os.Stat(deal.InboundFilePath)
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
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
		}, err
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

	defer func() {
		if pi != nil || err != nil {
			_ = os.Remove(ds.InboundFilePath)
		}
	}()

	// send message to event loop to run the deal through the acceptance filter and reserve the required resources
	// then wait for a response and return the response to the client.
	respChan := make(chan acceptDealResp, 1)
	select {
	case p.acceptDealsChan <- acceptDealReq{rsp: respChan, deal: &ds}:
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}

	var resp acceptDealResp
	select {
	case resp = <-respChan:
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
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

	return nil, nil
}

func (p *Provider) Start(ctx context.Context) error {
	log.Infow("storage provider: starting")

	p.ctx, p.cancel = context.WithCancel(ctx)

	// initialize the database
	err := p.db.Init(p.ctx)
	if err != nil {
		return err
	}

	// restart all existing deals
	deals, err := p.db.ListActive(p.ctx)
	if err != nil {
		return xerrors.Errorf("getting active deals: %w", err)
	}

	var restartWg sync.WaitGroup
	for _, deal := range deals {
		deal := deal
		restartWg.Add(1)
		go func() {
			defer restartWg.Done()

			select {
			case p.restartDealsChan <- restartReq{deal: deal}:
			case <-p.ctx.Done():
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

var ErrDealNotFound = fmt.Errorf("deal not found")

func (p *Provider) Deal(ctx context.Context, dealUuid uuid.UUID) (*types.ProviderDealState, error) {
	deal, err := p.db.ByID(ctx, dealUuid)
	if xerrors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("getting deal %s: %w", dealUuid, ErrDealNotFound)
	}
	return deal, nil
}

type acceptDealReq struct {
	rsp  chan acceptDealResp
	deal *types.ProviderDealState
}

type acceptDealResp struct {
	accepted bool
	ri       *api.ProviderDealRejectionInfo
	err      error
}

type failedDealReq struct {
	st  *types.ProviderDealState
	err error
}

type restartReq struct {
	deal *types.ProviderDealState
}

// TODO: This is transient -> If it dosen't work out, we will use locks.
// 1:N will move this problem elsewhere.
func (p *Provider) loop() {
	defer p.wg.Done()

	for {
		select {
		case restartReq := <-p.restartDealsChan:
			log.Infow("restart deal", "id", restartReq.deal.DealUuid)

			// Put ANY RESTART SYNCHRONIZATION LOGIC HERE.
			// ....
			//
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()

				p.doDeal(restartReq.deal)
			}()

		case dealReq := <-p.acceptDealsChan:
			deal := dealReq.deal
			log.Infow("process accept deal request", "id", deal.DealUuid)

			writeDealResp := func(accepted bool, ri *api.ProviderDealRejectionInfo, err error) {
				select {
				case dealReq.rsp <- acceptDealResp{accepted, ri, err}:
				case <-p.ctx.Done():
					return
				}
			}

			var err error
			if err != nil {
				go writeDealResp(false, nil, err)
				continue
			}

			// TODO: Deal filter, storage space manager, fund manager etc . basically synchronization
			// send rejection if deal is not accepted by the above filters
			accepted := true
			if !accepted {
				go writeDealResp(false, &api.ProviderDealRejectionInfo{}, nil)
				continue
			}
			go writeDealResp(true, nil, nil)

			// write deal state to the database
			log.Infow("insert deal into DB", "id", deal.DealUuid)

			deal.CreatedAt = time.Now()
			deal.Checkpoint = dealcheckpoints.New

			err = p.db.Insert(p.ctx, deal)
			if err != nil {
				go writeDealResp(false, nil, err)
				continue
			}

			// start executing the deal
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()

				p.doDeal(deal)
			}()

		case failedDeal := <-p.failedDealsChan:
			log.Errorw("deal failed", "id", failedDeal.st.DealUuid, "err", failedDeal.err)
			// Release storage space , funds, shared resources etc etc.

		case <-p.ctx.Done():
			return
		}
	}
}
