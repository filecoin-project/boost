package storagemarket

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/filecoin-project/boost/filestore"
	"github.com/filecoin-project/lotus/api/v1api"

	"github.com/libp2p/go-eventbus"

	"github.com/libp2p/go-libp2p-core/event"

	"github.com/filecoin-project/boost/storagemarket/types/dealcheckpoints"

	"github.com/filecoin-project/boost/storagemarket/datatransfer"

	"github.com/filecoin-project/go-address"

	"github.com/filecoin-project/boost/storagemarket/types"
)

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

	// filestore for manipulating files on disk.
	fs filestore.FileStore

	// event loop
	acceptDealsChan  chan acceptDealReq
	failedDealsChan  chan failedDealReq
	restartDealsChan chan restartReq

	// Database API
	//dbApi datastore.API

	fullnodeApi v1api.FullNode
	//dagStore    stores.DAGStoreWrapper

	transport datatransfer.Transport

	adapter *Adapter
}

//func NewProvider(dbApi datastore.API, lotusNode lotusnode.StorageProviderNode) (*provider, error) {
func NewProvider(fullnodeApi v1api.FullNode) (*Provider, error) {
	ctx, cancel := context.WithCancel(context.Background())

	return &Provider{
		ctx:         ctx,
		cancel:      cancel,
		fullnodeApi: fullnodeApi,
		//dbApi:     dbApi,

		adapter: &Adapter{}, // TODO: instantiate properly
	}, nil
}

func (p *Provider) GetAsk() *types.StorageAsk {
	return nil
}

func (p *Provider) ExecuteDeal(dp *types.ClientDealParams) (dh *DealHandler, pi *types.ProviderDealRejectionInfo, err error) {
	if _, err := url.Parse(dp.TransferURL); err != nil {
		return nil, nil, fmt.Errorf("transfer url is invalid: %w", err)
	}

	ds := types.ProviderDealState{
		DealUuid:           dp.DealUuid,
		ClientDealProposal: dp.ClientDealProposal,
		SelfPeerID:         dp.MinerPeerID,
		ClientPeerID:       dp.ClientPeerID,
		DealDataRoot:       dp.DealDataRoot,
		TransferURL:        dp.TransferURL,
	}

	// validate the deal proposal
	if err := p.validateDealProposal(ds); err != nil {
		return nil, &types.ProviderDealRejectionInfo{
			Reason: fmt.Sprintf("failed validation: %s", err),
		}, err
	}

	// create a temp file where we will hold the deal data.
	tmp, err := p.fs.CreateTemp()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(string(tmp.OsPath()))
		return nil, nil, fmt.Errorf("failed to close temp file: %w", err)
	}
	ds.InboundCARPath = string(tmp.OsPath())

	// create the pub-sub plumbing for this deal
	bus := eventbus.NewBus()
	publisher, sub, err := createPubSub(bus)
	if err != nil {
		_ = os.Remove(ds.InboundCARPath)
		return nil, nil, err
	}

	defer func() {
		if pi != nil || err != nil {
			_ = os.Remove(ds.InboundCARPath)
			_ = sub.Close()
		}
	}()

	// send message to event loop to run the deal through the acceptance filter and reserve the required resources
	// then wait for a response and return the response to the client.
	respChan := make(chan acceptDealResp, 1)
	select {
	case p.acceptDealsChan <- acceptDealReq{&ds, respChan, publisher}:
	case <-p.ctx.Done():
		return nil, nil, p.ctx.Err()
	}

	var resp acceptDealResp
	select {
	case resp = <-respChan:
	case <-p.ctx.Done():
		return nil, nil, p.ctx.Err()
	}

	// if there was an error, we return no rejection reason as well.
	if resp.err != nil {
		return nil, nil, fmt.Errorf("failed to accept deal: %w", resp.err)
	}
	// return rejection reason as provider has rejected a valid deal.
	if !resp.accepted {
		return nil, resp.ri, nil
	}

	dh = newDealHandler(dp.DealUuid, sub)
	return dh, nil, nil
}

func createPubSub(bus event.Bus) (event.Emitter, event.Subscription, error) {
	emitter, err := bus.Emitter(&types.ProviderDealEvent{}, eventbus.Stateful)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create event emitter: %w", err)
	}
	sub, err := bus.Subscribe(new(types.ProviderDealEvent), eventbus.BufSize(256))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create subscriber: %w", err)
	}

	return emitter, sub, nil
}

func (p *Provider) Start() []*DealHandler {
	// restart all existing deals
	// execute db query to get all non-terminated deals here
	var deals []*types.ProviderDealState
	var restartWg sync.WaitGroup
	dhs := make([]*DealHandler, 0, len(deals))

	for i := range deals {
		pub, sub, err := createPubSub(eventbus.NewBus())
		if err != nil {
			panic(err)
		}

		deal := deals[i]
		req := restartReq{deal, pub}

		restartWg.Add(1)
		go func() {
			defer restartWg.Done()

			select {
			case p.restartDealsChan <- req:
			case <-p.ctx.Done():
			}
		}()

		dhs = append(dhs, newDealHandler(deal.DealUuid, sub))
	}

	p.wg.Add(1)
	go p.loop()

	// wait for all deals to be restarted before returning so we know new deals will be processed
	// after all existing deals have restarted and accounted for their resources.
	restartWg.Wait()

	return dhs
}

func (p *Provider) Close() error {
	p.closeSync.Do(func() {
		p.cancel()
		p.wg.Wait()
	})
	return nil
}

type acceptDealReq struct {
	st        *types.ProviderDealState
	rsp       chan acceptDealResp
	publisher event.Emitter
}

type acceptDealResp struct {
	accepted bool
	ri       *types.ProviderDealRejectionInfo
	err      error
}

type failedDealReq struct {
	st  *types.ProviderDealState
	err error
}

type restartReq struct {
	st        *types.ProviderDealState
	publisher event.Emitter
}

// TODO: This is transient -> If it dosen't work out, we will use locks.
// 1:N will move this problem elsewhere.
func (p *Provider) loop() {
	defer p.wg.Done()

	for {
		select {
		case restartReq := <-p.restartDealsChan:
			// Put ANY RESTART SYNCHRONIZATION LOGIC HERE.
			// ....
			//
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				p.doDeal(restartReq.st, restartReq.publisher)
			}()

		case dealReq := <-p.acceptDealsChan:
			writeDealResp := func(accepted bool, ri *types.ProviderDealRejectionInfo, err error) {
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
			var accepted bool
			if !accepted {
				go writeDealResp(false, &types.ProviderDealRejectionInfo{}, nil)
				continue
			}
			go writeDealResp(true, nil, nil)

			// start executing the deal
			dealReq.st.Checkpoint = dealcheckpoints.New

			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				p.doDeal(dealReq.st, dealReq.publisher)
			}()

		case failedDeal := <-p.failedDealsChan:
			fmt.Println(failedDeal)
			// Release storage space , funds, shared resources etc etc.

		case <-p.ctx.Done():
			return
		}
	}
}
