package storagemarket

import (
	"context"
	"sync"
	"time"

	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"

	"github.com/filecoin-project/boost/storagemarket/fundmanager"

	"github.com/filecoin-project/boost/storagemarket/sqlstore"
)

type provider struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeSync sync.Once
	wg        sync.WaitGroup

	// Database API
	dbApi sqlstore.API

	// interacts with lotus
	lotusNode types.StorageProviderNode

	fundManager fundmanager.Manager

	// storage provider vars
	Address address.Address
}

func NewProvider(dbApi sqlstore.API, lotusNode types.StorageProviderNode, fundManager fundmanager.Manager) (*provider, error) {
	return &provider{}, nil
}

func (p *provider) GetAsk() *types.StorageAsk {
	return nil
}

func (p *provider) ExecuteDeal(dp types.ClientDealParams) (dh *DealHandler, backOff time.Duration, err error) {
	ds := types.ProviderDealState{
		ClientDealProposal: dp.ClientDealProposal,
		DealUuid:           dp.DealUuid,
		Miner:              dp.Miner,
		Client:             dp.Client,

		PieceCid:     dp.PieceCid,
		DealDataRoot: dp.DealDataRoot,
		PieceSize:    dp.PieceSize,
	}

	// validate the deal proposal
	if err := p.validateDealProposal(ds); err != nil {
		return nil, 0, err
	}

	// send message to the event loop and apply the deal acceptance filter and reserve funds atomically.

	// wait for response from event loop
	return &DealHandler{}, 0, nil
}

func (p *provider) Close() error {
	p.closeSync.Do(func() {
		p.cancel()
		p.wg.Wait()
	})
	return nil
}

func (p *provider) Start() {
	p.wg.Add(1)
	go p.loop()
}
