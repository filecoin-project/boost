package storagemarket

import (
	"bytes"
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
	"github.com/filecoin-project/boost/storage/sectorblocks"
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
	"github.com/filecoin-project/go-state-types/exitcode"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	ctypes "github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/extern/storage-sealing"
	"github.com/filecoin-project/lotus/lib/sigs"
	"github.com/filecoin-project/lotus/markets/utils"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"golang.org/x/xerrors"
)

var (
	log = logging.Logger("boost-provider")

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
	dealPublisher  *DealPublisher
	transfers      *dealTransfers

	secb                        *sectorblocks.SectorBlocks
	maxDealCollateralMultiplier uint64

	fullnodeApi v1api.FullNode

	dhsMu sync.RWMutex
	dhs   map[uuid.UUID]*dealHandler
}

func NewProvider(repoRoot string, h host.Host, sqldb *sql.DB, dealsDB *db.DealsDB, fundMgr *fundmanager.FundManager, storageMgr *storagemanager.StorageManager, fullnodeApi v1api.FullNode, dealPublisher *DealPublisher, addr address.Address, secb *sectorblocks.SectorBlocks, sps sealingpipeline.State) (*Provider, error) {
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

		Transport:      httptransport.New(h),
		fundManager:    fundMgr,
		storageManager: storageMgr,

		dealPublisher:               dealPublisher,
		fullnodeApi:                 fullnodeApi,
		secb:                        secb,
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
	if err := p.validateDealProposal(ds); err != nil {
		return &api.ProviderDealRejectionInfo{
			Reason: fmt.Sprintf("failed validation: %s", err),
		}, nil
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
	sectorNum, offset, err := p.secb.AddPiece(ctx, pieceSize, pieceData, sdInfo)
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
			sectorNum, offset, err = p.secb.AddPiece(ctx, pieceSize, pieceData, sdInfo)
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

func (p *Provider) WaitForPublishDeals(ctx context.Context, publishCid cid.Cid, proposal market2.DealProposal) (*storagemarket.PublishDealsWaitResult, error) {
	// Wait for deal to be published (plus additional time for confidence)
	receipt, err := p.fullnodeApi.StateWaitMsg(ctx, publishCid, 2*build.MessageConfidence, api.LookbackNoLimit, true)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals errored: %w", err)
	}
	if receipt.Receipt.ExitCode != exitcode.Ok {
		return nil, xerrors.Errorf("WaitForPublishDeals exit code: %s", receipt.Receipt.ExitCode)
	}

	// The deal ID may have changed since publish if there was a reorg, so
	// get the current deal ID
	head, err := p.fullnodeApi.ChainHead(ctx)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals failed to get chain head: %w", err)
	}

	res, err := p.GetCurrentDealInfo(ctx, head.Key(), (*market.DealProposal)(&proposal), publishCid)
	if err != nil {
		return nil, xerrors.Errorf("WaitForPublishDeals getting deal info errored: %w", err)
	}

	return &storagemarket.PublishDealsWaitResult{DealID: res.DealID, FinalCid: receipt.Message}, nil
}

type CurrentDealInfo struct {
	DealID           abi.DealID
	MarketDeal       *lapi.MarketDeal
	PublishMsgTipSet ctypes.TipSetKey
}

// GetCurrentDealInfo gets the current deal state and deal ID.
// Note that the deal ID is assigned when the deal is published, so it may
// have changed if there was a reorg after the deal was published.
func (p *Provider) GetCurrentDealInfo(ctx context.Context, tok ctypes.TipSetKey, proposal *market.DealProposal, publishCid cid.Cid) (CurrentDealInfo, error) {
	// Lookup the deal ID by comparing the deal proposal to the proposals in
	// the publish deals message, and indexing into the message return value
	dealID, pubMsgTok, err := p.dealIDFromPublishDealsMsg(ctx, tok, proposal, publishCid)
	if err != nil {
		return CurrentDealInfo{}, err
	}

	// Lookup the deal state by deal ID
	marketDeal, err := p.fullnodeApi.StateMarketStorageDeal(ctx, dealID, tok)
	if err == nil && proposal != nil {
		// Make sure the retrieved deal proposal matches the target proposal
		equal, err := p.CheckDealEquality(ctx, tok, *proposal, marketDeal.Proposal)
		if err != nil {
			return CurrentDealInfo{}, err
		}
		if !equal {
			return CurrentDealInfo{}, xerrors.Errorf("Deal proposals for publish message %s did not match", publishCid)
		}
	}
	return CurrentDealInfo{DealID: dealID, MarketDeal: marketDeal, PublishMsgTipSet: pubMsgTok}, err
}

// dealIDFromPublishDealsMsg looks up the publish deals message by cid, and finds the deal ID
// by looking at the message return value
func (p *Provider) dealIDFromPublishDealsMsg(ctx context.Context, tok ctypes.TipSetKey, proposal *market.DealProposal, publishCid cid.Cid) (abi.DealID, ctypes.TipSetKey, error) {
	dealID := abi.DealID(0)

	// Get the return value of the publish deals message
	wmsg, err := p.fullnodeApi.StateSearchMsg(p.ctx, ctypes.EmptyTSK, publishCid, api.LookbackNoLimit, true)
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("getting publish deals message return value: %w", err)
	}

	if wmsg == nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("looking for publish deal message %s: not found", publishCid)
	}

	if wmsg.Receipt.ExitCode != exitcode.Ok {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("looking for publish deal message %s: non-ok exit code: %s", publishCid, wmsg.Receipt.ExitCode)
	}

	nv, err := p.fullnodeApi.StateNetworkVersion(ctx, wmsg.TipSet)
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("getting network version: %w", err)
	}

	retval, err := market.DecodePublishStorageDealsReturn(wmsg.Receipt.Return, nv)
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("looking for publish deal message %s: decoding message return: %w", publishCid, err)
	}

	dealIDs, err := retval.DealIDs()
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("looking for publish deal message %s: getting dealIDs: %w", publishCid, err)
	}

	// TODO: Can we delete this? We're well past the point when we first introduced the proposals into sealing deal info
	// Previously, publish deals messages contained a single deal, and the
	// deal proposal was not included in the sealing deal info.
	// So check if the proposal is nil and check the number of deals published
	// in the message.
	if proposal == nil {
		if len(dealIDs) > 1 {
			return dealID, ctypes.EmptyTSK, xerrors.Errorf(
				"getting deal ID from publish deal message %s: "+
					"no deal proposal supplied but message return value has more than one deal (%d deals)",
				publishCid, len(dealIDs))
		}

		// There is a single deal in this publish message and no deal proposal
		// was supplied, so we have nothing to compare against. Just assume
		// the deal ID is correct and that it was valid
		return dealIDs[0], wmsg.TipSet, nil
	}

	// Get the parameters to the publish deals message
	pubmsg, err := p.fullnodeApi.ChainGetMessage(ctx, publishCid)
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("getting publish deal message %s: %w", publishCid, err)
	}

	var pubDealsParams market2.PublishStorageDealsParams
	if err := pubDealsParams.UnmarshalCBOR(bytes.NewReader(pubmsg.Params)); err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("unmarshalling publish deal message params for message %s: %w", publishCid, err)
	}

	// Scan through the deal proposals in the message parameters to find the
	// index of the target deal proposal
	dealIdx := -1
	for i, paramDeal := range pubDealsParams.Deals {
		eq, err := p.CheckDealEquality(ctx, tok, *proposal, market.DealProposal(paramDeal.Proposal))
		if err != nil {
			return dealID, ctypes.EmptyTSK, xerrors.Errorf("comparing publish deal message %s proposal to deal proposal: %w", publishCid, err)
		}
		if eq {
			dealIdx = i
			break
		}
	}

	if dealIdx == -1 {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("could not find deal in publish deals message %s", publishCid)
	}

	if dealIdx >= len(dealIDs) {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf(
			"deal index %d out of bounds of deals (len %d) in publish deals message %s",
			dealIdx, len(dealIDs), publishCid)
	}

	valid, err := retval.IsDealValid(uint64(dealIdx))
	if err != nil {
		return dealID, ctypes.EmptyTSK, xerrors.Errorf("determining deal validity: %w", err)
	}

	if !valid {
		return dealID, ctypes.EmptyTSK, xerrors.New("deal was invalid at publication")
	}

	return dealIDs[dealIdx], wmsg.TipSet, nil
}

func (p *Provider) CheckDealEquality(ctx context.Context, tok ctypes.TipSetKey, p1, p2 market.DealProposal) (bool, error) {
	p1ClientID, err := p.fullnodeApi.StateLookupID(ctx, p1.Client, tok)
	if err != nil {
		return false, err
	}
	p2ClientID, err := p.fullnodeApi.StateLookupID(ctx, p2.Client, tok)
	if err != nil {
		return false, err
	}
	return p1.PieceCID.Equals(p2.PieceCID) &&
		p1.PieceSize == p2.PieceSize &&
		p1.VerifiedDeal == p2.VerifiedDeal &&
		p1.Label == p2.Label &&
		p1.StartEpoch == p2.StartEpoch &&
		p1.EndEpoch == p2.EndEpoch &&
		p1.StoragePricePerEpoch.Equals(p2.StoragePricePerEpoch) &&
		p1.ProviderCollateral.Equals(p2.ProviderCollateral) &&
		p1.ClientCollateral.Equals(p2.ClientCollateral) &&
		p1.Provider == p2.Provider &&
		p1ClientID == p2ClientID, nil
}
