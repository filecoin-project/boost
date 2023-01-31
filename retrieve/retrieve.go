package retrieve

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/boost/retrieve/rep"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-data-transfer/channelmonitor"
	dtimpl "github.com/filecoin-project/go-data-transfer/impl"
	dtnet "github.com/filecoin-project/go-data-transfer/network"
	gst "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	"github.com/filecoin-project/go-fil-markets/retrievalmarket"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p/core/host"
	inet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var (
	Tracer = otel.Tracer("retrieve-client")

	log = logging.Logger("retrieve-client")
)

const (
	RetrievalQueryProtocol = "/fil/retrieval/qry/1.0.0"

	maxTraversalLinks = 32 * (1 << 20)
)

type Client struct {
	api          api.Gateway
	wallet       *wallet.LocalWallet
	host         host.Host
	ClientAddr   address.Address
	blockstore   blockstore.Blockstore
	dataTransfer datatransfer.Manager
	graphSync    *gsimpl.GraphSync
	transport    *gst.Transport
	rep          *rep.RetrievalEventPublisher

	logRetrievalProgressEvents bool
}

type GetPieceCommFunc func(ctx context.Context, payloadCid cid.Cid, bstore blockstore.Blockstore) (cid.Cid, uint64, abi.UnpaddedPieceSize, error)

type Config struct {
	DataDir                    string
	GraphsyncOpts              []gsimpl.Option
	Api                        api.Gateway
	Wallet                     *wallet.LocalWallet
	Addr                       address.Address
	Blockstore                 blockstore.Blockstore
	Datastore                  datastore.Batching
	Host                       host.Host
	ChannelMonitorConfig       channelmonitor.Config
	RetrievalConfigurer        datatransfer.TransportConfigurer
	LogRetrievalProgressEvents bool
}

func NewClient(h host.Host, api api.Gateway, w *wallet.LocalWallet, addr address.Address, bs blockstore.Blockstore, ds datastore.Batching, ddir string, opts ...func(*Config)) (*Client, error) {
	cfg := &Config{
		Host:       h,
		Api:        api,
		Wallet:     w,
		Addr:       addr,
		Blockstore: bs,
		Datastore:  ds,
		DataDir:    ddir,
		GraphsyncOpts: []gsimpl.Option{
			gsimpl.MaxInProgressIncomingRequests(200),
			gsimpl.MaxInProgressOutgoingRequests(200),
			gsimpl.MaxMemoryResponder(8 << 30),
			gsimpl.MaxMemoryPerPeerResponder(32 << 20),
			gsimpl.MaxInProgressIncomingRequestsPerPeer(20),
			gsimpl.MessageSendRetries(2),
			gsimpl.SendMessageTimeout(2 * time.Minute),
			gsimpl.MaxLinksPerIncomingRequests(maxTraversalLinks),
			gsimpl.MaxLinksPerOutgoingRequests(maxTraversalLinks),
		},
		ChannelMonitorConfig: channelmonitor.Config{

			AcceptTimeout:          time.Hour * 24,
			RestartDebounce:        time.Second * 10,
			RestartBackoff:         time.Second * 20,
			MaxConsecutiveRestarts: 15,
			//RestartAckTimeout:      time.Second * 30,
			CompleteTimeout: time.Minute * 40,

			// Called when a restart completes successfully
			//OnRestartComplete func(id datatransfer.ChannelID)
		},
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return NewClientWithConfig(cfg)
}

func NewClientWithConfig(cfg *Config) (*Client, error) {
	gse := gsimpl.New(context.Background(),
		gsnet.NewFromLibp2pHost(cfg.Host),
		storeutil.LinkSystemForBlockstore(cfg.Blockstore),
		cfg.GraphsyncOpts...,
	).(*gsimpl.GraphSync)

	dtn := dtnet.NewFromLibp2pHost(cfg.Host)
	tpt := gst.NewTransport(cfg.Host.ID(), gse)

	dtRestartConfig := dtimpl.ChannelRestartConfig(cfg.ChannelMonitorConfig)

	cidlistsdirPath := filepath.Join(cfg.DataDir, "cidlistsdir")
	if err := os.MkdirAll(cidlistsdirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to initialize cidlistsdir: %w", err)
	}

	ctx := context.Background()

	mgr, err := dtimpl.NewDataTransfer(cfg.Datastore, dtn, tpt, dtRestartConfig)
	if err != nil {
		return nil, err
	}

	err = mgr.RegisterVoucherType(&retrievalmarket.DealProposal{}, nil)
	if err != nil {
		return nil, err
	}

	err = mgr.RegisterVoucherType(&retrievalmarket.DealPayment{}, nil)
	if err != nil {
		return nil, err
	}

	err = mgr.RegisterVoucherResultType(&retrievalmarket.DealResponse{})
	if err != nil {
		return nil, err
	}

	if cfg.RetrievalConfigurer != nil {
		if err := mgr.RegisterTransportConfigurer(&retrievalmarket.DealProposal{}, cfg.RetrievalConfigurer); err != nil {
			return nil, err
		}
	}

	if err := mgr.Start(ctx); err != nil {
		return nil, err
	}

	// Create a retrieval event publisher
	pb := rep.New(ctx)

	c := &Client{
		host:                       cfg.Host,
		api:                        cfg.Api,
		wallet:                     cfg.Wallet,
		ClientAddr:                 cfg.Addr,
		blockstore:                 cfg.Blockstore,
		graphSync:                  gse,
		transport:                  tpt,
		dataTransfer:               mgr,
		logRetrievalProgressEvents: cfg.LogRetrievalProgressEvents,
		rep:                        pb,
	}

	// Subscribe this instance to retrieval events
	pb.Subscribe(c)

	return c, nil
}

func (c *Client) GetDtMgr() datatransfer.Manager {
	return c.dataTransfer
}

func (c *Client) streamToMiner(ctx context.Context, maddr address.Address, protocol ...protocol.ID) (inet.Stream, error) {
	ctx, span := Tracer.Start(ctx, "streamToMiner", trace.WithAttributes(
		attribute.Stringer("miner", maddr),
	))
	defer span.End()

	mpid, err := c.ConnectToMiner(ctx, maddr)
	if err != nil {
		return nil, err
	}

	s, err := c.host.NewStream(ctx, mpid, protocol...)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream to peer: %w", err)
	}

	return s, nil
}

// Errors - ErrMinerConnectionFailed, ErrLotusError
func (c *Client) ConnectToMiner(ctx context.Context, maddr address.Address) (peer.ID, error) {
	addrInfo, err := c.minerAddrInfo(ctx, maddr)
	if err != nil {
		return "", err
	}

	if err := c.host.Connect(ctx, *addrInfo); err != nil {
		return "", NewErrMinerConnectionFailed(err)
	}

	return addrInfo.ID, nil
}

func (c *Client) minerAddrInfo(ctx context.Context, maddr address.Address) (*peer.AddrInfo, error) {
	minfo, err := c.api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return nil, NewErrLotusError(err)
	}

	if minfo.PeerId == nil {
		return nil, NewErrMinerConnectionFailed(fmt.Errorf("miner %s has no peer ID set", maddr))
	}

	var maddrs []multiaddr.Multiaddr
	for _, mma := range minfo.Multiaddrs {
		ma, err := multiaddr.NewMultiaddrBytes(mma)
		if err != nil {
			return nil, NewErrMinerConnectionFailed(fmt.Errorf("miner %s had invalid multiaddrs in their info: %w", maddr, err))
		}
		maddrs = append(maddrs, ma)
	}

	if len(maddrs) == 0 {
		return nil, NewErrMinerConnectionFailed(fmt.Errorf("miner %s has no multiaddrs set on chain", maddr))
	}

	if err := c.host.Connect(ctx, peer.AddrInfo{
		ID:    *minfo.PeerId,
		Addrs: maddrs,
	}); err != nil {
		return nil, NewErrMinerConnectionFailed(err)
	}

	return &peer.AddrInfo{
		ID:    *minfo.PeerId,
		Addrs: maddrs,
	}, nil
}

func (c *Client) openStreamToPeer(ctx context.Context, addr peer.AddrInfo, protocol protocol.ID) (inet.Stream, error) {
	ctx, span := Tracer.Start(ctx, "openStreamToPeer", trace.WithAttributes(
		attribute.Stringer("peerID", addr.ID),
	))
	defer span.End()

	err := c.connectToPeer(ctx, addr)
	if err != nil {
		return nil, err
	}

	s, err := c.host.NewStream(ctx, addr.ID, protocol)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream to peer: %w", err)
	}

	return s, nil
}

// Errors - ErrMinerConnectionFailed, ErrLotusError
func (c *Client) connectToPeer(ctx context.Context, addr peer.AddrInfo) error {
	if err := c.host.Connect(ctx, addr); err != nil {
		return NewErrMinerConnectionFailed(err)
	}

	return nil
}

func (c *Client) MinerPeer(ctx context.Context, miner address.Address) (peer.AddrInfo, error) {
	minfo, err := c.api.StateMinerInfo(ctx, miner, types.EmptyTSK)
	if err != nil {
		return peer.AddrInfo{}, err
	}

	if minfo.PeerId == nil {
		return peer.AddrInfo{}, fmt.Errorf("miner %s has no peer ID set", miner)
	}

	var maddrs []multiaddr.Multiaddr
	for _, mma := range minfo.Multiaddrs {
		ma, err := multiaddr.NewMultiaddrBytes(mma)
		if err != nil {
			return peer.AddrInfo{}, fmt.Errorf("miner %s had invalid multiaddrs in their info: %w", miner, err)
		}
		maddrs = append(maddrs, ma)
	}

	return peer.AddrInfo{
		ID:    *minfo.PeerId,
		Addrs: maddrs,
	}, nil
}

func (c *Client) minerOwner(ctx context.Context, miner address.Address) (address.Address, error) {
	minfo, err := c.api.StateMinerInfo(ctx, miner, types.EmptyTSK)
	if err != nil {
		return address.Undef, err
	}
	if minfo.PeerId == nil {
		return address.Undef, fmt.Errorf("miner has no peer id")
	}

	return minfo.Owner, nil
}

func doRpc(ctx context.Context, s inet.Stream, req interface{}, resp interface{}) error {
	dline, ok := ctx.Deadline()
	if ok {
		_ = s.SetDeadline(dline)
		defer func() { _ = s.SetDeadline(time.Time{}) }()
	}

	if err := cborutil.WriteCborRPC(s, req); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	if err := cborutil.ReadCborRPC(s, resp); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	return nil
}

type TransferType string

const (
	BoostTransfer     TransferType = "boost"
	GraphsyncTransfer TransferType = "graphsync"
)

type ChannelState struct {
	//datatransfer.Channel

	// SelfPeer returns the peer this channel belongs to
	SelfPeer   peer.ID `json:"selfPeer"`
	RemotePeer peer.ID `json:"remotePeer"`

	// Status is the current status of this channel
	Status    datatransfer.Status `json:"status"`
	StatusStr string              `json:"statusMessage"`

	// Sent returns the number of bytes sent
	Sent uint64 `json:"sent"`

	// Received returns the number of bytes received
	Received uint64 `json:"received"`

	// Message offers additional information about the current status
	Message string `json:"message"`

	BaseCid string `json:"baseCid"`

	ChannelID datatransfer.ChannelID `json:"channelId"`

	TransferID string `json:"transferId"`

	// Vouchers returns all vouchers sent on this channel
	//Vouchers []datatransfer.Voucher

	// VoucherResults are results of vouchers sent on the channel
	//VoucherResults []datatransfer.VoucherResult

	// LastVoucher returns the last voucher sent on the channel
	//LastVoucher datatransfer.Voucher

	// LastVoucherResult returns the last voucher result sent on the channel
	//LastVoucherResult datatransfer.VoucherResult

	// ReceivedCids returns the cids received so far on the channel
	//ReceivedCids []cid.Cid

	// Queued returns the number of bytes read from the node and queued for sending
	//Queued uint64

	Stages *datatransfer.ChannelStages

	TransferType TransferType
}

func ChannelStateConv(st datatransfer.ChannelState) *ChannelState {
	return &ChannelState{
		SelfPeer:     st.SelfPeer(),
		RemotePeer:   st.OtherPeer(),
		Status:       st.Status(),
		StatusStr:    datatransfer.Statuses[st.Status()],
		Sent:         st.Sent(),
		Received:     st.Received(),
		Message:      st.Message(),
		BaseCid:      st.BaseCID().String(),
		ChannelID:    st.ChannelID(),
		TransferID:   st.ChannelID().String(),
		Stages:       st.Stages(),
		TransferType: GraphsyncTransfer,
	}
}

func (c *Client) GraphSyncStats() graphsync.Stats {
	return c.graphSync.Stats()
}

func (c *Client) TransferStatus(ctx context.Context, chanid *datatransfer.ChannelID) (*ChannelState, error) {
	st, err := c.dataTransfer.ChannelState(ctx, *chanid)
	if err != nil {
		return nil, err
	}

	return ChannelStateConv(st), nil
}

var ErrNoTransferFound = fmt.Errorf("no transfer found")

func (c *Client) RestartTransfer(ctx context.Context, chanid *datatransfer.ChannelID) error {
	return c.dataTransfer.RestartDataTransferChannel(ctx, *chanid)
}

func (c *Client) SubscribeToDataTransferEvents(f datatransfer.Subscriber) func() {
	return c.dataTransfer.SubscribeToEvents(f)
}

type Balance struct {
	Account         address.Address `json:"account"`
	Balance         types.FIL       `json:"balance"`
	MarketEscrow    types.FIL       `json:"marketEscrow"`
	MarketLocked    types.FIL       `json:"marketLocked"`
	MarketAvailable types.FIL       `json:"marketAvailable"`

	VerifiedClientBalance *abi.StoragePower `json:"verifiedClientBalance"`
}

func (c *Client) Balance(ctx context.Context) (*Balance, error) {
	act, err := c.api.StateGetActor(ctx, c.ClientAddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	market, err := c.api.StateMarketBalance(ctx, c.ClientAddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	vcstatus, err := c.api.StateVerifiedClientStatus(ctx, c.ClientAddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	avail := types.BigSub(market.Escrow, market.Locked)

	return &Balance{
		Account:               c.ClientAddr,
		Balance:               types.FIL(act.Balance),
		MarketEscrow:          types.FIL(market.Escrow),
		MarketLocked:          types.FIL(market.Locked),
		MarketAvailable:       types.FIL(avail),
		VerifiedClientBalance: vcstatus,
	}, nil
}

type LockFundsResp struct {
	MsgCid cid.Cid
}

func (c *Client) CheckChainDeal(ctx context.Context, dealid abi.DealID) (bool, *api.MarketDeal, error) {
	deal, err := c.api.StateMarketStorageDeal(ctx, dealid, types.EmptyTSK)
	if err != nil {
		nfs := fmt.Sprintf("deal %d not found", dealid)
		if strings.Contains(err.Error(), nfs) {
			return false, nil, nil
		}

		return false, nil, err
	}

	return true, deal, nil
}

func (c *Client) CheckOngoingTransfer(ctx context.Context, miner address.Address, st *ChannelState) (outerr error) {
	defer func() {
		// TODO: this is only here because for some reason restarting a data transfer can just panic
		// https://github.com/filecoin-project/go-data-transfer/issues/150
		if e := recover(); e != nil {
			outerr = fmt.Errorf("panic while checking transfer: %s", e)
		}
	}()
	// make sure we at least have an open connection to the miner
	if c.host.Network().Connectedness(st.RemotePeer) != inet.Connected {
		// try reconnecting
		mpid, err := c.ConnectToMiner(ctx, miner)
		if err != nil {
			return err
		}

		if mpid != st.RemotePeer {
			return fmt.Errorf("miner peer ID is different than RemotePeer in data transfer channel")
		}
	}

	return c.dataTransfer.RestartDataTransferChannel(ctx, st.ChannelID)

}

func (c *Client) RetrievalQuery(ctx context.Context, maddr address.Address, pcid cid.Cid) (*retrievalmarket.QueryResponse, error) {
	ctx, span := Tracer.Start(ctx, "retrievalQuery", trace.WithAttributes(
		attribute.Stringer("miner", maddr),
	))
	defer span.End()

	s, err := c.streamToMiner(ctx, maddr, RetrievalQueryProtocol)
	if err != nil {
		// publish fail event, log the err
		c.rep.Publish(
			rep.NewRetrievalEventFailure(rep.QueryPhase, pcid, "", maddr,
				fmt.Sprintf("failed connecting to miner: %s", err.Error())))
		return nil, err
	}

	c.host.ConnManager().Protect(s.Conn().RemotePeer(), "RetrievalQuery")
	defer func() {
		c.host.ConnManager().Unprotect(s.Conn().RemotePeer(), "RetrievalQuery")
		s.Close()
	}()

	// We have connected
	// publish connected event
	c.rep.Publish(rep.NewRetrievalEventConnect(rep.QueryPhase, pcid, "", maddr))

	q := &retrievalmarket.Query{
		PayloadCID: pcid,
	}

	var resp retrievalmarket.QueryResponse
	if err := doRpc(ctx, s, q, &resp); err != nil {
		// publish failure event
		c.rep.Publish(
			rep.NewRetrievalEventFailure(rep.QueryPhase, pcid, "", maddr,
				fmt.Sprintf("failed retrieval query ask: %s", err.Error())))
		return nil, fmt.Errorf("retrieval query rpc: %w", err)
	}

	// publish query ask event
	c.rep.Publish(rep.NewRetrievalEventQueryAsk(rep.QueryPhase, pcid, "", maddr, resp))

	return &resp, nil
}

func (c *Client) RetrievalQueryToPeer(ctx context.Context, minerPeer peer.AddrInfo, pcid cid.Cid) (*retrievalmarket.QueryResponse, error) {
	ctx, span := Tracer.Start(ctx, "retrievalQueryPeer", trace.WithAttributes(
		attribute.Stringer("peerID", minerPeer.ID),
	))
	defer span.End()

	s, err := c.openStreamToPeer(ctx, minerPeer, RetrievalQueryProtocol)
	if err != nil {
		// publish fail event, log the err
		c.rep.Publish(
			rep.NewRetrievalEventFailure(rep.QueryPhase, pcid, minerPeer.ID, address.Undef,
				fmt.Sprintf("failed connecting to miner: %s", err.Error())))
		return nil, err
	}

	c.host.ConnManager().Protect(s.Conn().RemotePeer(), "RetrievalQueryToPeer")
	defer func() {
		c.host.ConnManager().Unprotect(s.Conn().RemotePeer(), "RetrievalQueryToPeer")
		s.Close()
	}()

	// We have connected
	// publish connected event
	c.rep.Publish(rep.NewRetrievalEventConnect(rep.QueryPhase, pcid, minerPeer.ID, address.Address{}))

	q := &retrievalmarket.Query{
		PayloadCID: pcid,
	}

	var resp retrievalmarket.QueryResponse
	if err := doRpc(ctx, s, q, &resp); err != nil {
		// publish failure event
		c.rep.Publish(
			rep.NewRetrievalEventFailure(rep.QueryPhase, pcid, minerPeer.ID, address.Undef,
				fmt.Sprintf("failed retrieval query ask: %s", err.Error())))
		return nil, fmt.Errorf("retrieval query rpc: %w", err)
	}

	// publish query ask event
	c.rep.Publish(rep.NewRetrievalEventQueryAsk(rep.QueryPhase, pcid, minerPeer.ID, address.Undef, resp))

	return &resp, nil
}

type RetrievalStats struct {
	Peer         peer.ID
	Size         uint64
	Duration     time.Duration
	AverageSpeed uint64
	TotalPayment abi.TokenAmount
	NumPayments  int
	AskPrice     abi.TokenAmount

	// TODO: we should be able to get this if we hook into the graphsync event stream
	//TimeToFirstByte time.Duration
}

func (c *Client) RetrieveContent(
	ctx context.Context,
	miner address.Address,
	proposal *retrievalmarket.DealProposal,
) (*RetrievalStats, error) {

	return c.RetrieveContentWithProgressCallback(ctx, miner, proposal, nil)
}

func (c *Client) RetrieveContentWithProgressCallback(
	ctx context.Context,
	miner address.Address,
	proposal *retrievalmarket.DealProposal,
	progressCallback func(bytesReceived uint64),
) (*RetrievalStats, error) {

	log.Infof("Starting retrieval with miner: %s", miner)

	minerPeer, err := c.MinerPeer(ctx, miner)
	if err != nil {
		return nil, err
	}
	minerOwner, err := c.minerOwner(ctx, miner)
	if err != nil {
		return nil, err
	}
	return c.RetrieveContentFromPeerWithProgressCallback(ctx, minerPeer.ID, minerOwner, proposal, progressCallback)
}

func (c *Client) RetrieveContentFromPeerWithProgressCallback(
	ctx context.Context,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievalmarket.DealProposal,
	progressCallback func(bytesReceived uint64),
) (*RetrievalStats, error) {
	return c.retrieveContentFromPeerWithProgressCallback(ctx, peerID, minerWallet, proposal, progressCallback, nil)
}

type RetrievalResult struct {
	*RetrievalStats
	Err error
}

func (c *Client) RetrieveContentFromPeerAsync(
	ctx context.Context,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievalmarket.DealProposal,
) (result <-chan RetrievalResult, onProgress <-chan uint64, gracefulShutdown func()) {
	gracefulShutdownChan := make(chan struct{}, 1)
	resultChan := make(chan RetrievalResult, 1)
	progressChan := make(chan uint64)
	internalCtx, internalCancel := context.WithCancel(ctx)
	go func() {
		defer internalCancel()
		result, err := c.retrieveContentFromPeerWithProgressCallback(internalCtx, peerID, minerWallet, proposal, func(bytes uint64) {
			select {
			case <-internalCtx.Done():
			case progressChan <- bytes:
			}
		}, gracefulShutdownChan)
		resultChan <- RetrievalResult{result, err}
	}()
	return resultChan, progressChan, func() {
		gracefulShutdownChan <- struct{}{}
	}
}

func (c *Client) retrieveContentFromPeerWithProgressCallback(
	ctx context.Context,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *retrievalmarket.DealProposal,
	progressCallback func(bytesReceived uint64),
	gracefulShutdownRequested <-chan struct{},
) (*RetrievalStats, error) {
	if progressCallback == nil {
		progressCallback = func(bytesReceived uint64) {}
	}

	log.Infof("Starting retrieval with miner peer ID: %s", peerID)

	ctx, span := Tracer.Start(ctx, "retrieveContent")
	defer span.End()

	// Stats
	startTime := time.Now()
	totalPayment := abi.NewTokenAmount(0)

	rootCid := proposal.PayloadCID
	var chanid datatransfer.ChannelID
	var chanidLk sync.Mutex

	pchRequired := !proposal.PricePerByte.IsZero() || !proposal.UnsealPrice.IsZero()
	if pchRequired {
		return nil, errors.New("payment channel required, boost doesn't support these retrievals")
	}

	// Set up incoming events handler

	// The next nonce (incrementing unique ID starting from 0) for the next voucher
	var nonce uint64 = 0

	// dtRes receives either an error (failure) or nil (success) which is waited
	// on and handled below before exiting the function
	dtRes := make(chan error, 1)

	finish := func(err error) {
		select {
		case dtRes <- err:
		default:
		}
	}

	dealID := proposal.ID
	allBytesReceived := false
	dealComplete := false
	receivedFirstByte := false

	unsubscribe := c.dataTransfer.SubscribeToEvents(func(event datatransfer.Event, state datatransfer.ChannelState) {
		// Copy chanid so it can be used later in the callback
		chanidLk.Lock()
		chanidCopy := chanid
		chanidLk.Unlock()

		// Skip all events that aren't related to this channel
		if state.ChannelID() != chanidCopy {
			return
		}

		silenceEventCode := false
		eventCodeNotHandled := false

		switch event.Code {
		case datatransfer.Open:
		case datatransfer.Accept:
		case datatransfer.Restart:
		case datatransfer.DataReceived:
			silenceEventCode = true
		case datatransfer.DataSent:
		case datatransfer.Cancel:
		case datatransfer.Error:
			finish(fmt.Errorf("datatransfer error: %s", event.Message))
			return
		case datatransfer.CleanupComplete:
			finish(nil)
			return
		case datatransfer.NewVoucher:
		case datatransfer.NewVoucherResult:

			switch resType := state.LastVoucherResult().(type) {
			case *retrievalmarket.DealResponse:
				if len(resType.Message) != 0 {
					log.Debugf("Received deal response voucher result %s (%v): %s\n\t%+v", resType.Status, resType.Status, resType.Message, resType)
				} else {
					log.Debugf("Received deal response voucher result %s (%v)\n\t%+v", resType.Status, resType.Status, resType)
				}

				switch resType.Status {
				case retrievalmarket.DealStatusAccepted:
					log.Info("Deal accepted")

					// publish deal accepted event
					c.rep.Publish(rep.NewRetrievalEventAccepted(rep.RetrievalPhase, rootCid, peerID, address.Undef))

				// Respond with a payment voucher when funds are requested
				case retrievalmarket.DealStatusFundsNeeded, retrievalmarket.DealStatusFundsNeededLastPayment:
					if pchRequired {
						finish(errors.New("payment channel required"))
						return
					} else {
						finish(fmt.Errorf("the miner requested payment even though this transaction was determined to be zero cost"))
						return
					}
				case retrievalmarket.DealStatusRejected:
					finish(fmt.Errorf("deal rejected: %s", resType.Message))
					return
				case retrievalmarket.DealStatusFundsNeededUnseal, retrievalmarket.DealStatusUnsealing:
					finish(fmt.Errorf("data is sealed"))
					return
				case retrievalmarket.DealStatusCancelled:
					finish(fmt.Errorf("deal cancelled: %s", resType.Message))
					return
				case retrievalmarket.DealStatusErrored:
					finish(fmt.Errorf("deal errored: %s", resType.Message))
					return
				case retrievalmarket.DealStatusCompleted:
					if allBytesReceived {
						finish(nil)
						return
					}
					dealComplete = true
				}
			}
		case datatransfer.PauseInitiator:
		case datatransfer.ResumeInitiator:
		case datatransfer.PauseResponder:
		case datatransfer.ResumeResponder:
		case datatransfer.FinishTransfer:
			if dealComplete {
				finish(nil)
				return
			}
			allBytesReceived = true
		case datatransfer.ResponderCompletes:
		case datatransfer.ResponderBeginsFinalization:
		case datatransfer.BeginFinalizing:
		case datatransfer.Disconnected:
		case datatransfer.Complete:
		case datatransfer.CompleteCleanupOnRestart:
		case datatransfer.DataQueued:
		case datatransfer.DataQueuedProgress:
		case datatransfer.DataSentProgress:
		case datatransfer.DataReceivedProgress:
			// First byte has been received

			// publish first byte event
			if !receivedFirstByte {
				receivedFirstByte = true
				c.rep.Publish(rep.NewRetrievalEventFirstByte(rep.RetrievalPhase, rootCid, peerID, address.Undef))
			}

			progressCallback(state.Received())
			silenceEventCode = true
		case datatransfer.RequestTimedOut:
		case datatransfer.SendDataError:
		case datatransfer.ReceiveDataError:
		case datatransfer.TransferRequestQueued:
		case datatransfer.RequestCancelled:
		case datatransfer.Opened:
		default:
			eventCodeNotHandled = true
		}

		name := datatransfer.Events[event.Code]
		code := event.Code
		msg := event.Message
		blocksIndex := state.ReceivedCidsTotal()
		totalReceived := state.Received()
		if eventCodeNotHandled {
			log.Warnw("unhandled retrieval event", "dealID", dealID, "rootCid", rootCid, "peerID", peerID, "name", name, "code", code, "message", msg, "blocksIndex", blocksIndex, "totalReceived", totalReceived)
		} else {
			if !silenceEventCode || c.logRetrievalProgressEvents {
				log.Debugw("retrieval event", "dealID", dealID, "rootCid", rootCid, "peerID", peerID, "name", name, "code", code, "message", msg, "blocksIndex", blocksIndex, "totalReceived", totalReceived)
			}
		}
	})
	defer unsubscribe()

	// Submit the retrieval deal proposal to the miner
	newchid, err := c.dataTransfer.OpenPullDataChannel(ctx, peerID, proposal, proposal.PayloadCID, selectorparse.CommonSelector_ExploreAllRecursively)
	if err != nil {
		// We could fail before a successful proposal
		// publish event failure
		c.rep.Publish(
			rep.NewRetrievalEventFailure(rep.RetrievalPhase, rootCid, peerID, address.Undef,
				fmt.Sprintf("deal proposal failed: %s", err.Error())))
		return nil, err
	}

	// Deal has been proposed
	// publish deal proposed event
	c.rep.Publish(rep.NewRetrievalEventProposed(rep.RetrievalPhase, rootCid, peerID, address.Undef))

	chanidLk.Lock()
	chanid = newchid
	chanidLk.Unlock()

	defer func() { _ = c.dataTransfer.CloseDataTransferChannel(ctx, chanid) }()

	// Wait for the retrieval to finish before exiting the function
awaitfinished:
	for {
		select {
		case err := <-dtRes:
			if err != nil {
				// If there is an error, publish a retrieval event failure
				c.rep.Publish(
					rep.NewRetrievalEventFailure(rep.RetrievalPhase, rootCid, peerID, address.Undef,
						fmt.Sprintf("data transfer failed: %s", err.Error())))
				return nil, fmt.Errorf("data transfer failed: %w", err)
			}

			log.Debugf("data transfer for retrieval complete")
			break awaitfinished
		case <-gracefulShutdownRequested:
			go func() {
				_ = c.dataTransfer.CloseDataTransferChannel(ctx, chanid)
			}()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Confirm that we actually ended up with the root block we wanted, failure
	// here indicates a data transfer error that was not properly reported
	if has, err := c.blockstore.Has(ctx, rootCid); err != nil {
		err = fmt.Errorf("could not get query blockstore: %w", err)
		c.rep.Publish(
			rep.NewRetrievalEventFailure(rep.RetrievalPhase, rootCid, peerID, address.Undef, err.Error()))
		return nil, err
	} else if !has {
		msg := "data transfer failed: unconfirmed block transfer"
		c.rep.Publish(
			rep.NewRetrievalEventFailure(rep.RetrievalPhase, rootCid, peerID, address.Undef, msg))
		return nil, errors.New(msg)
	}

	// Compile the retrieval stats

	state, err := c.dataTransfer.ChannelState(ctx, chanid)
	if err != nil {
		err = fmt.Errorf("could not get channel state: %w", err)
		c.rep.Publish(
			rep.NewRetrievalEventFailure(rep.RetrievalPhase, rootCid, peerID, address.Undef, err.Error()))
		return nil, err
	}

	duration := time.Since(startTime)
	speed := uint64(float64(state.Received()) / duration.Seconds())

	// Otherwise publish a retrieval event success
	c.rep.Publish(rep.NewRetrievalEventSuccess(rep.RetrievalPhase, rootCid, peerID, address.Undef, state.Received(), state.ReceivedCidsTotal(), duration, totalPayment))

	return &RetrievalStats{
		Peer:         state.OtherPeer(),
		Size:         state.Received(),
		Duration:     duration,
		AverageSpeed: speed,
		TotalPayment: totalPayment,
		NumPayments:  int(nonce),
		AskPrice:     proposal.PricePerByte,
	}, nil
}

func (fc *Client) SubscribeToRetrievalEvents(subscriber rep.RetrievalSubscriber) {
	fc.rep.Subscribe(subscriber)
}

// Implement RetrievalSubscriber
func (fc *Client) OnRetrievalEvent(event rep.RetrievalEvent) {
	kv := make([]interface{}, 0)
	logadd := func(kva ...interface{}) {
		if len(kva)%2 != 0 {
			panic("bad number of key/value arguments")
		}
		for i := 0; i < len(kva); i += 2 {
			key, ok := kva[i].(string)
			if !ok {
				panic("expected string key")
			}
			kv = append(kv, key, kva[i+1])
		}
	}
	logadd("code", event.Code(),
		"phase", event.Phase(),
		"payloadCid", event.PayloadCid(),
		"storageProviderId", event.StorageProviderId(),
		"storageProviderAddr", event.StorageProviderAddr())
	switch tevent := event.(type) {
	case rep.RetrievalEventQueryAsk:
		logadd("queryResponse:Status", tevent.QueryResponse().Status,
			"queryResponse:PieceCIDFound", tevent.QueryResponse().PieceCIDFound,
			"queryResponse:Size", tevent.QueryResponse().Size,
			"queryResponse:PaymentAddress", tevent.QueryResponse().PaymentAddress,
			"queryResponse:MinPricePerByte", tevent.QueryResponse().MinPricePerByte,
			"queryResponse:MaxPaymentInterval", tevent.QueryResponse().MaxPaymentInterval,
			"queryResponse:MaxPaymentIntervalIncrease", tevent.QueryResponse().MaxPaymentIntervalIncrease,
			"queryResponse:Message", tevent.QueryResponse().Message,
			"queryResponse:UnsealPrice", tevent.QueryResponse().UnsealPrice)
	case rep.RetrievalEventFailure:
		logadd("errorMessage", tevent.ErrorMessage())
	case rep.RetrievalEventSuccess:
		logadd("receivedSize", tevent.ReceivedSize())
	}
	log.Debugw("retrieval-event", kv...)
}

var dealIdGen = shared.NewTimeCounter()

func RetrievalProposalForAsk(ask *retrievalmarket.QueryResponse, c cid.Cid, optionalSelector ipld.Node) (*retrievalmarket.DealProposal, error) {
	if optionalSelector == nil {
		optionalSelector = selectorparse.CommonSelector_ExploreAllRecursively
	}

	params, err := retrievalmarket.NewParamsV1(
		ask.MinPricePerByte,
		ask.MaxPaymentInterval,
		ask.MaxPaymentIntervalIncrease,
		optionalSelector,
		nil,
		ask.UnsealPrice,
	)
	if err != nil {
		return nil, err
	}
	return &retrievalmarket.DealProposal{
		PayloadCID: c,
		ID:         retrievalmarket.DealID(dealIdGen.Next()),
		Params:     params,
	}, nil
}
