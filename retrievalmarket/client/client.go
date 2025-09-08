package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	gsimpl "github.com/filecoin-project/boost-graphsync/impl"
	gsnet "github.com/filecoin-project/boost-graphsync/network"
	"github.com/filecoin-project/boost-graphsync/storeutil"
	datatransfer2 "github.com/filecoin-project/boost/datatransfer"
	dtnet "github.com/filecoin-project/boost/datatransfer/network"
	gst "github.com/filecoin-project/boost/datatransfer/transport/graphsync"
	"github.com/filecoin-project/boost/markets/shared"
	"github.com/filecoin-project/boost/retrievalmarket/types/legacyretrievaltypes"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"

	"github.com/filecoin-project/boost/datatransfer/channelmonitor"
	dtimpl "github.com/filecoin-project/boost/datatransfer/impl"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
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

	dealIdGen = shared.NewTimeCounter()
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
	dataTransfer datatransfer2.Manager

	logRetrievalProgressEvents bool
}

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
	RetrievalConfigurer        datatransfer2.TransportConfigurer
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
			CompleteTimeout:        time.Minute * 40,
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

	mgr, err := dtimpl.NewDataTransfer(cfg.Datastore, dtn, tpt, dtRestartConfig)
	if err != nil {
		return nil, err
	}

	err = mgr.RegisterVoucherType(&legacyretrievaltypes.DealProposal{}, nil)
	if err != nil {
		return nil, err
	}

	err = mgr.RegisterVoucherType(&legacyretrievaltypes.DealPayment{}, nil)
	if err != nil {
		return nil, err
	}

	err = mgr.RegisterVoucherResultType(&legacyretrievaltypes.DealResponse{})
	if err != nil {
		return nil, err
	}

	if cfg.RetrievalConfigurer != nil {
		if err := mgr.RegisterTransportConfigurer(&legacyretrievaltypes.DealProposal{}, cfg.RetrievalConfigurer); err != nil {
			return nil, err
		}
	}

	errCh := make(chan error)
	startedCh := make(chan struct{})

	mgr.OnReady(func(err error) {
		if err != nil {
			errCh <- err
			return
		}
		close(startedCh)
	})

	if err := mgr.Start(context.Background()); err != nil {
		return nil, err
	}

	select {
	case <-startedCh:
	case err := <-errCh:
		return nil, err
	}

	c := &Client{
		host:                       cfg.Host,
		api:                        cfg.Api,
		wallet:                     cfg.Wallet,
		ClientAddr:                 cfg.Addr,
		blockstore:                 cfg.Blockstore,
		dataTransfer:               mgr,
		logRetrievalProgressEvents: cfg.LogRetrievalProgressEvents,
	}

	return c, nil
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

func (c *Client) ConnectToMiner(ctx context.Context, maddr address.Address) (peer.ID, error) {
	addrInfo, err := c.minerAddrInfo(ctx, maddr)
	if err != nil {
		return "", err
	}

	if err := c.host.Connect(ctx, *addrInfo); err != nil {
		return "", err
	}

	return addrInfo.ID, nil
}

func (c *Client) minerAddrInfo(ctx context.Context, maddr address.Address) (*peer.AddrInfo, error) {
	minfo, err := c.api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return nil, err
	}

	if minfo.PeerId == nil {
		return nil, fmt.Errorf("miner %s has no peer ID set", maddr)
	}

	var maddrs []multiaddr.Multiaddr
	for _, mma := range minfo.Multiaddrs {
		ma, err := multiaddr.NewMultiaddrBytes(mma)
		if err != nil {
			return nil, fmt.Errorf("miner %s had invalid multiaddrs in their info: %w", maddr, err)
		}
		maddrs = append(maddrs, ma)
	}

	if len(maddrs) == 0 {
		return nil, fmt.Errorf("miner %s has no multiaddrs set on chain", maddr)
	}

	err = c.host.Connect(ctx, peer.AddrInfo{ID: *minfo.PeerId, Addrs: maddrs})
	if err != nil {
		return nil, err
	}

	return &peer.AddrInfo{
		ID:    *minfo.PeerId,
		Addrs: maddrs,
	}, nil
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

func (c *Client) RetrievalQuery(ctx context.Context, maddr address.Address, pcid cid.Cid) (*legacyretrievaltypes.QueryResponse, error) {
	ctx, span := Tracer.Start(ctx, "retrievalQuery", trace.WithAttributes(
		attribute.Stringer("miner", maddr),
	))
	defer span.End()

	s, err := c.streamToMiner(ctx, maddr, RetrievalQueryProtocol)
	if err != nil {
		return nil, err
	}

	c.host.ConnManager().Protect(s.Conn().RemotePeer(), "RetrievalQuery")
	defer func() {
		c.host.ConnManager().Unprotect(s.Conn().RemotePeer(), "RetrievalQuery")
		_ = s.Close()
	}()

	// We have connected

	q := &legacyretrievaltypes.Query{
		PayloadCID: pcid,
	}

	var resp legacyretrievaltypes.QueryResponse
	if err := doRpc(ctx, s, q, &resp); err != nil {
		return nil, fmt.Errorf("retrieval query rpc: %w", err)
	}

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
}

func (c *Client) RetrieveContentWithProgressCallback(
	ctx context.Context,
	miner address.Address,
	proposal *legacyretrievaltypes.DealProposal,
	progressCallback func(bytesReceived uint64),
) (*RetrievalStats, error) {

	log.Infof("Starting retrieval with miner: %s", miner)

	minerPeer, err := c.MinerPeer(ctx, miner)
	if err != nil {
		return nil, err
	}
	minerOwnerWallet, err := c.minerOwner(ctx, miner)
	if err != nil {
		return nil, err
	}
	return c.retrieveContentFromPeerWithProgressCallback(ctx, minerPeer.ID, minerOwnerWallet, proposal, progressCallback, nil)
}

func (c *Client) retrieveContentFromPeerWithProgressCallback(
	ctx context.Context,
	peerID peer.ID,
	minerWallet address.Address,
	proposal *legacyretrievaltypes.DealProposal,
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
	var chanid datatransfer2.ChannelID
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

	unsubscribe := c.dataTransfer.SubscribeToEvents(func(event datatransfer2.Event, state datatransfer2.ChannelState) {
		// Copy chanid so, it can be used later in the callback
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
		case datatransfer2.Open:
		case datatransfer2.Accept:
		case datatransfer2.Restart:
		case datatransfer2.DataReceived:
			silenceEventCode = true
		case datatransfer2.DataSent:
		case datatransfer2.Cancel:
		case datatransfer2.Error:
			finish(fmt.Errorf("datatransfer error: %s", event.Message))
			return
		case datatransfer2.CleanupComplete:
			finish(nil)
			return
		case datatransfer2.NewVoucher:
		case datatransfer2.NewVoucherResult:

			switch resType := state.LastVoucherResult().(type) {
			case *legacyretrievaltypes.DealResponse:
				if len(resType.Message) != 0 {
					log.Debugf("Received deal response voucher result %s (%v): %s\n\t%+v", resType.Status, resType.Status, resType.Message, resType)
				} else {
					log.Debugf("Received deal response voucher result %s (%v)\n\t%+v", resType.Status, resType.Status, resType)
				}

				switch resType.Status {
				case legacyretrievaltypes.DealStatusAccepted:
					log.Info("Deal accepted")

				// Respond with a payment voucher when funds are requested
				case legacyretrievaltypes.DealStatusFundsNeeded, legacyretrievaltypes.DealStatusFundsNeededLastPayment:
					if pchRequired {
						finish(errors.New("payment channel required"))
						return
					} else {
						finish(fmt.Errorf("the miner requested payment even though this transaction was determined to be zero cost"))
						return
					}
				case legacyretrievaltypes.DealStatusRejected:
					finish(fmt.Errorf("deal rejected: %s", resType.Message))
					return
				case legacyretrievaltypes.DealStatusFundsNeededUnseal, legacyretrievaltypes.DealStatusUnsealing:
					finish(fmt.Errorf("data is sealed"))
					return
				case legacyretrievaltypes.DealStatusCancelled:
					finish(fmt.Errorf("deal cancelled: %s", resType.Message))
					return
				case legacyretrievaltypes.DealStatusErrored:
					finish(fmt.Errorf("deal errored: %s", resType.Message))
					return
				case legacyretrievaltypes.DealStatusCompleted:
					if allBytesReceived {
						finish(nil)
						return
					}
					dealComplete = true
				}
			}
		case datatransfer2.PauseInitiator:
		case datatransfer2.ResumeInitiator:
		case datatransfer2.PauseResponder:
		case datatransfer2.ResumeResponder:
		case datatransfer2.FinishTransfer:
			if dealComplete {
				finish(nil)
				return
			}
			allBytesReceived = true
		case datatransfer2.ResponderCompletes:
		case datatransfer2.ResponderBeginsFinalization:
		case datatransfer2.BeginFinalizing:
		case datatransfer2.Disconnected:
		case datatransfer2.Complete:
		case datatransfer2.CompleteCleanupOnRestart:
		case datatransfer2.DataQueued:
		case datatransfer2.DataQueuedProgress:
		case datatransfer2.DataSentProgress:
		case datatransfer2.DataReceivedProgress:
			// First byte has been received

			// publish first byte event
			if !receivedFirstByte {
				receivedFirstByte = true
			}

			progressCallback(state.Received())
			silenceEventCode = true
		case datatransfer2.RequestTimedOut:
		case datatransfer2.SendDataError:
		case datatransfer2.ReceiveDataError:
		case datatransfer2.TransferRequestQueued:
		case datatransfer2.RequestCancelled:
		case datatransfer2.Opened:
		default:
			eventCodeNotHandled = true
		}

		name := datatransfer2.Events[event.Code]
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
	selector := selectorparse.CommonSelector_ExploreAllRecursively
	if proposal.SelectorSpecified() {
		var err error
		selector, err = ipld.Decode(proposal.Selector.Raw, dagcbor.Decode)
		if err != nil {
			return nil, fmt.Errorf("failed to decode selector from proposal: %w", err)
		}
	}
	newchid, err := c.dataTransfer.OpenPullDataChannel(ctx, peerID, proposal, proposal.PayloadCID, selector)
	if err != nil {
		// We could fail before a successful proposal
		// publish event failure
		return nil, err
	}

	// Deal has been proposed

	chanidLk.Lock()
	chanid = newchid
	chanidLk.Unlock()

	// Wait for the retrieval to finish before exiting the function
awaitfinished:
	for {
		select {
		case err := <-dtRes:
			if err != nil {
				go func() {
					_ = c.dataTransfer.CloseDataTransferChannel(ctx, chanid)
				}()

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
		return nil, fmt.Errorf("could not get query blockstore: %w", err)
	} else if !has {
		return nil, errors.New("data transfer failed: unconfirmed block transfer")
	}

	// Compile the retrieval stats

	state, err := c.dataTransfer.ChannelState(ctx, chanid)
	if err != nil {
		return nil, fmt.Errorf("could not get channel state: %w", err)
	}

	duration := time.Since(startTime)
	speed := uint64(float64(state.Received()) / duration.Seconds())

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

func RetrievalProposalForAsk(ask *legacyretrievaltypes.QueryResponse, c cid.Cid, optionalSelector ipld.Node) (*legacyretrievaltypes.DealProposal, error) {
	if optionalSelector == nil {
		optionalSelector = selectorparse.CommonSelector_ExploreAllRecursively
	}

	params, err := legacyretrievaltypes.NewParamsV1(
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
	return &legacyretrievaltypes.DealProposal{
		PayloadCID: c,
		ID:         legacyretrievaltypes.DealID(dealIdGen.Next()),
		Params:     params,
	}, nil
}
