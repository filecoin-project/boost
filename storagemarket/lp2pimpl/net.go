package lp2pimpl

import (
	"bufio"
	"context"
	"time"

	ma "github.com/multiformats/go-multiaddr"

	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-fil-markets/shared"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
)

var log = logging.Logger("boost-net")

const DealProtocolID = "/fil/storage/mk/1.2.0"

// TagPriority is the priority of deal streams in the connection manager
const TagPriority = 100

// Option is an option for configuring the libp2p storage market network
type Option func(*dealNet)

// RetryParameters changes the default parameters around connection reopening
func RetryParameters(minDuration time.Duration, maxDuration time.Duration, attempts float64, backoffFactor float64) Option {
	return func(n *dealNet) {
		n.retryStream.SetOptions(shared.RetryParameters(minDuration, maxDuration, attempts, backoffFactor))
	}
}

type dealNet struct {
	host        host.Host
	retryStream *shared.RetryStream
}

// NewDealStream creates a new libp2p stream to send deal proposals to the
// given peer
func (n *dealNet) NewDealStream(ctx context.Context, id peer.ID) (*DealStream, error) {
	s, err := n.retryStream.OpenStream(ctx, id, []protocol.ID{DealProtocolID})
	if err != nil {
		return nil, err
	}
	buffered := bufio.NewReaderSize(s, 16)
	return &DealStream{p: id, rw: s, buffered: buffered, host: n.host}, nil
}

func (n *dealNet) ID() peer.ID {
	return n.host.ID()
}

func (n *dealNet) AddAddrs(p peer.ID, addrs []ma.Multiaddr) {
	n.host.Peerstore().AddAddrs(p, addrs, 8*time.Hour)
}

func (n *dealNet) TagPeer(p peer.ID, id string) {
	n.host.ConnManager().TagPeer(p, id, TagPriority)
}

func (n *dealNet) UntagPeer(p peer.ID, id string) {
	n.host.ConnManager().UntagPeer(p, id)
}

// DealClient sends deal proposals over libp2p
type DealClient struct {
	dealNet
}

func NewClient(h host.Host, options ...Option) *DealClient {
	c := &DealClient{
		dealNet: dealNet{
			host:        h,
			retryStream: shared.NewRetryStream(h),
		},
	}
	for _, option := range options {
		option(&c.dealNet)
	}
	return c
}

// DealProvider listens for incoming deal proposals over libp2p
type DealProvider struct {
	dealNet
	prov *storagemarket.Provider
}

func NewProvider(h host.Host, prov *storagemarket.Provider, options ...Option) *DealProvider {
	p := &DealProvider{
		dealNet: dealNet{
			host:        h,
			retryStream: shared.NewRetryStream(h),
		},
		prov: prov,
	}
	for _, option := range options {
		option(&p.dealNet)
	}
	return p
}

func (n *DealProvider) Start() {
	n.host.SetStreamHandler(DealProtocolID, n.handleNewDealStream)
}

func (n *DealProvider) Stop() {
	n.host.RemoveStreamHandler(DealProtocolID)
}

// Called when the client opens a libp2p stream with a new deal proposal
func (n *DealProvider) handleNewDealStream(s network.Stream) {
	defer s.Close()

	// Read the deal proposal from the stream
	reader := bufio.NewReaderSize(s, 16)
	ds := &DealStream{s.Conn().RemotePeer(), n.host, s, reader}
	proposal, err := ds.ReadDealProposal()
	if err != nil {
		log.Warnw("reading storage deal proposal from stream", "id", proposal.DealUUID, "err", err)
		return
	}

	log.Infow("new libp2p deal proposal", "id", proposal.DealUUID, "client-peer", s.Conn().RemotePeer())

	// Start executing the deal.
	// Note: This method just waits for the deal to be accepted, it doesn't
	// wait for deal execution to complete.
	res, err := n.prov.ExecuteDeal(&proposal)
	if err != nil {
		log.Warnw("executing deal proposal", "id", proposal.DealUUID, "err", err)
		return
	}

	// If the deal was accepted, res is nil
	message := ""
	if res != nil {
		// If the deal was rejected, res.Reason is the rejection reason
		message = res.Reason
	}

	// Write the response to the client
	log.Infow("send deal proposal response", "id", proposal.DealUUID, "msg", message)
	err = ds.WriteDealResponse(types.DealResponse{Message: message})
	if err != nil {
		log.Warnw("writing deal response", "id", proposal.DealUUID, "err", err)
		return
	}
}
