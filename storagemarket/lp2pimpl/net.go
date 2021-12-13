package lp2pimpl

import (
	"context"
	"time"

	"golang.org/x/xerrors"

	cborutil "github.com/filecoin-project/go-cbor-util"

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
const providerReadDeadline = 10 * time.Second
const providerWriteDeadline = 10 * time.Second
const clientReadDeadline = 10 * time.Second
const clientWriteDeadline = 10 * time.Second

// DealClientOption is an option for configuring the libp2p storage deal client
type DealClientOption func(*DealClient)

// RetryParameters changes the default parameters around connection reopening
func RetryParameters(minDuration time.Duration, maxDuration time.Duration, attempts float64, backoffFactor float64) DealClientOption {
	return func(c *DealClient) {
		c.retryStream.SetOptions(shared.RetryParameters(minDuration, maxDuration, attempts, backoffFactor))
	}
}

// DealClient sends deal proposals over libp2p
type DealClient struct {
	retryStream *shared.RetryStream
}

// SendDealProposal sends a deal proposal over a libp2p stream to the peer
func (c *DealClient) SendDealProposal(ctx context.Context, id peer.ID, params types.DealParams) (*types.DealResponse, error) {
	log.Debugw("send deal proposal", "id", params.DealUUID, "provider-peer", id)

	// Create a libp2p stream to the provider
	s, err := c.retryStream.OpenStream(ctx, id, []protocol.ID{DealProtocolID})
	if err != nil {
		return nil, err
	}

	defer s.Close() // nolint

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(clientWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	// Write the deal proposal to the stream
	if err = cborutil.WriteCborRPC(s, &params); err != nil {
		return nil, xerrors.Errorf("sending deal proposal: %w", err)
	}

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(clientReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	// Read the response from the stream
	var resp types.DealResponse
	if err := resp.UnmarshalCBOR(s); err != nil {
		return nil, xerrors.Errorf("reading proposal response: %w", err)
	}

	log.Debugw("received deal proposal response", "id", params.DealUUID, "accepted", resp.Accepted, "reason", resp.Message)

	return &resp, nil
}

func NewDealClient(h host.Host, options ...DealClientOption) *DealClient {
	c := &DealClient{
		retryStream: shared.NewRetryStream(h),
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// DealProvider listens for incoming deal proposals over libp2p
type DealProvider struct {
	host host.Host
	prov *storagemarket.Provider
}

func NewDealProvider(h host.Host, prov *storagemarket.Provider) *DealProvider {
	p := &DealProvider{
		host: h,
		prov: prov,
	}
	return p
}

func (p *DealProvider) Start() {
	p.host.SetStreamHandler(DealProtocolID, p.handleNewDealStream)
}

func (p *DealProvider) Stop() {
	p.host.RemoveStreamHandler(DealProtocolID)
}

// Called when the client opens a libp2p stream with a new deal proposal
func (p *DealProvider) handleNewDealStream(s network.Stream) {
	defer s.Close()

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(providerReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	// Read the deal proposal from the stream
	var proposal types.DealParams
	err := proposal.UnmarshalCBOR(s)
	if err != nil {
		log.Warnw("reading storage deal proposal from stream", "err", err)
		return
	}

	log.Infow("received deal proposal", "id", proposal.DealUUID, "client-peer", s.Conn().RemotePeer())

	// Start executing the deal.
	// Note: This method just waits for the deal to be accepted, it doesn't
	// wait for deal execution to complete.
	res, err := p.prov.ExecuteDeal(&proposal)
	if err != nil {
		log.Warnw("executing deal proposal", "id", proposal.DealUUID, "err", err)
		return
	}

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(providerWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	// Write the response to the client
	log.Infow("send deal proposal response", "id", proposal.DealUUID, "accepted", res.Accepted, "msg", res.Reason)
	err = cborutil.WriteCborRPC(s, &types.DealResponse{Accepted: res.Accepted, Message: res.Reason})
	if err != nil {
		log.Warnw("writing deal response", "id", proposal.DealUUID, "err", err)
		return
	}
}
