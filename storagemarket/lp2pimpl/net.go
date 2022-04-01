package lp2pimpl

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-fil-markets/shared"
	"github.com/filecoin-project/lotus/api/v0api"
	chaintypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/sigs"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"golang.org/x/xerrors"
)

var log = logging.Logger("boost-net")

const DealProtocolID = "/fil/storage/mk/1.2.0"
const DealStatusV12ProtocolID = "/fil/storage/status/1.2.0"
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
	addr        address.Address
	retryStream *shared.RetryStream
	fullNode    v0api.FullNode
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

func (c *DealClient) SendDealStatusRequest(ctx context.Context, id peer.ID, dealUUID uuid.UUID) (*types.DealStatusResponse, error) {
	log.Debugw("send deal status req", "id", dealUUID)

	uuidBytes, err := dealUUID.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("getting uuid bytes: %w", err)
	}

	sig, err := c.fullNode.WalletSign(ctx, c.addr, uuidBytes)
	if err != nil {
		return nil, fmt.Errorf("signing uuid bytes: %w", err)
	}

	// Create a libp2p stream to the provider
	s, err := c.retryStream.OpenStream(ctx, id, []protocol.ID{DealStatusV12ProtocolID})
	if err != nil {
		return nil, err
	}

	defer s.Close() // nolint

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(clientWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	// Write the deal status request to the stream
	req := types.DealStatusRequest{DealUUID: dealUUID, Signature: *sig}
	if err = cborutil.WriteCborRPC(s, &req); err != nil {
		return nil, xerrors.Errorf("sending deal status req: %w", err)
	}

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(clientReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	// Read the response from the stream
	var resp types.DealStatusResponse
	if err := resp.UnmarshalCBOR(s); err != nil {
		return nil, xerrors.Errorf("reading deal status response: %w", err)
	}

	log.Debugw("received deal status response", "id", resp.DealUUID, "status", resp.DealStatus)

	return &resp, nil
}

func NewDealClient(h host.Host, addr address.Address, fullNodeApi v0api.FullNode, options ...DealClientOption) *DealClient {
	c := &DealClient{
		addr:        addr,
		retryStream: shared.NewRetryStream(h),
		fullNode:    fullNodeApi,
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// DealProvider listens for incoming deal proposals over libp2p
type DealProvider struct {
	ctx      context.Context
	host     host.Host
	prov     *storagemarket.Provider
	fullNode v0api.FullNode
}

func NewDealProvider(h host.Host, prov *storagemarket.Provider, fullNodeApi v0api.FullNode) *DealProvider {
	p := &DealProvider{
		host:     h,
		prov:     prov,
		fullNode: fullNodeApi,
	}
	return p
}

func (p *DealProvider) Start(ctx context.Context) {
	p.ctx = ctx
	p.host.SetStreamHandler(DealProtocolID, p.handleNewDealStream)
	p.host.SetStreamHandler(DealStatusV12ProtocolID, p.handleNewDealStatusStream)
}

func (p *DealProvider) Stop() {
	p.host.RemoveStreamHandler(DealProtocolID)
	p.host.RemoveStreamHandler(DealStatusV12ProtocolID)
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
	res, _, err := p.prov.ExecuteDeal(&proposal, s.Conn().RemotePeer())
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

func (p *DealProvider) handleNewDealStatusStream(s network.Stream) {
	defer s.Close()

	_ = s.SetReadDeadline(time.Now().Add(providerReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	var req types.DealStatusRequest
	err := req.UnmarshalCBOR(s)
	if err != nil {
		log.Warnw("reading deal status request from stream", "err", err)
		return
	}
	log.Debugw("received deal status request", "id", req.DealUUID, "client-peer", s.Conn().RemotePeer())

	resp := p.getDealStatus(req)

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(providerWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	if err := cborutil.WriteCborRPC(s, &resp); err != nil {
		log.Errorw("failed to write deal status response", "err", err)
		return
	}
}

func (p *DealProvider) getDealStatus(req types.DealStatusRequest) types.DealStatusResponse {
	errResp := func(err string) types.DealStatusResponse {
		return types.DealStatusResponse{DealUUID: req.DealUUID, Error: err}
	}

	pds, err := p.prov.Deal(p.ctx, req.DealUUID)
	if err != nil && xerrors.Is(err, storagemarket.ErrDealNotFound) {
		return errResp(fmt.Sprintf("no storage deal found with deal UUID %s", req.DealUUID))
	}

	if err != nil {
		log.Errorw("failed to fetch deal status", "err", err)
		return errResp("failed to fetch deal status")
	}

	// verify request signature
	uuidBytes, err := req.DealUUID.MarshalBinary()
	if err != nil {
		log.Errorw("failed to serialize request deal UUID", "err", err)
		return errResp("failed to serialize request deal UUID")
	}

	clientAddr := pds.ClientDealProposal.Proposal.Client
	addr, err := p.fullNode.StateAccountKey(p.ctx, clientAddr, chaintypes.EmptyTSK)
	if err != nil {
		log.Errorw("failed to get account key for client addr", "client", clientAddr.String(), "err", err)
		msg := fmt.Sprintf("failed to get account key for client addr %s", clientAddr.String())
		return errResp(msg)
	}

	err = sigs.Verify(&req.Signature, addr, uuidBytes)
	if err != nil {
		log.Warnw("signature verification failed", "err", err)
		return errResp("signature verification failed")
	}

	signedPropCid, err := pds.SignedProposalCid()
	if err != nil {
		log.Errorw("getting signed proposal cid", "err", err)
		return errResp("getting signed proposal cid")
	}

	return types.DealStatusResponse{
		DealUUID: req.DealUUID,
		DealStatus: &types.DealStatus{
			Error:             pds.Err,
			Status:            pds.Checkpoint.String(),
			Proposal:          pds.ClientDealProposal.Proposal,
			SignedProposalCid: signedPropCid,
			PublishCid:        pds.PublishCID,
			ChainDealID:       pds.ChainDealID,
		},
	}
}
