package lp2pimpl

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/filecoin-project/boost-gfm/shared"
	gfm_storagemarket "github.com/filecoin-project/boost-gfm/storagemarket"
	gfm_migration "github.com/filecoin-project/boost-gfm/storagemarket/migrations"
	gfm_network "github.com/filecoin-project/boost-gfm/storagemarket/network"
	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/db"
	"github.com/filecoin-project/boost/storagemarket"
	"github.com/filecoin-project/boost/storagemarket/sealingpipeline"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	cborutil "github.com/filecoin-project/go-cbor-util"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/lotus/api/v1api"
	chaintypes "github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/sigs"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	typegen "github.com/whyrusleeping/cbor-gen"
	"go.uber.org/zap"
)

var log = logging.Logger("boost-net")
var propLog = logging.Logger("boost-prop")

const DealProtocolv120ID = "/fil/storage/mk/1.2.0"
const DealProtocolv121ID = "/fil/storage/mk/1.2.1"
const DealStatusV12ProtocolID = "/fil/storage/status/1.2.0"

// The time limit to read a message from the client when the client opens a stream
const providerReadDeadline = 10 * time.Second

// The time limit to write a response to the client
const providerWriteDeadline = 10 * time.Second

// The time limit to wait for the provider to send a response to a client's request.
// This includes the time it takes for the provider to process the request and
// send a response.
const clientReadDeadline = 60 * time.Second

// The time limit to write a message to the provider
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
	walletApi   api.Wallet
}

// SendDealProposal sends a deal proposal over a libp2p stream to the peer
func (c *DealClient) SendDealProposal(ctx context.Context, id peer.ID, params types.DealParams) (*types.DealResponse, error) {
	log.Debugw("send deal proposal", "id", params.DealUUID, "provider-peer", id)

	// Create a libp2p stream to the provider
	s, err := c.retryStream.OpenStream(ctx, id, []protocol.ID{DealProtocolv121ID, DealProtocolv120ID})
	if err != nil {
		return nil, err
	}

	defer s.Close() // nolint

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(clientWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	// Write the deal proposal to the stream
	if err = cborutil.WriteCborRPC(s, &params); err != nil {
		return nil, fmt.Errorf("sending deal proposal: %w", err)
	}

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(clientReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	// Read the response from the stream
	var resp types.DealResponse
	if err := resp.UnmarshalCBOR(s); err != nil {
		return nil, fmt.Errorf("reading proposal response: %w", err)
	}

	log.Debugw("received deal proposal response", "id", params.DealUUID, "accepted", resp.Accepted, "reason", resp.Message)

	return &resp, nil
}

func (c *DealClient) SendDealStatusRequest(ctx context.Context, id peer.ID, dealUUID uuid.UUID) (*types.DealStatusResponse, error) {
	log.Debugw("send deal status req", "deal-uuid", dealUUID, "id", id)

	uuidBytes, err := dealUUID.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("getting uuid bytes: %w", err)
	}

	sig, err := c.walletApi.WalletSign(ctx, c.addr, uuidBytes)
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
		return nil, fmt.Errorf("sending deal status req: %w", err)
	}

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(clientReadDeadline))
	defer s.SetReadDeadline(time.Time{}) // nolint

	// Read the response from the stream
	var resp types.DealStatusResponse
	if err := resp.UnmarshalCBOR(s); err != nil {
		return nil, fmt.Errorf("reading deal status response: %w", err)
	}

	log.Debugw("received deal status response", "id", resp.DealUUID, "status", resp.DealStatus)

	return &resp, nil
}

func NewDealClient(h host.Host, addr address.Address, walletApi api.Wallet, options ...DealClientOption) *DealClient {
	c := &DealClient{
		addr:        addr,
		retryStream: shared.NewRetryStream(h),
		walletApi:   walletApi,
	}
	for _, option := range options {
		option(c)
	}
	return c
}

// DealProvider listens for incoming deal proposals over libp2p
type DealProvider struct {
	ctx               context.Context
	host              host.Host
	prov              *storagemarket.Provider
	fullNode          v1api.FullNode
	plDB              *db.ProposalLogsDB
	spApi             sealingpipeline.API
	enableLegacyDeals bool
}

func NewDealProvider(h host.Host, prov *storagemarket.Provider, fullNodeApi v1api.FullNode, plDB *db.ProposalLogsDB, spApi sealingpipeline.API, enableLegacyDeals bool) *DealProvider {
	p := &DealProvider{
		host:              h,
		prov:              prov,
		fullNode:          fullNodeApi,
		plDB:              plDB,
		spApi:             spApi,
		enableLegacyDeals: enableLegacyDeals,
	}
	return p
}

func (p *DealProvider) Start(ctx context.Context) {
	p.ctx = ctx

	// Note that the handling for deal protocol v1.2.0 and v1.2.1 is the same.
	// Deal protocol v1.2.1 has a couple of new fields: SkipIPNIAnnounce and
	// RemoveUnsealedCopy.
	// If a client that supports deal protocol v1.2.0 sends a request to a
	// boostd server that supports deal protocol v1.2.1, the DealParams struct
	// will be missing these new fields.
	// When the DealParams struct is unmarshalled the missing fields will be
	// set to false, which maintains the previous behaviour:
	// - SkipIPNIAnnounce=false:    announce deal to IPNI
	// - RemoveUnsealedCopy=false:  keep unsealed copy of deal data
	p.host.SetStreamHandler(DealProtocolv121ID, p.handleNewDealStream)
	p.host.SetStreamHandler(DealProtocolv120ID, p.handleNewDealStream)

	p.host.SetStreamHandler(DealStatusV12ProtocolID, p.handleNewDealStatusStream)

	// Handle legacy deal stream here and reject all legacy deals
	if !p.enableLegacyDeals {
		p.host.SetStreamHandler(gfm_storagemarket.DealProtocolID101, p.handleLegacyDealStream)
		p.host.SetStreamHandler(gfm_storagemarket.DealProtocolID110, p.handleLegacyDealStream)
		p.host.SetStreamHandler(gfm_storagemarket.DealProtocolID111, p.handleLegacyDealStream)
	}
}

func (p *DealProvider) Stop() {
	p.host.RemoveStreamHandler(DealProtocolv121ID)
	p.host.RemoveStreamHandler(DealProtocolv120ID)
	p.host.RemoveStreamHandler(DealStatusV12ProtocolID)
}

// Called when the client opens a libp2p stream with a new deal proposal
func (p *DealProvider) handleNewDealStream(s network.Stream) {
	start := time.Now()
	reqLogUuid := uuid.New()
	reqLog := log.With("reqlog-uuid", reqLogUuid.String(), "client-peer", s.Conn().RemotePeer())
	reqLog.Debugw("new deal proposal request")

	defer func() {
		err := s.Close()
		if err != nil {
			reqLog.Infow("closing stream", "err", err)
		}
		reqLog.Debugw("handled deal proposal request", "duration", time.Since(start).String())
	}()

	// Set a deadline on reading from the stream so it doesn't hang
	_ = s.SetReadDeadline(time.Now().Add(providerReadDeadline))

	// Read the deal proposal from the stream
	var proposal types.DealParams
	err := proposal.UnmarshalCBOR(s)
	_ = s.SetReadDeadline(time.Time{}) // Clear read deadline so conn doesn't get closed
	if err != nil {
		reqLog.Warnw("reading storage deal proposal from stream", "err", err)
		return
	}

	reqLog = reqLog.With("id", proposal.DealUUID)
	reqLog.Infow("received deal proposal")

	// Start executing the deal.
	// Note: This method just waits for the deal to be accepted, it doesn't
	// wait for deal execution to complete.
	startExec := time.Now()
	res, err := p.prov.ExecuteDeal(context.Background(), &proposal, s.Conn().RemotePeer())
	reqLog.Debugw("processed deal proposal accept")
	if err != nil {
		reqLog.Warnw("deal proposal failed", "err", err, "reason", res.Reason)
	}

	// Log the response
	propLog.Infow("send deal proposal response",
		"id", proposal.DealUUID,
		"accepted", res.Accepted,
		"msg", res.Reason,
		"peer id", s.Conn().RemotePeer(),
		"client address", proposal.ClientDealProposal.Proposal.Client,
		"provider address", proposal.ClientDealProposal.Proposal.Provider,
		"piece cid", proposal.ClientDealProposal.Proposal.PieceCID.String(),
		"piece size", proposal.ClientDealProposal.Proposal.PieceSize,
		"verified", proposal.ClientDealProposal.Proposal.VerifiedDeal,
		"label", proposal.ClientDealProposal.Proposal.Label,
		"start epoch", proposal.ClientDealProposal.Proposal.StartEpoch,
		"end epoch", proposal.ClientDealProposal.Proposal.EndEpoch,
		"price per epoch", proposal.ClientDealProposal.Proposal.StoragePricePerEpoch,
		"duration", time.Since(startExec).String(),
	)
	_ = p.plDB.InsertLog(p.ctx, proposal, res.Accepted, res.Reason) //nolint:errcheck

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(providerWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	// Write the response to the client
	err = cborutil.WriteCborRPC(s, &types.DealResponse{Accepted: res.Accepted, Message: res.Reason})
	if err != nil {
		reqLog.Warnw("writing deal response", "err", err)
	}
}

func (p *DealProvider) handleNewDealStatusStream(s network.Stream) {
	start := time.Now()
	reqLogUuid := uuid.New()
	reqLog := log.With("reqlog-uuid", reqLogUuid.String(), "client-peer", s.Conn().RemotePeer())
	reqLog.Debugw("new deal status request")

	defer func() {
		err := s.Close()
		if err != nil {
			reqLog.Infow("closing stream", "err", err)
		}
		reqLog.Debugw("handled deal status request", "duration", time.Since(start).String())
	}()

	// Read the deal status request from the stream
	_ = s.SetReadDeadline(time.Now().Add(providerReadDeadline))
	var req types.DealStatusRequest
	err := req.UnmarshalCBOR(s)
	_ = s.SetReadDeadline(time.Time{}) // Clear read deadline so conn doesn't get closed
	if err != nil {
		reqLog.Warnw("reading deal status request from stream", "err", err)
		return
	}
	reqLog = reqLog.With("id", req.DealUUID)
	reqLog.Debugw("received deal status request")

	resp := p.getDealStatus(req, reqLog)
	reqLog.Debugw("processed deal status request")

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(providerWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	if err := cborutil.WriteCborRPC(s, &resp); err != nil {
		reqLog.Errorw("failed to write deal status response", "err", err)
	}
}

func (p *DealProvider) getDealStatus(req types.DealStatusRequest, reqLog *zap.SugaredLogger) types.DealStatusResponse {
	errResp := func(err string) types.DealStatusResponse {
		return types.DealStatusResponse{DealUUID: req.DealUUID, Error: err}
	}

	pds, err := p.prov.Deal(p.ctx, req.DealUUID)
	if err != nil && errors.Is(err, storagemarket.ErrDealNotFound) {
		return errResp(fmt.Sprintf("no storage deal found with deal UUID %s", req.DealUUID))
	}

	if err != nil {
		reqLog.Errorw("failed to fetch deal status", "err", err)
		return errResp("failed to fetch deal status")
	}

	// verify request signature
	uuidBytes, err := req.DealUUID.MarshalBinary()
	if err != nil {
		reqLog.Errorw("failed to serialize request deal UUID", "err", err)
		return errResp("failed to serialize request deal UUID")
	}

	clientAddr := pds.ClientDealProposal.Proposal.Client
	addr, err := p.fullNode.StateAccountKey(p.ctx, clientAddr, chaintypes.EmptyTSK)
	if err != nil {
		reqLog.Errorw("failed to get account key for client addr", "client", clientAddr.String(), "err", err)
		msg := fmt.Sprintf("failed to get account key for client addr %s", clientAddr.String())
		return errResp(msg)
	}

	err = sigs.Verify(&req.Signature, addr, uuidBytes)
	if err != nil {
		reqLog.Warnw("signature verification failed", "err", err)
		return errResp("signature verification failed")
	}

	signedPropCid, err := pds.SignedProposalCid()
	if err != nil {
		reqLog.Errorw("getting signed proposal cid", "err", err)
		return errResp("getting signed proposal cid")
	}

	bts := p.prov.NBytesReceived(req.DealUUID)

	si, err := p.spApi.SectorsStatus(p.ctx, pds.SectorID, false)
	if err != nil {
		reqLog.Errorw("getting sector status from sealer", "err", err)
		return errResp("getting sector status from sealer")
	}

	sealingStatus := string(si.State)

	if storagemarket.IsFinalSealingState(si.State) {
		if !storagemarket.HasDeal(si.Deals, pds.ChainDealID) {
			sealingStatus = storagemarket.ErrDealNotFound.Error()
		}
	}

	return types.DealStatusResponse{
		DealUUID: req.DealUUID,
		DealStatus: &types.DealStatus{
			Error:             pds.Err,
			Status:            pds.Checkpoint.String(),
			SealingStatus:     sealingStatus,
			Proposal:          pds.ClientDealProposal.Proposal,
			SignedProposalCid: signedPropCid,
			PublishCid:        pds.PublishCID,
			ChainDealID:       pds.ChainDealID,
		},
		IsOffline:      pds.IsOffline,
		TransferSize:   pds.Transfer.Size,
		NBytesReceived: bts,
	}
}

func (p *DealProvider) handleLegacyDealStream(s network.Stream) {
	start := time.Now()
	reqLogUuid := uuid.New()
	reqLog := log.With("reqlog-uuid", reqLogUuid.String(), "client-peer", s.Conn().RemotePeer())
	reqLog.Debugw("new legacy deal status request")
	defer func() {
		err := s.Close()
		if err != nil {
			reqLog.Infow("closing stream", "err", err)
		}
		reqLog.Debugw("handled legacy deal status request", "duration", time.Since(start).String())
	}()

	rejMsg := fmt.Sprintf("deal proposals made over the legacy %s protocol are deprecated"+
		" - please use the %s deal proposal protocol", s.Protocol(), DealProtocolv121ID)
	const rejState = gfm_storagemarket.StorageDealProposalRejected
	var signedResponse typegen.CBORMarshaler

	_ = s.SetReadDeadline(time.Now().Add(providerReadDeadline))
	switch s.Protocol() {
	case gfm_storagemarket.DealProtocolID101:
		var prop gfm_migration.Proposal0
		err := prop.UnmarshalCBOR(s)
		_ = s.SetReadDeadline(time.Time{}) // Clear read deadline so conn doesn't get closed
		if err != nil {
			reqLog.Errorf("failed to unmarshal the proposal message: %s", err)
			return
		}

		pcid, err := prop.DealProposal.Proposal.Cid()
		if err != nil {
			reqLog.Errorf("failed to get the proposal cid: %s", err)
			return
		}

		resp := gfm_migration.Response0{State: rejState, Message: rejMsg, Proposal: pcid}
		sig, err := p.signLegacyResponse(&resp)
		if err != nil {
			reqLog.Errorf("getting signed response: %s", err)
			return
		}

		signedResponse = &gfm_migration.SignedResponse0{Response: resp, Signature: sig}

	case gfm_storagemarket.DealProtocolID110:
		var prop gfm_migration.Proposal1
		err := prop.UnmarshalCBOR(s)
		_ = s.SetReadDeadline(time.Time{}) // Clear read deadline so conn doesn't get closed
		if err != nil {
			reqLog.Errorf("failed to unmarshal the proposal message: %s", err)
			return
		}

		pcid, err := prop.DealProposal.Proposal.Cid()
		if err != nil {
			reqLog.Errorf("failed to get the proposal cid: %s", err)
			return
		}

		resp := gfm_network.Response{State: rejState, Message: rejMsg, Proposal: pcid}
		sig, err := p.signLegacyResponse(&resp)
		if err != nil {
			reqLog.Errorf("getting signed response: %s", err)
			return
		}

		signedResponse = &gfm_network.SignedResponse{Response: resp, Signature: sig}

	case gfm_storagemarket.DealProtocolID111:
		var prop gfm_network.Proposal
		err := prop.UnmarshalCBOR(s)
		_ = s.SetReadDeadline(time.Time{}) // Clear read deadline so conn doesn't get closed
		if err != nil {
			reqLog.Errorf("failed to unmarshal the proposal message: %s", err)
			return
		}

		pcid, err := prop.DealProposal.Proposal.Cid()
		if err != nil {
			reqLog.Errorf("failed to get the proposal cid: %s", err)
			return
		}

		resp := gfm_network.Response{State: rejState, Message: rejMsg, Proposal: pcid}
		sig, err := p.signLegacyResponse(&resp)
		if err != nil {
			reqLog.Errorf("getting signed response: %s", err)
			return
		}

		signedResponse = &gfm_network.SignedResponse{Response: resp, Signature: sig}
	}

	// Set a deadline on writing to the stream so it doesn't hang
	_ = s.SetWriteDeadline(time.Now().Add(providerWriteDeadline))
	defer s.SetWriteDeadline(time.Time{}) // nolint

	err := signedResponse.MarshalCBOR(s)
	if err != nil {
		reqLog.Errorf("error writing response to the stream: %s", err)
	}
}

func (p *DealProvider) signLegacyResponse(resp typegen.CBORMarshaler) (*crypto.Signature, error) {
	ts, err := p.fullNode.ChainHead(p.ctx)
	if err != nil {
		return nil, fmt.Errorf("getting chain head: %w", err)
	}

	maddr, err := p.spApi.ActorAddress(p.ctx)
	if err != nil {
		return nil, fmt.Errorf("getting miner actor address: %w", err)
	}

	mi, err := p.fullNode.StateMinerInfo(p.ctx, maddr, ts.Key())
	if err != nil {
		return nil, fmt.Errorf("getting miner info: %w", err)
	}

	msg, err := cborutil.Dump(resp)
	if err != nil {
		return nil, fmt.Errorf("could not convert response to bytes: %w", err)
	}

	localSignature, err := p.fullNode.WalletSign(p.ctx, mi.Worker, msg)
	if err != nil {
		return nil, fmt.Errorf("failed to sign the message: %w", err)
	}

	return localSignature, err
}
