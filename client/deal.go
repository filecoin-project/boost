package client

import (
	"context"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/storagemarket/lp2pimpl"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
)

// Client starts storage deals with Boost over libp2p
type Client struct {
	PeerStore  peerstore.Peerstore
	dealClient *lp2pimpl.DealClient
}

func NewClient(ctx context.Context) (*Client, error) {
	pstore := pstoremem.NewPeerstore()
	opts := []libp2p.Option{
		libp2p.DefaultTransports,
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.Peerstore(pstore),
		libp2p.NoListenAddrs,
	}

	h, err := libp2p.New(ctx, opts...)
	if err != nil {
		return nil, err
	}

	retryOpts := lp2pimpl.RetryParameters(time.Millisecond, time.Millisecond, 1, 1)
	return &Client{
		dealClient: lp2pimpl.NewClient(h, retryOpts),
		PeerStore:  pstore,
	}, nil
}

func (c *Client) StorageDeal(ctx context.Context, params types.DealParams) (*api.ProviderDealRejectionInfo, error) {
	// Create a libp2p stream to boost
	ds, err := c.dealClient.NewDealStream(ctx, params.MinerPeerID)
	if err != nil {
		return nil, xerrors.Errorf("opening dealstream to storage provider peer %s: %w", params.MinerPeerID, err)
	}

	// Write the deal proposal to the stream
	if err = ds.WriteDealProposal(params); err != nil {
		return nil, xerrors.Errorf("sending deal proposal: %w", err)
	}

	// Read the response from the stream
	resp, err := ds.ReadDealResponse()
	if err != nil {
		return nil, xerrors.Errorf("reading proposal response: %w", err)
	}

	// Check if there was deal rejection message
	if resp.Message != "" {
		return &api.ProviderDealRejectionInfo{Reason: resp.Message}, nil
	}

	// Deal was accepted
	return nil, nil
}
