package client

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/storagemarket/lp2pimpl"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api/v1api"
	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
)

// StorageClient starts storage deals with Boost over libp2p
type StorageClient struct {
	PeerStore  peerstore.Peerstore
	dealClient *lp2pimpl.DealClient
}

func NewStorageClient(addr address.Address, fullNodeApi v1api.FullNode) (*StorageClient, error) {
	pstore, err := pstoremem.NewPeerstore()
	if err != nil {
		return nil, fmt.Errorf("creating peer store: %w", err)
	}
	opts := []libp2p.Option{
		libp2p.DefaultTransports,
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
		libp2p.Peerstore(pstore),
		libp2p.NoListenAddrs,
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}

	retryOpts := lp2pimpl.RetryParameters(time.Millisecond, time.Millisecond, 1, 1)
	return &StorageClient{
		dealClient: lp2pimpl.NewDealClient(h, addr, fullNodeApi, retryOpts),
		PeerStore:  pstore,
	}, nil
}

func (c *StorageClient) StorageDeal(ctx context.Context, params types.DealParams, providerID peer.ID) (*api.ProviderDealRejectionInfo, error) {
	// Send the deal proposal to the provider
	resp, err := c.dealClient.SendDealProposal(ctx, providerID, params)
	if err != nil {
		return nil, fmt.Errorf("sending deal proposal: %w", err)
	}

	return &api.ProviderDealRejectionInfo{
		Accepted: resp.Accepted,
		Reason:   resp.Message,
	}, nil
}

func (c *StorageClient) DealStatus(ctx context.Context, providerID peer.ID, dealUUid uuid.UUID) (*types.DealStatusResponse, error) {
	// Send the deal proposal to the provider
	return c.dealClient.SendDealStatusRequest(ctx, providerID, dealUUid)
}
