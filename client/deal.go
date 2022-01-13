package client

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/boost/api"
	"github.com/filecoin-project/boost/storagemarket/lp2pimpl"
	"github.com/filecoin-project/boost/storagemarket/types"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
)

// StorageClient starts storage deals with Boost over libp2p
type StorageClient struct {
	PeerStore  peerstore.Peerstore
	dealClient *lp2pimpl.DealClient
}

func NewStorageClient(ctx context.Context) (*StorageClient, error) {
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
		dealClient: lp2pimpl.NewDealClient(h, retryOpts),
		PeerStore:  pstore,
	}, nil
}

func (c *StorageClient) StorageDeal(ctx context.Context, params types.DealParams, providerID peer.ID) (*api.ProviderDealRejectionInfo, error) {
	// Send the deal proposal to the provider
	resp, err := c.dealClient.SendDealProposal(ctx, providerID, params)
	if err != nil {
		return nil, xerrors.Errorf("sending deal proposal: %w", err)
	}

	return &api.ProviderDealRejectionInfo{
		Accepted: resp.Accepted,
		Reason:   resp.Message,
	}, nil
}
