package client

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/boost/retrievalmarket/lp2pimpl"
	"github.com/filecoin-project/boost/retrievalmarket/types"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-peerstore/pstoremem"
)

// RetrievalClient runs retrieval queries with Boost over libp2p
type RetrievalClient struct {
	PeerStore   peerstore.Peerstore
	queryClient *lp2pimpl.QueryClient
}

// NewRetrievalClient sets up a libp2p host to run retrieval query ask v2 queries
func NewRetrievalClient() (*RetrievalClient, error) {
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
	return &RetrievalClient{
		queryClient: lp2pimpl.NewQueryClient(h, retryOpts),
		PeerStore:   pstore,
	}, nil
}

// Query sends a retrieval query v2 to another peer
func (c *RetrievalClient) Query(ctx context.Context, providerID peer.ID, query types.SignedQuery) (*types.QueryResponse, error) {
	// Send the deal proposal to the provider
	return c.queryClient.SendQuery(ctx, providerID, query)
}
