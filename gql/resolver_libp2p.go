package gql

import (
	"context"
	"fmt"
)

type libp2pAddrInfoResolver struct {
	Addresses []*string
	PeerID    string
}

func (r *resolver) Libp2pAddrInfo(ctx context.Context) (*libp2pAddrInfoResolver, error) {
	addrInfo, err := r.fullNode.NetAddrsListen(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting p2p listen address: %w", err)
	}

	addrs := make([]*string, 0, len(addrInfo.Addrs))
	for _, a := range addrInfo.Addrs {
		addr := a.String()
		addrs = append(addrs, &addr)
	}

	return &libp2pAddrInfoResolver{
		Addresses: addrs,
		PeerID:    addrInfo.ID.String(),
	}, nil
}
