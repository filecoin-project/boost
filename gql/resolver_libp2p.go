package gql

import "sort"

type libp2pAddrInfoResolver struct {
	Addresses []*string
	PeerID    string
	Protocols []*string
}

func (r *resolver) Libp2pAddrInfo() (*libp2pAddrInfoResolver, error) {
	addrs := make([]*string, 0, len(r.h.Addrs()))
	for _, a := range r.h.Addrs() {
		addrs = append(addrs, new(a.String()))
	}

	protos := make([]*string, 0, len(r.h.Mux().Protocols()))
	for _, proto := range r.h.Mux().Protocols() {
		protos = append(protos, new(string(proto)))
	}

	sort.Slice(protos, func(i, j int) bool {
		return *protos[i] < *protos[j]
	})

	return &libp2pAddrInfoResolver{
		Addresses: addrs,
		PeerID:    r.h.ID().String(),
		Protocols: protos,
	}, nil
}
