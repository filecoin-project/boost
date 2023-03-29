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
		addr := a.String()
		addrs = append(addrs, &addr)
	}

	protos := make([]*string, 0, len(r.h.Mux().Protocols()))
	for _, proto := range r.h.Mux().Protocols() {
		cp := string(proto)
		protos = append(protos, &cp)
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
