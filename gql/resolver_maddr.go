package gql

func (r *resolver) MinerAddress() string {
	return r.provider.Address.String()
}

func (r *resolver) GraphsyncRetrievalMinerAddresses() []string {
	addrs := r.mma.GetMinerAddresses()
	strs := make([]string, 0, len(addrs))
	for _, a := range addrs {
		strs = append(strs, a.String())
	}
	return strs
}
