package gql

func (r *resolver) MinerAddress() string {
	// TODO: this function doesn;t seem to be used at all. Confirm.
	return r.provider.Addresses[0].String()
}

func (r *resolver) GraphsyncRetrievalMinerAddresses() []string {
	addrs := r.mma.GetMinerAddresses()
	strs := make([]string, 0, len(addrs))
	for _, a := range addrs {
		strs = append(strs, a.String())
	}
	return strs
}
