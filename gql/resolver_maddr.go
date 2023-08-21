package gql

func (r *resolver) MinerAddress() string {
	return r.provider.Address.String()
}

func (r *resolver) GraphsyncRetrievalMinerAddresses() []string {
	addrs := r.cfg.Dealmaking.GraphsyncStorageAccessApiInfo
	if len(addrs) == 0 {
		addrs = []string{r.provider.Address.String()}
	}
	return addrs
}
