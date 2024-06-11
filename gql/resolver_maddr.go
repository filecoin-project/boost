package gql

type minerAddress struct {
	MinerID string
	IsCurio bool
}

func (r *resolver) MinerAddress() minerAddress {
	return minerAddress{
		MinerID: r.provider.Address.String(),
		IsCurio: r.curio,
	}
}

func (r *resolver) GraphsyncRetrievalMinerAddresses() []string {
	addrs := r.mma.GetMinerAddresses()
	strs := make([]string, 0, len(addrs))
	for _, a := range addrs {
		strs = append(strs, a.String())
	}
	return strs
}
