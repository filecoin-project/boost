package gql

func (r *resolver) MinerAddress() string {
	return r.provider.Address.String()
}
