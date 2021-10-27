package datastore

import "github.com/filecoin-project/boost/storagemarket/types"

type API interface {
	CreateOrUpdateDeal(newState *types.ProviderDealState) error
}
