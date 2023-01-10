package modules

import (
	"github.com/filecoin-project/boost/indexprovider"
	"github.com/filecoin-project/lotus/node/config"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	provider "github.com/ipni/index-provider"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

func IndexProvider(cfg config.IndexProviderConfig) func(params lotus_modules.IdxProv, marketHost host.Host, dt dtypes.ProviderDataTransfer, maddr dtypes.MinerAddress, ps *pubsub.PubSub, nn dtypes.NetworkName) (provider.Interface, error) {
	if !cfg.Enable {
		log.Warnf("Starting Boost with index provider disabled - no announcements will be made to the index provider")
		return func(params lotus_modules.IdxProv, marketHost host.Host, dt dtypes.ProviderDataTransfer, maddr dtypes.MinerAddress, ps *pubsub.PubSub, nn dtypes.NetworkName) (provider.Interface, error) {
			return indexprovider.NewDisabledIndexProvider(), nil
		}
	}
	return lotus_modules.IndexProvider(cfg)
}
