package modules

import (
	"github.com/filecoin-project/boost/indexprovider"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/lotus/node/config"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/libp2p/go-libp2p-core/host"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

func IndexProvider(cfg config.IndexProviderConfig) func(params lotus_modules.IdxProv, marketHost host.Host, dt dtypes.ProviderDataTransfer, maddr dtypes.MinerAddress, ps *pubsub.PubSub, nn dtypes.NetworkName) (provider.Interface, error) {
	if !cfg.Enable {
		log.Warnf("Starting Boost with index provider disabled - all index announcements will fail")
		return func(params lotus_modules.IdxProv, marketHost host.Host, dt dtypes.ProviderDataTransfer, maddr dtypes.MinerAddress, ps *pubsub.PubSub, nn dtypes.NetworkName) (provider.Interface, error) {
			return indexprovider.NewDisabledIndexProvider(), nil
		}
	}
	return lotus_modules.IndexProvider(cfg)
}
