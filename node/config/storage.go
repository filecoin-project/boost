package config

import (
	lotus_config "github.com/filecoin-project/lotus/node/config"
)

// StorageManager convert boost config to sealer.Config
func (c *Boost) StorageManager() lotus_config.SealerConfig {
	return lotus_config.SealerConfig{
		ParallelFetchLimit: c.Storage.ParallelFetchLimit,
	}
}
