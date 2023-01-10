package config

import "github.com/filecoin-project/lotus/storage/sealer"

// StorageManager convert boost config to sealer.Config
func (c *Boost) StorageManager() sealer.Config {
	return sealer.Config{
		ParallelFetchLimit: c.Storage.ParallelFetchLimit,
	}
}
