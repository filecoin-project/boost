package config

import (
	"fmt"
)

// Migrate from config version 2 to version 3 (i.e. remove a few fields and add recently added fields)
func v2Tov3(cfgPath string) (string, error) {
	cfg, err := FromFile(cfgPath, DefaultBoost())
	if err != nil {
		return "", fmt.Errorf("parsing config file %s: %w", cfgPath, err)
	}

	boostCfg, ok := cfg.(*Boost)
	if !ok {
		return "", fmt.Errorf("unexpected config type %T: expected *config.Boost", cfg)
	}

	// For the migration from v1 to v2 just remove fields and update config version and add
	// comments to the file

	// Update the Boost config version
	boostCfg.ConfigVersion = 3

	bz, err := ConfigUpdate(boostCfg, DefaultBoost(), true)
	if err != nil {
		return "", fmt.Errorf("applying configuration: %w", err)
	}

	return string(bz), nil
}
