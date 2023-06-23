package config

import (
	"fmt"
)

// Migrate from config version 1 to version 2 (i.e. remove a few redundant fields)
func v1Tov2(cfgPath string) (string, error) {
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
	boostCfg.ConfigVersion = 2

	bz, err := ConfigUpdate(boostCfg, DefaultBoost(), true, false)
	if err != nil {
		return "", fmt.Errorf("applying configuration: %w", err)
	}

	return string(bz), nil
}
