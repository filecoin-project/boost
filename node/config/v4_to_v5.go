package config

import (
	"fmt"
)

// Migrate from config version 4 to version 5
func v4Tov5(cfgPath string) (string, error) {
	cfg, err := FromFile(cfgPath, DefaultBoost())
	if err != nil {
		return "", fmt.Errorf("parsing config file %s: %w", cfgPath, err)
	}

	boostCfg, ok := cfg.(*Boost)
	if !ok {
		return "", fmt.Errorf("unexpected config type %T: expected *config.Boost", cfg)
	}

	// Update the Boost config version
	boostCfg.ConfigVersion = 5

	bz, err := ConfigUpdate(boostCfg, DefaultBoost(), true, false)
	if err != nil {
		return "", fmt.Errorf("applying configuration: %w", err)
	}

	return string(bz), nil
}
