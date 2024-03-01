package config

import (
	"fmt"
)

// Migrate from config version 5 to version 6
func v5Tov6(cfgPath string) (string, error) {
	cfg, err := FromFile(cfgPath, DefaultBoost())
	if err != nil {
		return "", fmt.Errorf("parsing config file %s: %w", cfgPath, err)
	}

	boostCfg, ok := cfg.(*Boost)
	if !ok {
		return "", fmt.Errorf("unexpected config type %T: expected *config.Boost", cfg)
	}

	// Update the Boost config version
	boostCfg.ConfigVersion = 6

	bz, err := ConfigUpdate(boostCfg, DefaultBoost(), true, false)
	if err != nil {
		return "", fmt.Errorf("applying configuration: %w", err)
	}

	return string(bz), nil
}
