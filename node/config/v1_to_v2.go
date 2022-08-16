package config

import (
	"bytes"
	"fmt"

	"github.com/BurntSushi/toml"
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

	bz, err := ConfigUpdate(boostCfg, DefaultBoost(), true)
	if err != nil {
		return "", fmt.Errorf("applying configuration: %w", err)
	}

	return string(bz), nil
}

func RemoveRedundantFieldsV1toV2(cfgCur interface{}) ([]byte, error) {
	var nodeStr string
	{
		buf := new(bytes.Buffer)
		e := toml.NewEncoder(buf)
		if err := e.Encode(cfgCur); err != nil {
			return nil, fmt.Errorf("encoding node config: %w", err)
		}

		nodeStr = buf.String()
	}

	return []byte(nodeStr), nil
}
