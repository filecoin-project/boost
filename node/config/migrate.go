package config

import (
	"fmt"
	"os"
	"path"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("cfg")

// CurrentVersion is the config version expected by Boost.
// We need to migrate the config file to this version.
const CurrentVersion = 2

type migrateUpFn = func(cfgPath string) (string, error)

var migrations = []migrateUpFn{
	v0Tov1, // index 0 => version 0
	v1Tov2, // index 1 => version 1
}

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

	// Update the Boost config version
	boostCfg.ConfigVersion = 2

	// Remove redundant fields
	bz, err := RemoveRedundantFieldsV1toV2(boostCfg)
	if err != nil {
		return "", fmt.Errorf("applying configuration: %w", err)
	}

	return string(bz), nil
}

// Migrate from config version 0 to version 1
func v0Tov1(cfgPath string) (string, error) {
	cfg, err := FromFile(cfgPath, DefaultBoost())
	if err != nil {
		return "", fmt.Errorf("parsing config file %s: %w", cfgPath, err)
	}

	boostCfg, ok := cfg.(*Boost)
	if !ok {
		return "", fmt.Errorf("unexpected config type %T: expected *config.Boost", cfg)
	}

	// Update the Boost config version
	boostCfg.ConfigVersion = 1

	// For the migration from v0 to v1 just add the config version and add
	// comments to the file
	bz, err := ConfigUpdateV0toV1(boostCfg, DefaultBoost(), true)
	if err != nil {
		return "", fmt.Errorf("applying configuration: %w", err)
	}

	return string(bz), nil
}

// This struct is used to get the config file version
type VersionCfg struct {
	ConfigVersion int
}

func ConfigMigrate(repoPath string) error {
	return configMigrate(repoPath, CurrentVersion)
}

// This method is called by the tests
func configMigrate(repoPath string, toVersion int) error {
	// Get the version of the config file
	cfgPath := path.Join(repoPath, "config.toml")
	cfg, err := FromFile(cfgPath, &VersionCfg{})
	if err != nil {
		return fmt.Errorf("parsing config file %s: %w", cfgPath, err)
	}

	versionCfg, ok := cfg.(*VersionCfg)
	if !ok {
		return fmt.Errorf("unexpected config type %T: expected *config.VersionCfg", cfg)
	}

	// Ensure there's a "config" directory in the repo
	cfgDir := path.Join(repoPath, "config")
	err = os.MkdirAll(cfgDir, 0775)
	if err != nil {
		return fmt.Errorf("creating config directory %s: %w", cfgDir, err)
	}

	// If the config is out of date, migrate up
	if versionCfg.ConfigVersion < toVersion {
		err := migrateUp(versionCfg.ConfigVersion, toVersion, cfgPath, cfgDir)
		if err != nil {
			return fmt.Errorf("couldn't migrate config up to version %d: %w", toVersion, err)
		}
	}

	// If we need to roll back to an older config, migrate down
	if versionCfg.ConfigVersion > toVersion {
		err := migrateDown(versionCfg.ConfigVersion, toVersion, cfgPath, cfgDir)
		if err != nil {
			return fmt.Errorf("couldn't migrate config down to version %d: %w", toVersion, err)
		}
	}

	return nil
}

func migrateUp(fromVersion int, toVersion int, currentCfgPath string, cfgDir string) error {
	for oldVersion := fromVersion; oldVersion < toVersion; oldVersion++ {
		newVersion := oldVersion + 1

		log.Infof("Migrating config up from v%d to v%d", oldVersion, newVersion)

		// Copy the current config file to the "config" dir
		oldConfigContent, err := os.ReadFile(currentCfgPath)
		if err != nil {
			return fmt.Errorf("reading config file %s: %w", currentCfgPath, err)
		}

		oldVersionPath := path.Join(cfgDir, fmt.Sprintf("config.toml.%d", oldVersion))
		err = os.WriteFile(oldVersionPath, oldConfigContent, 0644)
		if err != nil {
			return fmt.Errorf("writing config file %s: %w", oldVersionPath, err)
		}

		if oldVersion >= len(migrations) {
			return fmt.Errorf("cannot migrate config file to v%d: no migration script defined", newVersion)
		}
		newConfigContent, err := migrations[oldVersion](currentCfgPath)
		if err != nil {
			return fmt.Errorf("migrating config file from v%d to v%d: %w", oldVersion, newVersion, err)
		}

		// Write the new version of the file
		newVersionFilename := fmt.Sprintf("config.toml.%d", toVersion)
		newVersionFilePath := path.Join(cfgDir, newVersionFilename)
		err = os.WriteFile(newVersionFilePath, []byte(newConfigContent), 0644)
		if err != nil {
			return fmt.Errorf("writing config file %s: %w", newVersionFilePath, err)
		}

		// Symlink from config.toml to the new version
		err = makeSymlink(currentCfgPath, toVersion)
		if err != nil {
			return err
		}
	}

	return nil
}

// Just move the symlink to point at the version we want to migrate to
func migrateDown(fromVersion int, toVersion int, currentCfgPath string, cfgDir string) error {
	// In practice toVersion should never be zero, because there is no boost
	// binary with config version 0. But in any case Version 0 and version 1
	// are compatible, so no need to migrate down.
	if toVersion == 0 {
		return nil
	}

	log.Infof("Migrating config down from v%d to v%d", fromVersion, toVersion)

	versionFilename := fmt.Sprintf("config.toml.%d", toVersion)
	versionFilePath := path.Join(cfgDir, versionFilename)
	_, err := os.Stat(versionFilePath)
	if err != nil {
		return fmt.Errorf("reading file for config version %d at %s: %w", toVersion, versionFilePath, err)
	}

	return makeSymlink(currentCfgPath, toVersion)
}

func makeSymlink(cfgPath string, version int) error {
	// Remove the existing file
	err := os.Remove(cfgPath)
	if err != nil {
		return fmt.Errorf("removing file %s: %w", cfgPath, err)
	}

	// Create a relative symlink config.toml => ./config/config.toml.<version>
	versionFilename := fmt.Sprintf("config.toml.%d", version)
	err = os.Symlink(path.Join("config", versionFilename), cfgPath)
	if err != nil {
		return fmt.Errorf("creating symlink to %s: %w", versionFilename, err)
	}

	return nil
}
