package config

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.com/BurntSushi/toml"
	logging "github.com/ipfs/go-log/v2"
	"github.com/kelseyhightower/envconfig"
)

var log = logging.Logger("cfg")

// CurrentVersion is the config version expected by Boost.
// We need to migrate the config file to this version.
const CurrentVersion = 6

type migrateUpFn = func(cfgPath string) (string, error)

var migrations = []migrateUpFn{
	v0Tov1, // index 0 => version 1
	v1Tov2, // index 1 => version 2
	v2Tov3, // index 2 => version 3
	v3Tov4, // index 3 => version 4
	v4Tov5, // index 4 => version 5
	v5Tov6, // index 5 => version 6
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

// FromFile loads config from a specified file overriding defaults specified in
// the def parameter. If file does not exist or is empty defaults are assumed.
func FromFile(path string, def interface{}) (interface{}, error) {
	file, err := os.Open(path)
	switch {
	case os.IsNotExist(err):
		return def, nil
	case err != nil:
		return nil, err
	}

	defer file.Close() //nolint:errcheck // The file is RO
	return FromReader(file, def)
}

// FromReader loads config from a reader instance.
func FromReader(reader io.Reader, def interface{}) (interface{}, error) {
	cfg := def
	_, err := toml.NewDecoder(reader).Decode(cfg)
	if err != nil {
		return nil, err
	}

	err = envconfig.Process("LOTUS", cfg)
	if err != nil {
		return nil, fmt.Errorf("processing env vars overrides: %s", err)
	}

	return cfg, nil
}
