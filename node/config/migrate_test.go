package config

import (
	"os"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const mockV0Config = `
SealerApiInfo = "api-endpoint"

[API]
  ListenAddress = "/ip4/127.0.0.1/tcp/1234/http"
`

const mockV2Config = `
ConfigVersion = 2
MyNewKey = "Hello"
`

func TestMigrate(t *testing.T) {
	// Add a new mock migration so as to be able to test migrating up and down
	mockv1Tov2 := func(string) (string, error) {
		return mockV2Config, nil
	}
	migrations = []migrateUpFn{
		v0Tov1,
		mockv1Tov2,
	}

	repoDir := t.TempDir()
	err := os.WriteFile(path.Join(repoDir, "config.toml"), []byte(mockV0Config), 0644)
	require.NoError(t, err)

	// Migrate up to v1
	err = configMigrate(repoDir, 1)
	require.NoError(t, err)

	// The existing config file should have been copied to config/config.toml.0
	v0File := path.Join(repoDir, "config", "config.toml.0")
	bz, err := os.ReadFile(v0File)
	require.NoError(t, err)
	require.Equal(t, mockV0Config, string(bz))

	// The new config file should have been written to config/config.toml.1
	v1File := path.Join(repoDir, "config", "config.toml.1")
	bz, err = os.ReadFile(v1File)
	v1FileContents := string(bz)
	require.NoError(t, err)

	// There should be a symlink from config.toml to config/config.toml.1
	symLink := path.Join(repoDir, "config.toml")
	bz, err = os.ReadFile(symLink)
	require.NoError(t, err)
	require.Equal(t, v1FileContents, string(bz))

	// The config file should have the new version
	require.True(t, strings.Contains(v1FileContents, `ConfigVersion = 1`))

	// The v1 config file should retain key / values that were changed from the defaults
	// in the original config file
	require.True(t, strings.Contains(v1FileContents, `SealerApiInfo = "api-endpoint"`))
	require.True(t, strings.Contains(v1FileContents, `ListenAddress = "/ip4/127.0.0.1/tcp/1234/http"`))

	// The config file should have comments:
	// # The connect string for the sealing RPC API (lotus miner)
	// #
	// # type: string
	// # env var: LOTUS__SEALERAPIINFO
	// SealerApiInfo = "api-endpoint"
	require.True(t, strings.Contains(v1FileContents, `The connect string for the sealing RPC API`))

	// Migrate to v1 should have no effect (because config file is already at v1)
	err = configMigrate(repoDir, 1)
	require.NoError(t, err)

	// Migrate up to v2 should apply v2 migration function
	err = configMigrate(repoDir, 2)
	require.NoError(t, err)

	bz, err = os.ReadFile(symLink)
	require.NoError(t, err)
	require.Equal(t, mockV2Config, string(bz))

	// Migrate down to v1 should restore v1 config
	err = configMigrate(repoDir, 1)
	require.NoError(t, err)

	bz, err = os.ReadFile(symLink)
	require.NoError(t, err)
	require.Equal(t, v1FileContents, string(bz))

	// Migrate down to v0 should have no effect (because the v0 and v1 config files are compatible)
	err = configMigrate(repoDir, 0)
	require.NoError(t, err)

	bz, err = os.ReadFile(symLink)
	require.NoError(t, err)
	require.Equal(t, v1FileContents, string(bz))
}

func TestConfigDiff(t *testing.T) {
	repoDir := t.TempDir()
	err := os.WriteFile(path.Join(repoDir, "config.toml"), []byte(mockV0Config), 0644)
	require.NoError(t, err)

	cgf, err := FromFile(path.Join(repoDir, "config.toml"), DefaultBoost())
	require.NoError(t, err)

	s, err := ConfigDiff(cgf, DefaultBoost(), false)
	require.NoError(t, err)

	require.False(t, strings.Contains(string(s), `The connect string for the sealing RPC API`))

	s, err = ConfigDiff(cgf, DefaultBoost(), true)
	require.NoError(t, err)

	require.True(t, strings.Contains(string(s), `The connect string for the sealing RPC API`))
}
