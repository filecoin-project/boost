package migrations

import (
	"github.com/libp2p/go-libp2p/core/peer"

	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/filecoin-project/go-ds-versioning/pkg/versioned"
)

// GetChannelStateMigrations returns a migration list for the channel states
func GetChannelStateMigrations(selfPeer peer.ID) (versioning.VersionedMigrationList, error) {
	return versioned.BuilderList{}.Build()
}
