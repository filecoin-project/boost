package gql

import (
	"errors"
	"os"
	"path"

	gqltypes "github.com/filecoin-project/boost/gql/types"
	"github.com/filecoin-project/boost/util"
	lotus_modules "github.com/filecoin-project/lotus/node/modules"
)

type legacyStorageResolver struct {
	Capacity   gqltypes.Uint64
	Used       gqltypes.Uint64
	MountPoint string
}

// query: legacyStorage: [LegacyStorage]
func (r *resolver) LegacyStorage() (*legacyStorageResolver, error) {
	stagingDir := path.Join(r.repo.Path(), lotus_modules.StagingAreaDirName)
	used, err := util.DirSize(stagingDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		used = 0
	}

	return &legacyStorageResolver{
		Capacity:   gqltypes.Uint64(r.cfg.LotusDealmaking.MaxStagingDealsBytes),
		Used:       gqltypes.Uint64(used),
		MountPoint: stagingDir,
	}, nil
}
