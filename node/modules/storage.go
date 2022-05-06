package modules

import (
	"context"
	"fmt"
	"path/filepath"

	"go.uber.org/fx"

	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/backupds"
	lotus_dtypes "github.com/filecoin-project/lotus/node/modules/dtypes"
	"github.com/filecoin-project/lotus/node/modules/helpers"
	lotus_repo "github.com/filecoin-project/lotus/node/repo"
)

func LockedRepo(lr lotus_repo.LockedRepo) func(lc fx.Lifecycle) lotus_repo.LockedRepo {
	return func(lc fx.Lifecycle) lotus_repo.LockedRepo {
		lc.Append(fx.Hook{
			OnStop: func(_ context.Context) error {
				return lr.Close()
			},
		})

		return lr
	}
}

func KeyStore(lr lotus_repo.LockedRepo) (types.KeyStore, error) {
	return lr.KeyStore()
}

func Datastore(disableLog bool) func(lc fx.Lifecycle, mctx helpers.MetricsCtx, r lotus_repo.LockedRepo) (lotus_dtypes.MetadataDS, error) {
	return func(lc fx.Lifecycle, mctx helpers.MetricsCtx, r lotus_repo.LockedRepo) (lotus_dtypes.MetadataDS, error) {
		ctx := helpers.LifecycleCtx(mctx, lc)
		mds, err := r.Datastore(ctx, "/metadata")
		if err != nil {
			return nil, err
		}

		var logdir string
		if !disableLog {
			logdir = filepath.Join(r.Path(), "kvlog/metadata")
		}

		bds, err := backupds.Wrap(mds, logdir)
		if err != nil {
			return nil, fmt.Errorf("opening backupds: %w", err)
		}

		lc.Append(fx.Hook{
			OnStop: func(_ context.Context) error {
				return bds.CloseLog()
			},
		})

		return bds, nil
	}
}
